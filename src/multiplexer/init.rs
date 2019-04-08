
use futures::{
  Future,
  Stream,
  Sink,
  lazy
};
use futures::future::{loop_fn, Loop, Either};
use futures::sync::mpsc::{UnboundedSender, unbounded};
use futures::sync::oneshot::{
  Sender as OneshotSender,
  channel as oneshot_channel
};

use crate::utils as client_utils;
use crate::multiplexer::utils;
use crate::multiplexer::connection;

use std::ops::{Deref, DerefMut, Mul};

use redis_protocol::prelude::*;
use crate::error::*;
use crate::types::{
  RedisConfig,
  RedisValue,
  RedisKey,
  ClientState
};
use crate::protocol::types::{
  RedisCommand,
  ClusterKeyCache,
  RedisCommandKind
};

use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;

use std::collections::{
  BTreeMap,
  VecDeque
};

use crate::protocol::types::ResponseSender;
use crate::client::{RedisClientInner, RedisClient};
use crate::multiplexer::types::{Streams, Sinks, RedisSink, RedisStream};

use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Instant, Duration};

use tokio_core::reactor::Handle;
use crate::multiplexer::Multiplexer;

use tokio_core::net::{
  TcpStream
};
use std::net::{
  ToSocketAddrs,
  SocketAddr
};

use crate::error::*;

use tokio_io::codec::Framed;
use crate::protocol::RedisCodec;

#[cfg(feature="enable-tls")]
use tokio_tls::{
  TlsConnector as TlsConnectorAsync,
  TlsStream
};
#[cfg(feature="enable-tls")]
use native_tls::{
  TlsConnector
};

use futures::stream::{
  SplitSink,
  SplitStream
};
use core::borrow::Borrow;
use crate::protocol::types::RedisCommandKind::{Dump, ClientSetname};

#[cfg(feature="enable-tls")]
type TlsFramed = Framed<TlsStream<TcpStream>, RedisCodec>;
#[cfg(not(feature="enable-tls"))]
type TlsFramed = Framed<TcpStream, RedisCodec>;

type TcpFramed = Framed<TcpStream, RedisCodec>;

#[cfg(feature="enable-tls")]
type TlsTransports = Vec<(String, TlsFramed)>;

#[cfg(not(feature="enable-tls"))]
type TlsTransports = Vec<(String, TlsFramed)>;

type TcpTransports = Vec<(String, TcpFramed)>;

#[cfg(feature="enable-tls")]
type TlsTransportSink = SplitSink<TlsFramed>;
#[cfg(feature="enable-tls")]
type TlsTransportStream = SplitStream<TlsFramed>;

#[cfg(not(feature="enable-tls"))]
type TlsTransportSink = SplitSink<TcpFramed>;
#[cfg(not(feature="enable-tls"))]
type TlsTransportStream = SplitStream<TcpFramed>;

type TcpTransportSink = SplitSink<TcpFramed>;
type TcpTransportStream = SplitStream<TcpFramed>;

type SplitTlsTransport = (TlsTransportSink, TlsTransportStream);
type SplitTcpTransport = (TcpTransportSink, TcpTransportStream);

const INIT_TIMEOUT_MS: u64 = 60_000;

#[cfg(feature="enable-tls")]
fn init_tls_transports(inner: &Arc<RedisClientInner>, handle: Handle, key: Option<String>, cache: ClusterKeyCache)
  -> Box<Future<Item=(Either<TlsTransports, TcpTransports>, ClusterKeyCache), Error=RedisError>>
{
  debug!("Initialize clustered tls transports.");

  Box::new(connection::create_all_transports_tls(handle, &cache, key, inner).map(move |result| {
    (Either::A(result), cache)
  }))
}

#[cfg(not(feature="enable-tls"))]
fn init_tls_transports(inner: &Arc<RedisClientInner>, handle: Handle, key: Option<String>, cache: ClusterKeyCache)
  -> Box<Future<Item=(Either<TlsTransports, TcpTransports>, ClusterKeyCache), Error=RedisError>>
{
  debug!("Initialize clustered tls transports without real tls.");

  Box::new(connection::create_all_transports(handle, &cache, key, inner).map(move |result| {
    (Either::B(result), cache)
  }))
}

#[cfg(feature="enable-tls")]
fn create_centralized_transport_tls(inner: &Arc<RedisClientInner>, addr: &SocketAddr, handle: &Handle)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  debug!("Initialize centralized tls transports.");

  Box::new(connection::create_transport_tls(&addr, &handle, inner).map(|(sink, stream)| {
    Either::A((sink, stream))
  }))
}

#[cfg(not(feature="enable-tls"))]
fn create_centralized_transport_tls(inner: &Arc<RedisClientInner>, addr: &SocketAddr, handle: &Handle)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  debug!("Initialize centralized tls transports without real tls.");

  Box::new(connection::create_transport_tls(&addr, &handle, inner).map(|(sink, stream)| {
    Either::A((sink, stream))
  }))
}

fn create_centralized_transport(inner: &Arc<RedisClientInner>, addr: &SocketAddr, handle: &Handle)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  debug!("Initialize centralized transports.");

  Box::new(connection::create_transport(&addr, &handle, inner).map(|(sink, stream)| {
    Either::B((sink, stream))
  }))
}

type InitState = (Handle, Arc<RedisClientInner>, Multiplexer);
type CommandLoopState = (Handle, Arc<RedisClientInner>, Multiplexer, Option<RedisError>);

fn backoff_and_retry(inner: Arc<RedisClientInner>, handle: Handle, multiplexer: Multiplexer, force_no_backoff: bool) -> Box<Future<Item=Loop<InitState, InitState>, Error=RedisError>> {
  let delay = if force_no_backoff {
    None
  }else{
    if let Some(delay) = utils::next_reconnect_delay(&inner.policy) {
      Some(delay)
    }else{
      return client_utils::future_error(RedisError::new_canceled());
    }
  };

  let timer_ft = if let Some(delay) = delay {
    let dur = Duration::from_millis(delay as u64);

    Box::new(inner.timer.sleep(dur)
      .from_err::<RedisError>()
      .then(|_| Ok(())))
  }else{
    client_utils::future_ok(())
  };

  Box::new(timer_ft.and_then(move |_| {
    Ok::<_, RedisError>(Loop::Continue((handle, inner, multiplexer)))
  }))
}

fn build_centralized_multiplexer(handle: Handle, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool) -> Box<Future<Item=InitState, Error=RedisError>> {
  Box::new(loop_fn((handle, inner, multiplexer), move |(handle, inner, multiplexer)| {
    debug!("Attempting to rebuild centralized connection.");
    if client_utils::read_closed_flag(&inner.closed) {
      debug!("Emitting canceled error checking closed flag.");
      client_utils::future_error::<Loop<InitState, InitState>>(RedisError::new_canceled());
    }

    let init_inner = inner.clone();
    let init_handle = handle.clone();

    let final_multiplexer = multiplexer.clone();
    let final_handle = handle.clone();
    let final_inner = inner.clone();

    let dur = Duration::from_millis(INIT_TIMEOUT_MS);
    let timer_ft = inner.timer.sleep(dur)
      .from_err::<RedisError>()
      .map(|_| ());

    let uses_tls = inner.config.read().deref().tls();
    let auth_key = utils::read_auth_key(&inner.config);

    // initialize a connection to the server and split it, then attach the sink and stream to the multiplexer
    let init_ft = lazy(move || {
      let addr_str = fry!(utils::read_centralized_host(&init_inner.config));
      let mut addr = fry!(addr_str.to_socket_addrs());

      let addr = match addr.next() {
        Some(a) => a,
        None => return client_utils::future_error(RedisError::new(
          RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
        ))
      };

      if uses_tls {
        create_centralized_transport_tls(&init_inner, &addr, &init_handle)
      }else{
        create_centralized_transport(&init_inner, &addr, &init_handle)
      }
    })
    .and_then(move |transport| {
      debug!("Adding transports to centralized multiplexer.");
      match transport {
        Either::A((tls_sink, tls_stream)) => {
          multiplexer.sinks.set_centralized_sink(RedisSink::Tls(tls_sink));
          multiplexer.streams.add_stream(RedisStream::Tls(tls_stream));
        },
        Either::B((tcp_sink, tcp_stream)) => {
          multiplexer.sinks.set_centralized_sink(RedisSink::Tcp(tcp_sink));
          multiplexer.streams.add_stream(RedisStream::Tcp(tcp_stream));
        }
      };

      // notify callers that the connection is ready to use on the next event loop tick
      handle.spawn_fn(move || {
        debug!("Emitting connect message after reconnecting.");
        let new_client: RedisClient = (&inner).into();

        utils::emit_connect(&inner.connect_tx, &new_client);
        utils::emit_reconnect(&inner.reconnect_tx, &new_client);
        client_utils::set_client_state(&inner.state, ClientState::Connected);

        Ok::<_, ()>(())
      });

      handle.spawn(multiplexer.listen());
      Ok(multiplexer)
    });

    timer_ft.select2(init_ft).then(move |result| {
      match result {
        Ok(Either::A((_, init_ft))) => {
          // timer_ft finished first (timeout)
          let err = RedisError::new_timeout();
          utils::emit_connect_error(&final_inner.connect_tx, &err);
          utils::emit_error(&final_inner.error_tx, &err);

          backoff_and_retry(final_inner, final_handle, final_multiplexer, force_no_backoff)
        },
        Ok(Either::B((multiplexer, timer_ft))) => {
          // initialization worked
          client_utils::future_ok(Loop::Break((final_handle, final_inner, final_multiplexer)))
        },
        Err(Either::A((timer_err, init_ft))) => {
          // timer had an error, try again without backoff
          warn!("Timer error building redis connections: {:?}", timer_err);
          client_utils::future_ok(Loop::Continue((final_handle, final_inner, final_multiplexer)))
        },
        Err(Either::B((init_err, timer_ft))) => {
          // initialization had an error
          debug!("Error initializing connection: {:?}", init_err);

          utils::emit_connect_error(&final_inner.connect_tx, &init_err);
          utils::emit_error(&final_inner.error_tx, &init_err);

          backoff_and_retry(final_inner, final_handle, final_multiplexer, force_no_backoff)
        }
      }
    })
  }))
}

fn build_clustered_multiplexer(handle: Handle, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool) -> Box<Future<Item=InitState, Error=RedisError>> {
  Box::new(loop_fn((handle, inner, multiplexer), move |(handle, inner, multiplexer)| {
    debug!("Attempting to rebuild centralized connection.");
    if client_utils::read_closed_flag(&inner.closed) {
      debug!("Emitting canceled error checking closed flag.");
      client_utils::future_error::<Loop<InitState, InitState>>(RedisError::new_canceled());
    }

    let init_inner = inner.clone();
    let init_handle = handle.clone();

    let final_multiplexer = multiplexer.clone();
    let final_handle = handle.clone();
    let final_inner = inner.clone();

    let dur = Duration::from_millis(INIT_TIMEOUT_MS);
    let timer_ft = inner.timer.sleep(dur)
      .from_err::<RedisError>()
      .map(|_| ());

    let uses_tls = inner.config.read().deref().tls();
    let auth_key = utils::read_auth_key(&inner.config);

    // build the initial cluster state cache and initialize connections to the redis servers
    let init_ft = connection::build_cluster_cache(&handle, &inner).and_then(move |cache| {
      if uses_tls {
        init_tls_transports(&init_inner, init_handle, auth_key, cache)
      }else{
        Box::new(connection::create_all_transports(init_handle, &cache, auth_key, &init_inner).map(move |result| {
          (Either::B(result), cache)
        }))
      }
    })
    .and_then(move |(mut transports, cache)| {
      // set the new connections on the multiplexer instance
      debug!("Adding transports to clustered multiplexer.");

      multiplexer.sinks.set_cluster_cache(cache);

      match transports {
        Either::A(tls_transports) => {
          for (server, transport) in tls_transports.into_iter() {
            let (sink, stream) = transport.split();

            multiplexer.sinks.set_clustered_sink(server, RedisSink::Tls(sink));
            multiplexer.streams.add_stream(RedisStream::Tls(stream));
          }
        },
        Either::B(tcp_transports) => {
          for (server, transport) in tcp_transports.into_iter() {
            let (sink, stream) = transport.split();

            multiplexer.sinks.set_clustered_sink(server, RedisSink::Tcp(sink));
            multiplexer.streams.add_stream(RedisStream::Tcp(stream));
          }
        }
      };

      // notify callers that the connection is ready to use on the next event loop tick
      handle.spawn_fn(move || {
        let new_client: RedisClient = (&inner).into();

        utils::emit_connect(&inner.connect_tx, &new_client);
        utils::emit_reconnect(&inner.reconnect_tx, &new_client);
        client_utils::set_client_state(&inner.state, ClientState::Connected);

        Ok::<_, ()>(())
      });

      handle.spawn(multiplexer.listen());
      Ok(multiplexer)
    });

    timer_ft.select2(init_ft).then(move |result| {
      match result {
        Ok(Either::A((_, init_ft))) => {
          // timer_ft finished first (timeout)
          let err = RedisError::new_timeout();
          utils::emit_connect_error(&final_inner.connect_tx, &err);
          utils::emit_error(&final_inner.error_tx, &err);

          backoff_and_retry(final_inner, final_handle, final_multiplexer, force_no_backoff)
        },
        Ok(Either::B((multiplexer, timer_ft))) => {
          // initialization worked
          client_utils::future_ok(Loop::Break((final_handle, final_inner, final_multiplexer)))
        },
        Err(Either::A((timer_err, init_ft))) => {
          // timer had an error, try again without backoff
          warn!("Timer error building redis connections: {:?}", timer_err);
          client_utils::future_ok(Loop::Continue((final_handle, final_inner, final_multiplexer)))
        },
        Err(Either::B((init_err, timer_ft))) => {
          // initialization had an error
          debug!("Error initializing connection: {:?}", init_err);

          utils::emit_connect_error(&final_inner.connect_tx, &init_err);
          utils::emit_error(&final_inner.error_tx, &init_err);

          backoff_and_retry(final_inner, final_handle, final_multiplexer, force_no_backoff)
        }
      }
    })
  }))
}

/// Rebuild the connection and attempt to send the last command until it succeeds or the max number of reconnects are hit.
fn rebuild_connection(handle: Handle, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool, last_command: RedisCommand) -> Box<Future<Item=(Handle, Arc<RedisClientInner>, Multiplexer, Option<RedisError>), Error=RedisError>> {
  Box::new(loop_fn((handle, inner, multiplexer, force_no_backoff, last_command), move |(handle, inner, multiplexer, force_no_backoff, mut last_command)| {
    multiplexer.sinks.close();
    multiplexer.streams.close();

    let ft = if multiplexer.is_clustered() {
      build_clustered_multiplexer(handle, inner, multiplexer, force_no_backoff)
    }else{
      build_centralized_multiplexer(handle, inner, multiplexer, force_no_backoff)
    };

    ft.then(move |result| {
      let (handle, inner, multiplexer) = match result {
        Ok((h, i, m)) => (h, i, m),
        Err(e) => {
          warn!("Could not reconnect to redis server when rebuilding connection: {:?}", e);
          return client_utils::future_error(e)
        },
      };
      client_utils::set_client_state(&inner.state, ClientState::Connected);

      if last_command.max_attempts_exceeded() {
        warn!("Not retrying command after multiple failed attempts sending: {:?}", last_command.kind);
        if let Some(tx) = last_command.tx.take() {
          let _ = tx.send(Err(RedisError::new(RedisErrorKind::Unknown, "Max write attempts exceeded.")));
        }

        return client_utils::future_ok((handle, inner, multiplexer, None));
      }

      debug!("Retry sending last command after building connection: {:?}", last_command.kind);

      let (tx, rx) = oneshot_channel();
      multiplexer.set_last_command_callback(Some(tx));

      let write_ft = multiplexer.write_command(&mut last_command);
      multiplexer.set_last_request(Some(last_command));

      Box::new(write_ft.then(move |result| Ok((handle, inner, multiplexer, Some((rx, result))))))
    })
    .and_then(move |(handle, inner, multiplexer, result)| {
      let (rx, result) = match result {
        Some((rx, result)) => (rx, result),
        // we didnt attempt to send the last command again, so break out
        None => return client_utils::future_ok(Loop::Break((handle, inner, multiplexer, None)))
      };

      if let Err(e) = result {
        warn!("Error writing command: {:?}", e);
        if let Some(ref mut p) = inner.policy.write().deref_mut() {
          p.reset_attempts();
        }

        let last_command = match multiplexer.take_last_request() {
          Some(c) => c,
          None => return client_utils::future_error(RedisError::new(
            RedisErrorKind::Unknown, "Missing last command rebuilding connection."
          ))
        };

        client_utils::future_ok(Loop::Continue((handle, inner, multiplexer, force_no_backoff, last_command)))
      }else{
        // wait on the last request callback, if successful break, else continue retrying
        Box::new(rx.from_err::<RedisError>().then(move |result| {
          match result {
            Ok(Some((last_command, error))) => {
              if let Some(ref mut p) = inner.policy.write().deref_mut() {
                p.reset_attempts();
              }

              debug!("Continue to retry connections after trying to write command: {:?}", error);
              Ok(Loop::Continue((handle, inner, multiplexer, force_no_backoff, last_command)))
            },
            Ok(None) => Ok(Loop::Break((handle, inner, multiplexer, None))),
            Err(e) => Err(e)
          }
        }))
      }
    })
  }))
}

fn create_commands_ft(handle: Handle, inner: Arc<RedisClientInner>) -> Box<Future<Item=Option<RedisError>, Error=RedisError>> {
  let (tx, rx) = unbounded();
  utils::set_command_tx(&inner, tx);

  let multiplexer = Multiplexer::new(&inner);

  Box::new(lazy(move || {
    if multiplexer.is_clustered() {
      build_clustered_multiplexer(handle, inner, multiplexer, true)
    }else{
      build_centralized_multiplexer(handle, inner, multiplexer, true)
    }
  })
  .and_then(move |(handle, inner, multiplexer)| {
    rx.from_err::<RedisError>().fold((handle, inner, multiplexer, None), |(handle, inner, multiplexer, err): CommandLoopState, mut command: RedisCommand| {
      debug!("Handling redis command {:?}", command.kind);
      client_utils::decr_atomic(&inner.cmd_buffer_len);

      if command.kind.is_close() {
        debug!("Recv close command on the command stream.");

        if client_utils::read_client_state(&inner.state) == ClientState::Disconnected {
          // use a ping command as last command
          let last_command = RedisCommand::new(RedisCommandKind::Ping, vec![], None);

          rebuild_connection(handle, inner, multiplexer, false, last_command)
        }else{
          debug!("Skip close command since the connection is already up.");
          client_utils::future_ok((handle, inner, multiplexer, err))
        }
      }else if command.kind.is_split() {
        let (resp_tx, key) = match command.kind.take_split() {
          Ok(mut i) => i.take(),
          Err(e) => {
            error!("Invalid split command: {:?}", e);
            return client_utils::future_ok((handle, inner, multiplexer, err));
          }
        };

        if let Some(resp_tx) = resp_tx {
          let res = multiplexer.sinks.centralized_configs(key);
          let _ = resp_tx.send(res);

          client_utils::future_ok((handle, inner, multiplexer, err))
        }else{
          error!("Invalid split command missing response sender.");
          client_utils::future_ok((handle, inner, multiplexer, err))
        }
      }else{
        if command.kind == RedisCommandKind::Quit {
          debug!("Setting state to disconnecting on quit command.");
          client_utils::set_client_state(&inner.state, ClientState::Disconnecting);
        }

        let (tx, rx) = oneshot_channel();
        multiplexer.set_last_command_callback(Some(tx));

        let write_ft = multiplexer.write_command(&mut command);
        multiplexer.set_last_request(Some(command));

        Box::new(write_ft.then(move |result| {
          if let Err(e) = result {
            // if an error while writing then check check reconnect policy and try to reconnect, build multiplexer state and cluster state
            warn!("Error writing command: {:?}", e);
            if let Some(ref mut p) = inner.policy.write().deref_mut() {
              p.reset_attempts();
            }

            let last_command = match multiplexer.take_last_request() {
              Some(c) => c,
              None => return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Missing last command rebuilding connection."
              ))
            };

            rebuild_connection(handle, inner, multiplexer, false, last_command)
          }else{
            Box::new(rx.from_err::<RedisError>().then(move |result| {
              // if an error occurs waiting on the response then check the reconnect policy and try to reconnect,
              // then build multiplexer state, otherwise move on to the next command

              debug!("Callback message recv: {:?}", result);

              match result {
                Ok(Some((last_command, error))) => {
                  if let Some(ref mut p) = inner.policy.write().deref_mut() {
                    p.reset_attempts();
                  }

                  rebuild_connection(handle, inner, multiplexer, false, last_command)
                },
                Ok(None) => {
                  client_utils::future_ok((handle, inner, multiplexer, None))
                },
                Err(e) => {
                  client_utils::future_error(e)
                }
              }
            }))
          }
        }))
      }
    })
    .map(|(_, _, _, err)| err)
    .then(|result| match result {
      Ok(e) => Ok(e),
      Err(e) => Ok::<_, RedisError>(Some(e))
    })
  }))
}

/// Initialize a connection to the Redis server.
pub fn connect(handle: &Handle, inner: Arc<RedisClientInner>) -> Box<Future<Item=Option<RedisError>, Error=RedisError>> {
  client_utils::set_client_state(&inner.state, ClientState::Connecting);

  Box::new(create_commands_ft(handle.clone(), inner).then(|result| {
    if let Err(ref e) = result {
      if e.is_canceled() {
        debug!("Suppressing canceled redis error: {:?}", e);

        Ok(None)
      }else{
        result
      }
    }else{
      result
    }
  }))
}