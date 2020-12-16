
use futures::{
  Future,
  Stream,
  StreamExt,
  Sink,
  select,
  // lazy
};
// use futures::future::{loop_fn, Loop, Either};
use futures::future::{Either, lazy, TryFutureExt, FutureExt};
use futures::channel::mpsc::{UnboundedSender, unbounded};
use futures::channel::oneshot::{
  Sender as OneshotSender,
  channel as oneshot_channel
};

#[cfg(feature="mocks")]
use crate::mocks;

use crate::utils as client_utils;
use crate::multiplexer::utils;
use crate::multiplexer::connection;
use crate::async_ng::*;

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

use crate::multiplexer::Multiplexer;

use tokio_02::net::{
  TcpStream
};
use std::net::{
  ToSocketAddrs,
  SocketAddr
};

use crate::error::*;

use tokio_util::codec::Framed;
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

/*
use futures::stream::{
  SplitSink,
  SplitStream
};
*/
use futures_util::stream::{
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
type TlsTransportSink = futures_util::stream::SplitSink<TcpFramed,Frame>; // FIXME: this appears to be in futures::stream too
#[cfg(not(feature="enable-tls"))]
type TlsTransportStream = futures_util::stream::SplitStream<TcpFramed>;

type TcpTransportSink = SplitSink<TcpFramed, Frame>;
type TcpTransportStream = SplitStream<TcpFramed>;

type SplitTlsTransport = (TlsTransportSink, TlsTransportStream);
type SplitTcpTransport = (TcpTransportSink, TcpTransportStream);

const INIT_TIMEOUT_MS: u64 = 60_000;

#[cfg(feature="enable-tls")]
fn init_tls_transports(inner: &Arc<RedisClientInner>, spawner: Spawner, key: Option<String>, cache: ClusterKeyCache)
  -> Box<Future<Item=(Either<TlsTransports, TcpTransports>, ClusterKeyCache), Error=RedisError>>
{
  debug!("{} Initialize clustered tls transports.", n!(inner));

  Box::new(connection::create_all_transports_tls(spawner, &cache, key, inner).map(move |result| {
    (Either::A(result), cache)
  }))
}

#[cfg(not(feature="enable-tls"))]
async fn init_tls_transports(inner: &Arc<RedisClientInner>, spawner: Spawner, key: Option<String>, cache: &ClusterKeyCache)
  -> Result<Either<TlsTransports, TcpTransports>, RedisError>
{
  debug!("{} Initialize clustered tls transports without real tls.", n!(inner));

  let result = connection::create_all_transports(spawner, cache, key, inner).await?;
  Ok(Either::Right(result))
}

#[cfg(feature="enable-tls")]
fn create_centralized_transport_tls(inner: &Arc<RedisClientInner>, addr: &SocketAddr, spawner: &Spawner)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  debug!("{} Initialize centralized tls transports.", n!(inner));

  Box::new(connection::create_transport_tls(&addr, &spawner, inner).map(|(sink, stream)| {
    Either::A((sink, stream))
  }))
}

#[cfg(not(feature="enable-tls"))]
async fn create_centralized_transport_tls(inner: &Arc<RedisClientInner>, addr: &SocketAddr, spawner: &Spawner)
  -> Result<Either<SplitTlsTransport, SplitTcpTransport>, RedisError>
{
  debug!("{} Initialize centralized tls transports without real tls.", n!(inner));

  let (sink, stream) = connection::create_transport_tls(&addr, &spawner, inner).await?;
  Ok(Either::Left((sink, stream)))
}

async fn create_centralized_transport(inner: &Arc<RedisClientInner>, addr: &SocketAddr, spawner: &Spawner)
  -> Result<Either<SplitTlsTransport, SplitTcpTransport>, RedisError>
{
  debug!("{} Initialize centralized transports.", n!(inner));

  let (sink,stream) = connection::create_transport(&addr, &spawner, inner).await?;
  Ok(Either::Right((sink, stream)))
}

type InitState = (Spawner, Arc<RedisClientInner>, Multiplexer);
type CommandLoopState = (Spawner, Arc<RedisClientInner>, Multiplexer, Option<RedisError>);

async fn backoff_and_retry(inner: Arc<RedisClientInner>, spawner: Spawner, multiplexer: Multiplexer, force_no_backoff: bool) -> Result<InitState, RedisError> {
  let delay = if force_no_backoff {
    None
  }else{
    if let Some(delay) = utils::next_reconnect_delay(&inner.policy) {
      Some(delay)
    }else{
      return Err(RedisError::new_canceled());
    }
  };

  if let Some(delay) = delay {
    tokio_02::time::delay_for(Duration::from_millis(delay as u64)).await;
  }

  Ok((spawner, inner, multiplexer))

  /*
  let timer_ft = if let Some(delay) = delay {
    let dur = Duration::from_millis(delay as u64);

    Box::new(inner.timer.sleep(dur)
      .from_err::<RedisError>()
      .then(|_| Ok(())))
  }else{
    client_utils::future_ok(())
  };

  Box::new(timer_ft.and_then(move |_| {
    Ok::<_, RedisError>(Loop::Continue((spawner, inner, multiplexer)))
  }))
   */
}

async fn build_centralized_multiplexer(spawner: Spawner, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool) -> Result<InitState, RedisError> {

  let multiplexer_orig = multiplexer.clone();
  let inner_orig = inner.clone();

  //Box::new(loop_fn((spawner, inner, multiplexer), move |(spawner, inner, multiplexer)| {
  loop {
    let multiplexer = multiplexer_orig.clone();
    let inner = inner_orig.clone();
    
    debug!("{} Attempting to rebuild centralized connection.", n!(inner));
    if client_utils::read_closed_flag(&inner.closed) {
      debug!("{} Emitting canceled error checking closed flag.", n!(inner));
      return Err(RedisError::new_canceled());
    }

    let init_inner = inner.clone();
    let init_spawner = spawner.clone();

    let final_multiplexer = multiplexer.clone();
    let final_spawner = spawner.clone();
    let final_inner = inner.clone();

    let dur = Duration::from_millis(INIT_TIMEOUT_MS);
    let mut timer_ft = Box::pin(tokio_02::time::delay_for(dur).fuse());
    //let timer_ft = Box::new(inner.timer.sleep(dur)).fuse(); // FIXME: can we fuse the 0.1 timer?
      //.err_into::<RedisError>()
      //.map(|_| ());

    let uses_tls = inner.config.read().deref().tls();
    let auth_key = utils::read_auth_key(&inner.config);

    // initialize a connection to the server and split it, then attach the sink and stream to the multiplexer
    /*
    let init_ft = lazy(move |_| {
      let addr_str = fry!(utils::read_centralized_host(&init_inner.config));
      let mut addr = fry!(addr_str.to_socket_addrs());

      let addr = match addr.next() {
        Some(a) => a,
        None => return crate::utils::future_error(RedisError::new(
          RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
        ))
      };

      if uses_tls {
        //Box::new(create_centralized_transport_tls(&init_inner, &addr, &init_spawner))
        Box::new(create_centralized_transport_tls(&init_inner, &addr, &init_spawner))
      }else{
        //Box::new(create_centralized_transport(&init_inner, &addr, &init_spawner))
        Box::new(create_centralized_transport(&init_inner, &addr, &init_spawner))
      }
    })
     */
    let init_ft = async move {
      let addr_str = utils::read_centralized_host(&init_inner.config)?;
      let mut addr = addr_str.to_socket_addrs()?;

      let addr = match addr.next() {
        Some(a) => a,
        None => return Err(RedisError::new(
          RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
        ))
      };

      if uses_tls {
        //Box::new(create_centralized_transport_tls(&init_inner, &addr, &init_spawner))
        create_centralized_transport_tls(&init_inner, &addr, &init_spawner).await
      }else{
        //Box::new(create_centralized_transport(&init_inner, &addr, &init_spawner))
        create_centralized_transport(&init_inner, &addr, &init_spawner).await
      }
    };

    let init_ft = init_ft.map_ok(move |transport| {
      debug!("{} Adding transports to centralized multiplexer.", n!(inner));
      match transport {
        Either::Left((tls_sink, tls_stream)) => {
          multiplexer.sinks.set_centralized_sink(RedisSink::Tls(tls_sink));
          multiplexer.streams.add_stream(RedisStream::Tls(tls_stream));
        },
        Either::Right((tcp_sink, tcp_stream)) => {
          multiplexer.sinks.set_centralized_sink(RedisSink::Tcp(tcp_sink));
          multiplexer.streams.add_stream(RedisStream::Tcp(tcp_stream));
        }
      };

      // FIXME: recheck logic here in case of error, and for both spawned tasks
      //

      // FIXME: re-enable this

      // notify callers that the connection is ready to use on the next event loop tick
      /*
      spawner.spawn_std(lazy(move |_| {
        debug!("{} Emitting connect message after reconnecting.", n!(inner));
        let new_client: RedisClient = (&inner).into();

        utils::emit_connect(&inner.connect_tx, &new_client);
        utils::emit_reconnect(&inner.reconnect_tx, &new_client);
        client_utils::set_client_state(&inner.state, ClientState::Connected);

        //Ok::<_, ()>(())
      }));

      spawner.spawn_std(multiplexer.listen());
      */
      multiplexer
    });
    let mut init_ft = Box::pin(init_ft);

    select! {
      timeout = timer_ft => match timeout {
        //Ok(_) => {
        () => {
          // timer_ft finished first (timeout)
          let err = RedisError::new_timeout();
          utils::emit_connect_error(&final_inner.connect_tx, &err);
          utils::emit_error(&final_inner.error_tx, &err);

          // FIXME: need to propagate rather than ignore error here?
          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff).await;
          continue;
        },/*
        Err((timer_err,_)) => {
          // timer had an error, try again without backoff
          warn!("{} Timer error building redis connections: {:?}", nw!(final_inner), timer_err);
          //client_utils::future_ok(Loop::Continue((final_spawner, final_inner, final_multiplexer)))
          continue;
        }*/
      },
      init_done = init_ft => match init_done {
        Ok(_) => {
          // initialization worked
          // client_utils::future_ok(Loop::Break((final_spawner, final_inner, final_multiplexer)))
          return Ok((final_spawner, final_inner, final_multiplexer))
        },
        Err(init_err) => {
          // initialization had an error
          debug!("{} Error initializing connection: {:?}", n!(final_inner), init_err);

          utils::emit_connect_error(&final_inner.connect_tx, &init_err);
          utils::emit_error(&final_inner.error_tx, &init_err);

          // FIXME: need to propagate rather than ignore error here?
          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff).await;
          continue;
        }
      }
    }

    /*
    Box::new(timer_ft.select2(init_ft).then(move |result| {
      match result {
        Ok(Either::A((_, init_ft))) => {
          // timer_ft finished first (timeout)
          let err = RedisError::new_timeout();
          utils::emit_connect_error(&final_inner.connect_tx, &err);
          utils::emit_error(&final_inner.error_tx, &err);

          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff)
        },
        Ok(Either::B((multiplexer, timer_ft))) => {
          // initialization worked
          client_utils::future_ok(Loop::Break((final_spawner, final_inner, final_multiplexer)))
        },
        Err(Either::A((timer_err, init_ft))) => {
          // timer had an error, try again without backoff
          warn!("{} Timer error building redis connections: {:?}", nw!(final_inner), timer_err);
          client_utils::future_ok(Loop::Continue((final_spawner, final_inner, final_multiplexer)))
        },
        Err(Either::B((init_err, timer_ft))) => {
          // initialization had an error
          debug!("{} Error initializing connection: {:?}", n!(final_inner), init_err);

          utils::emit_connect_error(&final_inner.connect_tx, &init_err);
          utils::emit_error(&final_inner.error_tx, &init_err);

          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff)
        }
      }
    }))
    */
  // }))
  }
}

async fn build_clustered_multiplexer(spawner: Spawner, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool) -> Result<InitState, RedisError> {
  //Box::new(loop_fn((spawner, inner, multiplexer), move |(spawner, inner, multiplexer)| {

  let spawner_orig = spawner.clone();
  let multiplexer_orig = multiplexer.clone();
  let inner_orig = inner.clone();

  loop {
    let spanwer = spawner_orig.clone();
    let multiplexer = multiplexer_orig.clone();
    let inner = inner_orig.clone();

    debug!("{} Attempting to rebuild centralized connection.", n!(inner));
    if client_utils::read_closed_flag(&inner.closed) {
      debug!("{} Emitting canceled error checking closed flag.", n!(inner));
      return Err(RedisError::new_canceled());
    }

    let init_inner = inner.clone();
    let init_spawner = spawner_orig.clone();

    let final_multiplexer = multiplexer.clone();
    let final_spawner = spawner_orig.clone();
    let final_inner = inner.clone();

    let dur = Duration::from_millis(INIT_TIMEOUT_MS);
    let mut timer_ft = Box::pin(tokio_02::time::delay_for(dur).fuse());
    //let timer_ft = inner.timer.sleep(dur)
    //  .err_info::<RedisError>()
    //  .map(|_| ());

    let uses_tls = inner.config.read().deref().tls();
    let auth_key = utils::read_auth_key(&inner.config);

    // build the initial cluster state cache and initialize connections to the redis servers
    let init_ft = async move {
      let cache = connection::build_cluster_cache(&init_spawner, &inner).await?;
      let transports = if uses_tls {
        init_tls_transports(&init_inner, init_spawner, auth_key, &cache).await?
      }else{
        connection::create_all_transports(init_spawner, &cache, auth_key, &init_inner).map_ok(move |result| {
          Either::Right(result) // FIXME: double check that this should be Right
        }).await?
      };

      // set the new connections on the multiplexer instance
      debug!("{} Adding transports to clustered multiplexer.", n!(inner));

      multiplexer.sinks.set_cluster_cache(cache);

      match transports {
        Either::Left(tls_transports) => {
          for (server, transport) in tls_transports.into_iter() {
            let (sink, stream) = transport.split();

            multiplexer.sinks.set_clustered_sink(server, RedisSink::Tls(sink));
            multiplexer.streams.add_stream(RedisStream::Tls(stream));
          }
        },
        Either::Right(tcp_transports) => {
          for (server, transport) in tcp_transports.into_iter() {
            let (sink, stream) = transport.split();

            multiplexer.sinks.set_clustered_sink(server, RedisSink::Tcp(sink));
            multiplexer.streams.add_stream(RedisStream::Tcp(stream));
          }
        }
      }

      Ok(multiplexer)
    };
    /*
    let init_ft = connection::build_cluster_cache(&spawner, &inner).and_then(move |cache| {
      if uses_tls {
        init_tls_transports(&init_inner, init_spawner, auth_key, cache)
      }else{
        connection::create_all_transports(init_spawner, &cache, auth_key, &init_inner).map(move |result| {
          (Either::B(result), cache)
        })
      }
    })
    .and_then(move |(mut transports, cache)| {
      // set the new connections on the multiplexer instance
      debug!("{} Adding transports to clustered multiplexer.", n!(inner));

      multiplexer.sinks.set_cluster_cache(cache);

      match transports {
        Either::Left(tls_transports) => {
          for (server, transport) in tls_transports.into_iter() {
            let (sink, stream) = transport.split();

            multiplexer.sinks.set_clustered_sink(server, RedisSink::Tls(sink));
            multiplexer.streams.add_stream(RedisStream::Tls(stream));
          }
        },
        Either::Right(tcp_transports) => {
          for (server, transport) in tcp_transports.into_iter() {
            let (sink, stream) = transport.split();

            multiplexer.sinks.set_clustered_sink(server, RedisSink::Tcp(sink));
            multiplexer.streams.add_stream(RedisStream::Tcp(stream));
          }
        }
      };
      */
/*
      // notify callers that the connection is ready to use on the next event loop tick
      spawner.spawn(lazy(move || {
        let new_client: RedisClient = (&inner).into();

        utils::emit_connect(&inner.connect_tx, &new_client);
        utils::emit_reconnect(&inner.reconnect_tx, &new_client);
        client_utils::set_client_state(&inner.state, ClientState::Connected);

        Ok::<_, ()>(())
      }));

      spawner.spawn_remote(multiplexer.listen());
      Ok(multiplexer)
    });
*/

    let mut init_ft = Box::pin(init_ft.fuse());
    select! {
      timeout = timer_ft => match timeout {
        () => {
          // timer_ft finished first (timeout)
          let err = RedisError::new_timeout();
          utils::emit_connect_error(&final_inner.connect_tx, &err);
          utils::emit_error(&final_inner.error_tx, &err);

          // FIXME: need to propagate rather than ignore error here?
          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff).await;
          continue;
        },
      },
      init_done = init_ft => match init_done {
        Ok(_) => {
          // initialization worked
          return Ok((final_spawner, final_inner, final_multiplexer))
        },
        Err(init_err) => {
          // initialization had an error
          debug!("{} Error initializing connection: {:?}", n!(final_inner), init_err);

          utils::emit_connect_error(&final_inner.connect_tx, &init_err);
          utils::emit_error(&final_inner.error_tx, &init_err);

          // FIXME: need to propagate rather than ignore error here?
          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff).await;
          continue;
        }
      }
    }
    /* Box::new(timer_ft.select2(init_ft).then(move |result| {
      match result {
        Ok(Either::A((_, init_ft))) => {
          // timer_ft finished first (timeout)
          let err = RedisError::new_timeout();
          utils::emit_connect_error(&final_inner.connect_tx, &err);
          utils::emit_error(&final_inner.error_tx, &err);

          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff)
        },
        Ok(Either::B((multiplexer, timer_ft))) => {
          // initialization worked
          client_utils::future_ok(Loop::Break((final_spawner, final_inner, final_multiplexer)))
        },
        Err(Either::A((timer_err, init_ft))) => {
          // timer had an error, try again without backoff
          warn!("{} Timer error building redis connections: {:?}", nw!(final_inner), timer_err);
          client_utils::future_ok(Loop::Continue((final_spawner, final_inner, final_multiplexer)))
        },
        Err(Either::B((init_err, timer_ft))) => {
          // initialization had an error
          debug!("{} Error initializing connection: {:?}", n!(final_inner), init_err);

          utils::emit_connect_error(&final_inner.connect_tx, &init_err);
          utils::emit_error(&final_inner.error_tx, &init_err);

          backoff_and_retry(final_inner, final_spawner, final_multiplexer, force_no_backoff)
        }
      }
    }))
     */

  //}))
  }
}

/// Rebuild the connection and attempt to send the last command until it succeeds or the max number of reconnects are hit.
//fn rebuild_connection(spawner: Spawner, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool, last_command: RedisCommand) -> Box<Future<Item=(Spawner, Arc<RedisClientInner>, Multiplexer, Option<RedisError>), Error=RedisError>> {
async fn rebuild_connection(spawner: Spawner, inner: Arc<RedisClientInner>, multiplexer: Multiplexer, force_no_backoff: bool, last_command: RedisCommand) ->
  Result<(Spawner, Arc<RedisClientInner>, Multiplexer, Option<RedisError>), RedisError>
{
  //Box::new(loop_fn((spawner, inner, multiplexer, force_no_backoff, last_command), move |(spawner, inner, multiplexer, force_no_backoff, mut last_command)| {

  let mut last_command_opt = Some(last_command); // FIXME: should be a more elegant way of dealing with ownership of this

  loop {
    // FIXME: probably don't want to actually clone the multiplexer every time... probably need to up
    let multiplexer = multiplexer.clone();
    let inner = inner.clone();
    let spawner = spawner.clone();

    multiplexer.sinks.close();
    multiplexer.streams.close();

    let logging_inner = inner.clone();

    let result = if multiplexer.is_clustered() {
      build_clustered_multiplexer(spawner, inner, multiplexer, force_no_backoff).await
    }else{
      build_centralized_multiplexer(spawner, inner, multiplexer, force_no_backoff).await
    };

    let (spawner, inner, multiplexer) = match result {
      Ok(t) => t,
      Err(e) => {
        warn!("{} Could not reconnect to redis server when rebuilding connection: {:?}", nw!(logging_inner), e);
        return Err(e);
      }
    };
    client_utils::set_client_state(&inner.state, ClientState::Connected);

    let mut last_command = last_command_opt.take().unwrap();

    if last_command.max_attempts_exceeded() {
      warn!("{} Not retrying command after multiple failed attempts sending: {:?}", nw!(inner), last_command.kind);
      if let Some(tx) = last_command.tx.take() {
        let _ = tx.send(Err(RedisError::new(RedisErrorKind::Unknown, "Max write attempts exceeded.")));
      }

      return Ok((spawner, inner, multiplexer, None));
    }

    debug!("{} Retry sending last command after building connection: {:?}", n!(inner), last_command.kind);
    let (tx, rx) = oneshot_channel();
    multiplexer.set_last_command_callback(Some(tx));

    let write_ft = multiplexer.write_and_take_command(&inner, last_command);
    //multiplexer.set_last_request(Some(last_command));
    let result = write_ft.await;

    if let Err(e) = result {
      warn!("{} Error writing command: {:?}", nw!(inner), e);
      if let Some(ref mut p) = inner.policy.write().deref_mut() {
        p.reset_attempts();
      }

      last_command_opt = multiplexer.take_last_request();
      if last_command_opt.is_none() {
        return Err(RedisError::new(
          RedisErrorKind::Unknown, "Missing last command rebuilding connection."
        ))
      };

      //client_utils::future_ok(Loop::Continue((spawner, inner, multiplexer, force_no_backoff, last_command)))
      continue;
    }else{
      // wait on the last request callback, if successful break, else continue retrying
      let result = rx.await;
      match result {
        Ok(Some((last_command, error))) => {
          if let Some(ref mut p) = inner.policy.write().deref_mut() {
            p.reset_attempts();
          }

          debug!("{} Continue to retry connections after trying to write command: {:?}", n!(inner), error);
          last_command_opt = Some(last_command);
          //Ok(Loop::Continue((spawner, inner, multiplexer, force_no_backoff, last_command)))
          continue;
        },
        //Ok(None) => Ok(Loop::Break((spawner, inner, multiplexer, None))),
        Ok(None) => return Ok((spawner, inner, multiplexer, None)),
        Err(e) => return Err(e.into()) // FIXME: double check logic here
      }
    }
  }

  /*
  loop {
    multiplexer.sinks.close();
    multiplexer.streams.close();

    let logging_inner = inner.clone();

    let ft = if multiplexer.is_clustered() {
      build_clustered_multiplexer(spawner, inner, multiplexer, force_no_backoff)
    }else{
      build_centralized_multiplexer(spawner, inner, multiplexer, force_no_backoff)
    };

    ft.then(move |result| {
      let (spawner, inner, multiplexer) = match result {
        Ok((h, i, m)) => (h, i, m),
        Err(e) => {
          warn!("{} Could not reconnect to redis server when rebuilding connection: {:?}", nw!(logging_inner), e);
          return Err(e);
        },
      };
      client_utils::set_client_state(&inner.state, ClientState::Connected);

      if last_command.max_attempts_exceeded() {
        warn!("{} Not retrying command after multiple failed attempts sending: {:?}", nw!(inner), last_command.kind);
        if let Some(tx) = last_command.tx.take() {
          let _ = tx.send(Err(RedisError::new(RedisErrorKind::Unknown, "Max write attempts exceeded.")));
        }

        return Ok((spawner, inner, multiplexer, None));
      }

      debug!("{} Retry sending last command after building connection: {:?}", n!(inner), last_command.kind);

      let (tx, rx) = oneshot_channel();
      multiplexer.set_last_command_callback(Some(tx));

      let write_ft = multiplexer.write_command(&inner, &mut last_command);
      multiplexer.set_last_request(Some(last_command));

      write_ft.then(move |result| Ok((spawner, inner, multiplexer, Some((rx, result)))))
    })
    .and_then(move |(spawner, inner, multiplexer, result)| {
      let (rx, result) = match result {
        Some((rx, result)) => (rx, result),
        // we didnt attempt to send the last command again, so break out
        None => return Ok((spawner, inner, multiplexer, None))
      };

      if let Err(e) = result {
        warn!("{} Error writing command: {:?}", nw!(inner), e);
        if let Some(ref mut p) = inner.policy.write().deref_mut() {
          p.reset_attempts();
        }

        let last_command = match multiplexer.take_last_request() {
          Some(c) => c,
          None => return Err(RedisError::new(
            RedisErrorKind::Unknown, "Missing last command rebuilding connection."
          ))
        };

        //client_utils::future_ok(Loop::Continue((spawner, inner, multiplexer, force_no_backoff, last_command)))
        continue;
      }else{
        // wait on the last request callback, if successful break, else continue retrying
        rx.err_into::<RedisError>().then(move |result| {
          match result {
            Ok(Some((last_command, error))) => {
              if let Some(ref mut p) = inner.policy.write().deref_mut() {
                p.reset_attempts();
              }

              debug!("{} Continue to retry connections after trying to write command: {:?}", n!(inner), error);
              //Ok(Loop::Continue((spawner, inner, multiplexer, force_no_backoff, last_command)))
              continue;
            },
            //Ok(None) => Ok(Loop::Break((spawner, inner, multiplexer, None))),
            Ok(None) => return Ok((spawner, inner, multiplexer, None)),
            Err(e) => return Err(e) // FIXME: double check logic here
          }
        })
      }
    })
  }
   */
}

async fn create_commands_ft(spawner: Spawner, inner: Arc<RedisClientInner>) -> Result<Option<RedisError>, RedisError> {
  //Box<Future<Item=Option<RedisError>, Error=RedisError>> {
  let (tx, mut rx) = unbounded();
  utils::set_command_tx(&inner, tx);

  let multiplexer = Multiplexer::new(&inner);
  let (spawner, inner, multiplexer) =
    if multiplexer.is_clustered() {
      build_clustered_multiplexer(spawner, inner, multiplexer, true).await?
    }else{
      build_centralized_multiplexer(spawner, inner, multiplexer, true).await?
    };

  while let Some(mut command) = rx.next().await {

    // FIXME: should not need this if we fix rebuild_connection to take refs?
    let spawner = spawner.clone();
    let inner = inner.clone();
    let multiplexer = multiplexer.clone();

    debug!("{} Handling redis command {:?}", n!(inner), command.kind);
    client_utils::decr_atomic(&inner.cmd_buffer_len);

    if command.kind.is_close() {
      debug!("{} Recv close command on the command stream.", n!(inner));

      if client_utils::read_client_state(&inner.state) == ClientState::Disconnected {
        // use a ping command as last command
        let last_command = RedisCommand::new(RedisCommandKind::Ping, vec![], None);
        rebuild_connection(spawner, inner, multiplexer, false, last_command).await?;
      }else{
        debug!("{} Skip close command since the connection is already up.", n!(inner));
        //client_utils::future_ok((spawner, inner, multiplexer, err))
        continue;
      }
    }
    else if command.kind.is_split() {
      let (resp_tx, key) = match command.kind.take_split() {
        Ok(mut i) => i.take(),
        Err(e) => {
          error!("{} Invalid split command: {:?}", ne!(inner), e);
          continue;
        }
      };

      if let Some(resp_tx) = resp_tx {
        let res = multiplexer.sinks.centralized_configs(key);
        let _ = resp_tx.send(res);
        continue;
      }else{
        error!("{} Invalid split command missing response sender.", ne!(inner));
        continue;
      }
    }
    else {
      if command.kind == RedisCommandKind::Quit {
        debug!("{} Setting state to disconnecting on quit command.", n!(inner));
        client_utils::set_client_state(&inner.state, ClientState::Disconnecting);
      }

      let (tx, rx) = oneshot_channel();
      multiplexer.set_last_command_callback(Some(tx));

      let write_ft = multiplexer.write_and_take_command(&inner, command);
      let result = write_ft.await;
      // multiplexer.set_last_request(Some(command));

      if let Err(e) = result {
        // if an error while writing then check reconnect policy and try to reconnect, build multiplexer state and cluster state
        warn!("{} Error writing command: {:?}", nw!(inner), e);
        if let Some(ref mut p) = inner.policy.write().deref_mut() {
          p.reset_attempts();
        }

        let last_command = match multiplexer.take_last_request() {
          Some(c) => c,
          None => return Err(RedisError::new(
            RedisErrorKind::Unknown, "Missing last command rebuilding connection."
          ))
        };

        rebuild_connection(spawner, inner, multiplexer, false, last_command).await?;
        continue;
      } else {
        let result = rx.await;

        // if an error occurs waiting on the response then check the reconnect policy and try to reconnect,
        // then build multiplexer state, otherwise move on to the next command
        debug!("{} Callback message recv: {:?}", n!(inner), result);

        match result {
          Ok(Some((last_command, error))) => {
            if let Some(ref mut p) = inner.policy.write().deref_mut() {
              p.reset_attempts();
            }

            rebuild_connection(spawner, inner, multiplexer, false, last_command).await?;
          },
          Ok(None) => {
            continue; // FIXME: replace with NOP
            // client_utils::future_ok((spawner, inner, multiplexer, None))
          },
          Err(e) => {
            return Err(e.into())
            //client_utils::future_error(e)
          }
        }
      }
    }
  }

  // FIXME: the original logic would have allowed rebuild_connection to succesfully
  // sett an error code which would have been propagated through the fold to the end
  // Check whether that was just a side-effect of maintaining the tuple for the
  // fold, or if that's something we still need to handle.
  Ok(None)

  //Box::new(lazy(move || {
  /*
  lazy(move |_| {
    if multiplexer.is_clustered() {
      build_clustered_multiplexer(spawner, inner, multiplexer, true)
    }else{
      build_centralized_multiplexer(spawner, inner, multiplexer, true)
    }
  })
  .and_then(move |(spawner, inner, multiplexer)| {
    rx.from_err::<RedisError>().fold((spawner, inner, multiplexer, None), |(spawner, inner, multiplexer, err): CommandLoopState, mut command: RedisCommand| {
      debug!("{} Handling redis command {:?}", n!(inner), command.kind);
      client_utils::decr_atomic(&inner.cmd_buffer_len);

      if command.kind.is_close() {
        debug!("{} Recv close command on the command stream.", n!(inner));

        if client_utils::read_client_state(&inner.state) == ClientState::Disconnected {
          // use a ping command as last command
          let last_command = RedisCommand::new(RedisCommandKind::Ping, vec![], None);

          rebuild_connection(spawner, inner, multiplexer, false, last_command)
        }else{
          debug!("{} Skip close command since the connection is already up.", n!(inner));
          client_utils::future_ok((spawner, inner, multiplexer, err))
        }
      }else if command.kind.is_split() {
        let (resp_tx, key) = match command.kind.take_split() {
          Ok(mut i) => i.take(),
          Err(e) => {
            error!("{} Invalid split command: {:?}", ne!(inner), e);
            return client_utils::future_ok((spawner, inner, multiplexer, err));
          }
        };

        if let Some(resp_tx) = resp_tx {
          let res = multiplexer.sinks.centralized_configs(key);
          let _ = resp_tx.send(res);

          client_utils::future_ok((spawner, inner, multiplexer, err))
        }else{
          error!("{} Invalid split command missing response sender.", ne!(inner));
          client_utils::future_ok((spawner, inner, multiplexer, err))
        }
      }else{
        if command.kind == RedisCommandKind::Quit {
          debug!("{} Setting state to disconnecting on quit command.", n!(inner));
          client_utils::set_client_state(&inner.state, ClientState::Disconnecting);
        }

        let (tx, rx) = oneshot_channel();
        multiplexer.set_last_command_callback(Some(tx));

        let write_ft = multiplexer.write_command(&inner, &mut command);
        multiplexer.set_last_request(Some(command));

        Box::new(write_ft.then(move |result| {
          if let Err(e) = result {
            // if an error while writing then check check reconnect policy and try to reconnect, build multiplexer state and cluster state
            warn!("{} Error writing command: {:?}", nw!(inner), e);
            if let Some(ref mut p) = inner.policy.write().deref_mut() {
              p.reset_attempts();
            }

            let last_command = match multiplexer.take_last_request() {
              Some(c) => c,
              None => return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Missing last command rebuilding connection."
              ))
            };

            rebuild_connection(spawner, inner, multiplexer, false, last_command)
          }else{
            Box::new(rx.from_err::<RedisError>().then(move |result| {
              // if an error occurs waiting on the response then check the reconnect policy and try to reconnect,
              // then build multiplexer state, otherwise move on to the next command

              debug!("{} Callback message recv: {:?}", n!(inner), result);

              match result {
                Ok(Some((last_command, error))) => {
                  if let Some(ref mut p) = inner.policy.write().deref_mut() {
                    p.reset_attempts();
                  }

                  rebuild_connection(spawner, inner, multiplexer, false, last_command)
                },
                Ok(None) => {
                  client_utils::future_ok((spawner, inner, multiplexer, None))
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
  //}))
  }).await
  */
}

/// Initialize a connection to the Redis server.
#[cfg(not(feature="mocks"))]
pub async fn connect(spawner: &Spawner, inner: Arc<RedisClientInner>) -> Result<Option<RedisError>, RedisError> {
  //Box<Future<Item=Option<RedisError>, Error=RedisError>> {
  client_utils::set_client_state(&inner.state, ClientState::Connecting);

  let result = create_commands_ft(spawner.clone(), inner.clone()).await;
  if let Err(ref e) = result {
    if e.is_canceled() {
      debug!("{} Suppressing canceled redis error: {:?}", n!(inner), e);

      Ok(None)
    }else{
      result
    }
  }else{
    result
  }
  
  /*
  create_commands_ft(spawner.clone(), inner.clone()).then(move |result| {
    if let Err(ref e) = result {
      if e.is_canceled() {
        debug!("{} Suppressing canceled redis error: {:?}", n!(inner), e);

        Ok(None)
      }else{
        result
      }
    }else{
      result
    }
  })
  */
}

#[cfg(feature="mocks")]
pub fn connect(spawner: &Spawner, inner: Arc<RedisClientInner>) -> Box<Future<Item=Option<RedisError>, Error=RedisError>> {
  client_utils::set_client_state(&inner.state, ClientState::Connected);

  Box::new(mocks::create_commands_ft(spawner.clone(), inner.clone()).then(move |result| {
    if let Err(ref e) = result {
      if e.is_canceled() {
        debug!("{} Suppressing canceled redis error: {:?}", n!(inner), e);

        Ok(None)
      }else{
        result
      }
    }else{
      result
    }
  }))
}
