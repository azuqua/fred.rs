#![allow(dead_code)]
#![allow(unused_imports)]

pub mod utils;

mod multiplexer;

use std::sync::Arc;

use types::*;
use protocol::types::*;

use super::RedisClient;
use self::multiplexer::Multiplexer;

use error::{
  RedisError,
  RedisErrorKind
};

use super::utils as client_utils;

use futures::future::{
  loop_fn,
  Loop,
  lazy
};
use futures::Future;
use futures::sync::oneshot::{
  Sender as OneshotSender,
};
use futures::sync::mpsc::{
  UnboundedSender,
  UnboundedReceiver,
  unbounded
};
use futures::stream::Stream;
use futures::sink::Sink;
use futures::future::Either;

use ::protocol::types::Frame;

use tokio_core::reactor::{
  Handle
};

use tokio_core::net::{
  TcpStream
};
use tokio_timer::Timer;

use parking_lot::{
  RwLock
};

use std::net::{
  ToSocketAddrs,
  SocketAddr
};

use tokio_io::codec::Framed;

use futures::stream::{
  SplitSink,
  SplitStream
};

use std::ops::Deref;

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use ::metrics::{
  SizeTracker,
  LatencyTracker
};

/// A future that resolves when the connection to the Redis server closes.
pub type ConnectionFuture = Box<Future<Item=Option<RedisError>, Error=RedisError>>;

#[cfg(feature="enable-tls")]
use tokio_tls::{
  TlsConnector as TlsConnectorAsync,
  TlsStream
};
#[cfg(feature="enable-tls")]
use native_tls::{
  TlsConnector
};

#[cfg(feature="enable-tls")]
type TlsTransports = Vec<(String, Framed<TlsStream<TcpStream>, RedisCodec>)>;

#[cfg(not(feature="enable-tls"))]
type TlsTransports = Vec<(String, Framed<TcpStream, RedisCodec>)>;


type TcpTransports = Vec<(String, Framed<TcpStream, RedisCodec>)>;

#[cfg(feature="enable-tls")]
fn init_tls_transports(config: Rc<RefCell<RedisConfig>>, handle: Handle, key: Option<String>, size_stats: Arc<RwLock<SizeTracker>>, cache: ClusterKeyCache)
 -> Box<Future<Item=(Either<TlsTransports, TcpTransports>, ClusterKeyCache), Error=RedisError>>
{
  debug!("Initialize clustered tls transports.");

  Box::new(utils::create_all_transports_tls(config, handle, &cache, key, size_stats).map(move |result| {
    (Either::A(result), cache)
  }))
}

#[cfg(not(feature="enable-tls"))]
fn init_tls_transports(config: Rc<RefCell<RedisConfig>>, handle: Handle, key: Option<String>, size_stats: Arc<RwLock<SizeTracker>>, cache: ClusterKeyCache)
   -> Box<Future<Item=(Either<TlsTransports, TcpTransports>, ClusterKeyCache), Error=RedisError>>
{
  Box::new(utils::create_all_transports(config, handle, &cache, key, size_stats).map(move |result| {
    (Either::B(result), cache)
  }))
}


fn init_clustered(
  client: RedisClient,
  handle: Handle,
  config: Rc<RefCell<RedisConfig>>,
  state: Arc<RwLock<ClientState>>,
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
  reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
  remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
  cmd_queue: Rc<RefCell<VecDeque<RedisCommand>>>,
)
  -> ConnectionFuture
{
  Box::new(lazy(move || {

    let memo = (
      client,
      handle,
      config,
      state,
      error_tx,
      message_tx,
      command_tx,
      connect_tx,
      reconnect_tx,
      remote_tx
    );

    // TODO change this to queue up commands while the cluster's state is being refreshed
    loop_fn(memo, move |(client, handle, config, state, error_tx, message_tx, command_tx, connect_tx, reconnect_tx, remote_tx)| {

      let cluster_handle = handle.clone();
      let key = utils::read_auth_key(&config);

      let mult_client = client.clone();
      let mult_config = config.clone();
      let mult_message_tx = message_tx.clone();
      let mult_error_tx = error_tx.clone();
      let mult_command_tx = command_tx.clone();
      let mult_state = state.clone();
      let mult_connect_tx = connect_tx.clone();
      let mult_reconnect_tx = reconnect_tx.clone();
      let mult_remote_tx = remote_tx.clone();
      let mult_cmd_queue = cmd_queue.clone();

      let cluster_config = config.clone();
      let (latency_stats, size_stats) = client.metrics_trackers_cloned();
      let init_size_stats = size_stats.clone();

      let uses_tls = config.borrow().tls();

      let init_handle = handle.clone();

      utils::build_cluster_cache(handle.clone(), &config, size_stats.clone()).and_then(move |cache: ClusterKeyCache| {
        if uses_tls {
          init_tls_transports(cluster_config, cluster_handle, key, init_size_stats, cache)
        }else{
          Box::new(utils::create_all_transports(cluster_config, cluster_handle, &cache, key, init_size_stats).map(move |result| {
            (Either::B(result), cache)
          }))
        }
      })
      .and_then(move |(mut transports, cache)| {
        let (tx, rx): (UnboundedSender<RedisCommand>, UnboundedReceiver<RedisCommand>) = unbounded();
        let cmd_queue_tx = tx.clone();

        let (multiplexer_ft, commands_ft) = match transports {
          Either::A(transports) => {
            let multiplexer = Multiplexer::new(
              mult_config.clone(),
              mult_message_tx.clone(),
              mult_error_tx.clone(),
              mult_command_tx.clone(),
              mult_state.clone(),
              latency_stats,
              size_stats,
              mult_cmd_queue.clone()
            );
            multiplexer.sinks.set_cluster_cache(cache);

            for (server, transport) in transports.into_iter() {
              let (sink, stream) = transport.split();

              multiplexer.sinks.set_clustered_sink(server, sink);
              multiplexer.streams.add_stream(stream);
            }

            // resolves when inbound responses stop
            let multiplexer_ft = utils::create_multiplexer_ft(multiplexer.clone(), mult_state.clone());
            // resolves when outbound requests stop
            let commands_ft = utils::create_commands_ft(rx, mult_error_tx, multiplexer.clone(), mult_state.clone());

            multiplexer.drain_cmd_queue(cmd_queue_tx);

            (multiplexer_ft, commands_ft)
          },
          Either::B(transports) => {
            let multiplexer = Multiplexer::new(
              mult_config.clone(),
              mult_message_tx.clone(),
              mult_error_tx.clone(),
              mult_command_tx.clone(),
              mult_state.clone(),
              latency_stats,
              size_stats,
              mult_cmd_queue.clone()
            );
            multiplexer.sinks.set_cluster_cache(cache);

            for (server, transport) in transports.into_iter() {
              let (sink, stream) = transport.split();

              multiplexer.sinks.set_clustered_sink(server, sink);
              multiplexer.streams.add_stream(stream);
            }

            // resolves when inbound responses stop
            let multiplexer_ft = utils::create_multiplexer_ft(multiplexer.clone(), mult_state.clone());
            // resolves when outbound requests stop
            let commands_ft = utils::create_commands_ft(rx, mult_error_tx, multiplexer.clone(), mult_state.clone());

            multiplexer.drain_cmd_queue(cmd_queue_tx);

            (multiplexer_ft, commands_ft)
          }
        };
        utils::set_command_tx(&mult_command_tx, tx);

        let init_state = mult_state.clone();
        init_handle.spawn_fn(move || {
          utils::emit_connect(&mult_connect_tx, mult_remote_tx, &mult_client);
          let _ = utils::emit_reconnect(&mult_reconnect_tx, &mult_client);
          client_utils::set_client_state(&init_state, ClientState::Connected);

          Ok::<_, ()>(())
        });

        utils::create_connection_ft(commands_ft, multiplexer_ft, mult_state)
      })
      .then(move |result| {
        match result {
          Ok(_) => Ok::<Loop<_, _>, RedisError>(Loop::Break(None)),
          Err(e) => match *e.kind() {
            RedisErrorKind::Cluster => {
              Ok(Loop::Continue((client, handle, config, state, error_tx, message_tx, command_tx, connect_tx, reconnect_tx, remote_tx)))
            },
            _ => {
              utils::emit_connect_error(&connect_tx, remote_tx, &e);

              Ok(Loop::Break(Some(e)))
            }
          }
        }
      })
    }).from_err::<RedisError>()
  }))
}

#[cfg(feature="enable-tls")]
type TlsTransportSink = SplitSink<Framed<TlsStream<TcpStream>, RedisCodec>>;
#[cfg(feature="enable-tls")]
type TlsTransportStream = SplitStream<Framed<TlsStream<TcpStream>, RedisCodec>>;

#[cfg(not(feature="enable-tls"))]
type TlsTransportSink = SplitSink<Framed<TcpStream, RedisCodec>>;
#[cfg(not(feature="enable-tls"))]
type TlsTransportStream = SplitStream<Framed<TcpStream, RedisCodec>>;

type TcpTransportSink = SplitSink<Framed<TcpStream, RedisCodec>>;
type TcpTransportStream = SplitStream<Framed<TcpStream, RedisCodec>>;

type SplitTlsTransport = (TlsTransportSink, TlsTransportStream);
type SplitTcpTransport = (TcpTransportSink, TcpTransportStream);

#[cfg(feature="enable-tls")]
fn create_centralized_transport_tls(addr: &SocketAddr, handle: &Handle, config: Rc<RefCell<RedisConfig>>, state: Arc<RwLock<ClientState>>, size_stats: Arc<RwLock<SizeTracker>>)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  Box::new(utils::create_transport_tls(&addr, &handle, config, state, size_stats).map(|(sink, stream)| {
    Either::A((sink, stream))
  }))
}

#[cfg(not(feature="enable-tls"))]
fn create_centralized_transport_tls(addr: &SocketAddr, handle: &Handle, config: Rc<RefCell<RedisConfig>>, state: Arc<RwLock<ClientState>>, size_stats: Arc<RwLock<SizeTracker>>)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  Box::new(utils::create_transport_tls(&addr, &handle, config, state, size_stats).map(|(sink, stream)| {
    Either::A((sink, stream))
  }))
}

fn create_centralized_transport(addr: &SocketAddr, handle: &Handle, config: Rc<RefCell<RedisConfig>>, state: Arc<RwLock<ClientState>>, size_stats: Arc<RwLock<SizeTracker>>)
  -> Box<Future<Item=Either<SplitTlsTransport, SplitTcpTransport>, Error=RedisError>>
{
  Box::new(utils::create_transport(&addr, &handle, config, state, size_stats).map(|(sink, stream)| {
    Either::B((sink, stream))
  }))
}

fn init_centralized(
  client: RedisClient,
  handle: &Handle,
  config: Rc<RefCell<RedisConfig>>,
  state: Arc<RwLock<ClientState>>,
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
  reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
  remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
  cmd_queue: Rc<RefCell<VecDeque<RedisCommand>>>)
  -> ConnectionFuture
{
  let addr_str = fry!(utils::read_centralized_host(&config));
  let mut addr = fry!(addr_str.to_socket_addrs());

  let addr = match addr.next() {
    Some(a) => a,
    None => return client_utils::future_error(RedisError::new(
      RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
    ))
  };

  let init_handle = handle.clone();
  client_utils::set_client_state(&state, ClientState::Connecting);

  let error_connect_tx = connect_tx.clone();
  let error_remote_tx = remote_tx.clone();
  let (latency_stats, size_stats) = client.metrics_trackers_cloned();

  let transport_ft = if config.borrow().tls() {
    create_centralized_transport_tls(&addr, &handle, config.clone(), state.clone(), size_stats.clone())
  }else{
    create_centralized_transport(&addr, &handle, config.clone(), state.clone(), size_stats.clone())
  };

  Box::new(transport_ft.and_then(move |transport| {
    let (tx, rx): (UnboundedSender<RedisCommand>, UnboundedReceiver<RedisCommand>) = unbounded();
    let cmd_queue_tx = tx.clone();

    let (multiplexer_ft, commands_ft) = match transport {
      Either::A((redis_sink, redis_stream)) => {
        let multiplexer = Multiplexer::new(
          config.clone(),
          message_tx.clone(),
          error_tx.clone(),
          command_tx.clone(),
          state.clone(),
          latency_stats,
          size_stats,
          cmd_queue.clone()
        );

        multiplexer.sinks.set_centralized_sink(redis_sink);
        multiplexer.streams.add_stream(redis_stream);

        // resolves when inbound responses stop
        let multiplexer_ft = utils::create_multiplexer_ft(multiplexer.clone(), state.clone());
        // resolves when outbound requests stop
        let commands_ft = utils::create_commands_ft(rx, error_tx, multiplexer.clone(), state.clone());

        debug!("Redis client successfully connected.");
        multiplexer.drain_cmd_queue(cmd_queue_tx);

        (multiplexer_ft, commands_ft)
      },
      Either::B((redis_sink, redis_stream)) => {
        let multiplexer = Multiplexer::new(
          config.clone(),
          message_tx.clone(),
          error_tx.clone(),
          command_tx.clone(),
          state.clone(),
          latency_stats,
          size_stats,
          cmd_queue.clone()
        );

        multiplexer.sinks.set_centralized_sink(redis_sink);
        multiplexer.streams.add_stream(redis_stream);

        // resolves when inbound responses stop
        let multiplexer_ft = utils::create_multiplexer_ft(multiplexer.clone(), state.clone());
        // resolves when outbound requests stop
        let commands_ft = utils::create_commands_ft(rx, error_tx, multiplexer.clone(), state.clone());

        debug!("Redis client successfully connected.");
        multiplexer.drain_cmd_queue(cmd_queue_tx);

        (multiplexer_ft, commands_ft)
      }
    };
    utils::set_command_tx(&command_tx, tx);

    let init_state = state.clone();
    init_handle.spawn_fn(move || {
      utils::emit_connect(&connect_tx, remote_tx, &client);
      let _ = utils::emit_reconnect(&reconnect_tx, &client);
      client_utils::set_client_state(&init_state, ClientState::Connected);

      Ok::<_, ()>(())
    });

    // resolves when both sides of the channel stop
    // resolve the outer future when the socket is connected and authenticated
    // the inner future `connection_ft` resolves when the connection is closed by either end
    utils::create_connection_ft(commands_ft, multiplexer_ft, state)
  })
  .then(move |result| {
    debug!("Centralized connection future closed with result {:?}.", result);

    if let Err(ref e) = result {
      utils::emit_connect_error(&error_connect_tx, error_remote_tx, &e);
    }

    result
  }))
}

#[cfg(not(feature="mock"))]
pub fn init(
  client: RedisClient,
  handle: &Handle,
  config: Rc<RefCell<RedisConfig>>,
  state: Arc<RwLock<ClientState>>,
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
  reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
  remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
  cmd_queue: Rc<RefCell<VecDeque<RedisCommand>>>
) -> ConnectionFuture
{
  let is_clustered = {
    let config_ref = config.borrow();

    match *config_ref {
      RedisConfig::Centralized { .. } => false,
      RedisConfig::Clustered { .. } => true
    }
  };

  if is_clustered {
    init_clustered(client,handle.clone(), config, state, error_tx, message_tx, command_tx, connect_tx, reconnect_tx, remote_tx, cmd_queue)
  }else{
    init_centralized(client,handle, config, state, error_tx, message_tx, command_tx, connect_tx, reconnect_tx, remote_tx, cmd_queue)
  }
}

#[cfg(not(feature="mock"))]
pub fn init_with_policy(
  client: RedisClient,
  handle: &Handle,
  config: Rc<RefCell<RedisConfig>>,
  state: Arc<RwLock<ClientState>>,
  closed: Arc<RwLock<bool>>,
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
  connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
  remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
  cmd_queue: Rc<RefCell<VecDeque<RedisCommand>>>,
  policy: ReconnectPolicy,
) -> Box<Future<Item=(), Error=RedisError>> {

  let handle = handle.clone();
  let timer = Timer::default();

  Box::new(lazy(move || {

    loop_fn((handle, timer, policy), move |(handle, timer, policy)| {

      let (_client, _config, _state, _error_tx, _message_tx, _command_tx, _connect_tx, _reconnect_tx, _remote_tx, _cmd_queue) = (
        client.clone(),
        config.clone(),
        state.clone(),
        error_tx.clone(),
        message_tx.clone(),
        command_tx.clone(),
        connect_tx.clone(),
        reconnect_tx.clone(),
        remote_tx.clone(),
        cmd_queue.clone()
      );

      let reconnect_tx_copy = reconnect_tx.clone();
      let command_tx_copy = command_tx.clone();
      let state_copy = state.clone();
      let error_tx_copy = error_tx.clone();
      let client_copy = client.clone();
      let connect_tx_copy = connect_tx.clone();
      let message_tx_copy = message_tx.clone();
      let closed_copy = closed.clone();
      let remote_tx_copy = remote_tx.clone();

      let is_clustered = {
        let config_ref = _config.borrow();
        match *config_ref {
          RedisConfig::Centralized {..} => false,
          RedisConfig::Clustered {..} => true
        }
      };

      let connection_ft = if is_clustered {
        Box::new(init_clustered(client_copy, handle.clone(), _config, _state,
          _error_tx, _message_tx, _command_tx, _connect_tx, _reconnect_tx, _remote_tx, _cmd_queue))
      }else{
        Box::new(init_centralized(client_copy, &handle, _config, _state, _error_tx,
          _message_tx, _command_tx, _connect_tx, _reconnect_tx, _remote_tx, _cmd_queue))
      };

      Box::new(connection_ft.then(move |result| {
        utils::reconnect(handle, timer, policy, state_copy, closed_copy,error_tx_copy, message_tx_copy,
         command_tx_copy, reconnect_tx_copy, connect_tx_copy, remote_tx_copy, result)
      }))
    })
    .map(|_: ()| ())
  })
  .from_err::<RedisError>())
}

#[cfg(feature="mock")]
pub use mocks::init;
#[cfg(feature="mock")]
pub use mocks::init_with_policy;

