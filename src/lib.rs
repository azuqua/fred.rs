#![allow(unused_mut)]

//! # Fred
//!
//! A client library for Redis based on [Futures](https://github.com/alexcrichton/futures-rs) and [Tokio](https://tokio.rs/).
//!
//!
//! ```
//! extern crate fred;
//! extern crate tokio_core;
//! extern crate futures;
//!
//! use fred::RedisClient;
//! use fred::types::*;
//!
//! use tokio_core::reactor::Core;
//! use futures::{
//!   Future,
//!   Stream
//! };
//!
//! fn main() {
//!   let config = RedisConfig::default();
//!
//!   let mut core = Core::new().unwrap();
//!   let handle = core.handle();
//!
//!   println!("Connecting to {:?}...", config);
//!
//!   let client = RedisClient::new(config);
//!   let connection = client.connect(&handle);
//!
//!   let commands = client.on_connect().and_then(|client| {
//!     println!("Client connected.");
//!
//!     client.select(0)
//!   })
//!   .and_then(|client| {
//!     println!("Selected database.");
//!
//!     client.info(None)
//!   })
//!   .and_then(|(client, info)| {
//!     println!("Redis server info: {}", info);
//!
//!     client.get("foo")
//!   })
//!   .and_then(|(client, result)| {
//!     println!("Got foo: {:?}", result);
//!
//!     client.set("foo", "bar", Some(Expiration::PX(1000)), Some(SetOptions::NX))
//!   })
//!   .and_then(|(client, result)| {
//!     println!("Set 'bar' at 'foo'? {}.", result);
//!
//!     client.quit()
//!   });
//!
//!   let (reason, client) = match core.run(connection.join(commands)) {
//!     Ok((r, c)) => (r, c),
//!     Err(e) => panic!("Connection closed abruptly: {}", e)
//!   };
//!
//!   println!("Connection closed gracefully with error: {:?}", reason);
//! }
//! ```


extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_timer;
extern crate bytes;
extern crate parking_lot;
extern crate url;
extern crate crc16;

#[cfg(feature="metrics")]
extern crate chrono;

#[cfg(feature="sync")]
extern crate boxfnonce;

#[cfg(feature="enable-flame")]
extern crate flame;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

#[cfg(feature="enable-tls")]
extern crate native_tls;
#[cfg(feature="enable-tls")]
extern crate tokio_tls;


#[macro_use]
mod _flame;

#[macro_use]
mod utils;

mod multiplexer;

#[cfg(feature="metrics")]
/// Structs for tracking latency and payload size metrics.
pub mod metrics;

#[cfg(not(feature="metrics"))]
mod metrics;

use metrics::{
  SizeTracker,
  LatencyTracker
};

#[cfg(feature="metrics")]
use metrics::{
  LatencyMetrics,
  SizeMetrics
};

/// Error handling.
pub mod error;
/// Configuration options, return value types, etc.
pub mod types;

/// `Send` and `Sync` wrappers for the client.
#[cfg(feature="sync")]
pub mod sync;

#[cfg(feature="fuzz")]
pub mod protocol;
#[cfg(not(feature="fuzz"))]
mod protocol;

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::hash::Hash;

use std::collections::{
  HashMap,
  VecDeque
};

use parking_lot::RwLock;

use tokio_core::reactor::Handle;

use futures::{
  Future,
  Stream
};
use futures::sync::oneshot::{
  Sender as OneshotSender,
  channel as oneshot_channel
};

use error::{
  RedisErrorKind,
  RedisError
};

use types::{
  SetOptions,
  Expiration,
  InfoKind,
  ClientState,
  RedisKey,
  RedisValue,
  RedisValueKind,
  RedisConfig,
  ReconnectPolicy,
  MultipleKeys,
  ASYNC
};

use multiplexer::{
  ConnectionFuture
};

use protocol::types::{
  RedisCommand,
  RedisCommandKind
};

use futures::sync::mpsc::{
  UnboundedSender,
  unbounded
};

use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;

#[cfg(feature="mock")]
mod mocks;

/// A Redis client.
#[derive(Clone)]
pub struct RedisClient {
  // The state of the underlying connection
  state: Arc<RwLock<ClientState>>,
  // The redis config used for initializing connections
  config: Rc<RefCell<RedisConfig>>,
  // An mpsc sender for errors to `on_error` streams
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  // An mpsc sender for commands to the multiplexer
  command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  // An mpsc sender for pubsub messages to `on_message` streams
  message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  // An mpsc sender for reconnection events to `on_reconnect` streams
  reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
  // MPSC senders for `on_connect` futures
  connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
  // A flag used to determine if the client was intentionally closed. This is used in the multiplexer reconnect logic
  // to determine if `quit` was called while the client was waiting to reconnect.
  closed: Arc<RwLock<bool>>,
  // Senders to remote handles around this client instance. Since forwarding messages between futures and streams itself
  // requires creating and running another future it is quite tedious to do across threads with the command stream pattern.
  // This field exists to allow remotes to register their own `on_connect` callbacks directly on the client.
  remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
  /// Latency metrics tracking, enabled with the feature `metrics`.
  latency_stats: Arc<RwLock<LatencyTracker>>,
  /// Payload size metrics, enabled with the feature `metrics`.
  size_stats: Arc<RwLock<SizeTracker>>
}

impl fmt::Debug for RedisClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisClient]")
  }
}

impl RedisClient {

  /// Create a new `RedisClient` instance.
  pub fn new(config: RedisConfig) -> RedisClient {
    let state = ClientState::Disconnected;
    let latency = LatencyTracker::default();
    let size = SizeTracker::default();

    RedisClient {
      config: Rc::new(RefCell::new(config)),
      state: Arc::new(RwLock::new(state)),
      error_tx: Rc::new(RefCell::new(VecDeque::new())),
      command_tx: Rc::new(RefCell::new(None)),
      message_tx: Rc::new(RefCell::new(VecDeque::new())),
      reconnect_tx: Rc::new(RefCell::new(VecDeque::new())),
      connect_tx: Rc::new(RefCell::new(VecDeque::new())),
      closed: Arc::new(RwLock::new(false)),
      remote_tx: Rc::new(RefCell::new(VecDeque::new())),
      latency_stats: Arc::new(RwLock::new(latency)),
      size_stats: Arc::new(RwLock::new(size))
    }
  }

  /// Close the connection to the Redis server. The returned future resolves when the command has been written to the socket,
  /// not when the connection has been fully closed. Some time after this future resolves the future returned by `connect`
  /// or `connect_with_policy` will resolve, and that indicates that the connection has been fully closed.
  ///
  /// This function will also close all error, message, and reconnection event streams.
  ///
  /// Note: This function will immediately succeed if the client is already disconnected. This is to allow `quit` to be used
  /// a means to break out from reconnect logic. If this function is called while the client is waiting to attempt to reconnect
  /// then when it next wakes up to try to reconnect it will instead break out with a `RedisErrorKind::Canceled` error.
  /// This in turn will resolve the future returned by `connect` or `connect_with_policy` some time later.
  pub fn quit(self) -> Box<Future<Item=Self, Error=RedisError>> {
    debug!("Closing Redis connection.");

    // need to lock the closed flag so any reconnect logic running in another thread doesn't screw this up,
    // but we also don't want to hold the lock if the client is connected
    let exit_early = {
      let mut closed_guard = self.closed.write();
      let mut closed_ref = closed_guard.deref_mut();

      if self.state() != ClientState::Connected {
        if *closed_ref {
          // client is already waiting to quit
          true
        }else{
          *closed_ref = true;

          true
        }
      }else{
        false
      }
    };

    // close anything left over from previous connections or reconnection attempts
    multiplexer::utils::close_error_tx(&self.error_tx);
    multiplexer::utils::close_reconnect_tx(&self.reconnect_tx);
    multiplexer::utils::close_messages_tx(&self.message_tx);
    multiplexer::utils::close_connect_tx(&self.connect_tx, &self.remote_tx);

    if exit_early {
      utils::future_ok(self)
    }else{
      Box::new(utils::request_response(&self.command_tx, &self.state, || {
        Ok((RedisCommandKind::Quit, vec![]))
      }).and_then(|_| {
        Ok(self)
      }))
    }
  }

  /// Read the state of the underlying connection.
  pub fn state(&self) -> ClientState {
    let state_guard = self.state.read();
    state_guard.deref().clone()
  }

  #[cfg(feature="metrics")]
  /// Read latency metrics across all commands.
  pub fn read_latency_metrics(&self) -> LatencyMetrics {
    metrics::read_latency_stats(&self.latency_stats)
  }

  #[cfg(feature="metrics")]
  /// Read and consume latency metrics, resetting their values afterwards.
  pub fn take_latency_metrics(&self) -> LatencyMetrics {
    metrics::take_latency_stats(&self.latency_stats)
  }

  #[cfg(feature="metrics")]
  /// Read payload size metrics across all commands.
  pub fn read_size_metrics(&self) -> SizeMetrics {
    metrics::read_size_stats(&self.size_stats)
  }

  #[cfg(feature="metrics")]
  /// Read and consume payload size metrics, resetting their values afterwards.
  pub fn take_size_metrics(&self) -> SizeMetrics {
    metrics::take_size_stats(&self.size_stats)
  }

  #[doc(hidden)]
  pub fn metrics_trackers_cloned(&self) -> (Arc<RwLock<LatencyTracker>>, Arc<RwLock<SizeTracker>>) {
    (self.latency_stats.clone(), self.size_stats.clone())
  }

  /// Read a clone of the internal connection state. Used internally by remote wrappers.
  #[doc(hidden)]
  #[cfg(feature="sync")]
  pub fn state_cloned(&self) -> Arc<RwLock<ClientState>> {
    self.state.clone()
  }

  /// Read a clone of the internal message senders. Used internally by remote wrappers.
  #[doc(hidden)]
  #[cfg(feature="sync")]
  pub fn messages_cloned(&self) -> Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>> {
    self.message_tx.clone()
  }

  /// Read a clone of the internal error senders. Used internally by remote wrappers.
  #[doc(hidden)]
  #[cfg(feature="sync")]
  pub fn errors_cloned(&self) -> Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>> {
    self.error_tx.clone()
  }

  /// Register a remote `on_connect` callback. This is only used internally.
  #[doc(hidden)]
  #[cfg(feature="sync")]
  pub fn register_connect_callback(&self, tx: OneshotSender<Result<(), RedisError>>) {
    let mut remote_tx_refs = self.remote_tx.borrow_mut();
    remote_tx_refs.push_back(tx);
  }

  /// Connect to the Redis server. The returned future will resolve when the connection to the Redis server has been fully closed by both ends.
  ///
  /// The `on_connect` function can be used to be notified when the client first successfully connects.
  pub fn connect(&self, handle: &Handle) -> ConnectionFuture {
    fry!(utils::check_client_state(ClientState::Disconnected, &self.state));
    fry!(utils::check_and_set_closed_flag(&self.closed, false));

    let (config, state, error_tx, message_tx, command_tx, connect_tx, reconnect_tx, remote_tx) = (
      self.config.clone(),
      self.state.clone(),
      self.error_tx.clone(),
      self.message_tx.clone(),
      self.command_tx.clone(),
      self.connect_tx.clone(),
      self.reconnect_tx.clone(),
      self.remote_tx.clone()
    );

    debug!("Connecting to Redis server.");
    multiplexer::init(self.clone(), handle, config, state, error_tx, message_tx, command_tx, connect_tx, reconnect_tx, remote_tx)
  }

  /// Connect to the Redis server with a `ReconnectPolicy` to apply if the connection closes due to an error.
  /// The returned future will resolve when `max_attempts` is reached on the `ReconnectPolicy`.
  ///
  /// Use the `on_error` and `on_reconnect` functions to be notified when the connection dies or successfully reconnects.
  /// Note that when the client automatically reconnects it will *not* re-select the previously selected database,
  /// nor will it re-subscribe to any pubsub channels. Use `on_reconnect` to implement that functionality if needed.
  ///
  /// Additionally, `on_connect` can be used to be notified when the client first successfully connects, since sometimes
  /// some special initialization is needed upon first connecting.
  pub fn connect_with_policy(&self, handle: &Handle, mut policy: ReconnectPolicy) -> Box<Future<Item=(), Error=RedisError>> {
    fry!(utils::check_client_state(ClientState::Disconnected, &self.state));
    fry!(utils::check_and_set_closed_flag(&self.closed, false));

    let (client, config, state, error_tx, message_tx, command_tx, reconnect_tx, connect_tx, closed, remote_tx) = (
      self.clone(),
      self.config.clone(),
      self.state.clone(),
      self.error_tx.clone(),
      self.message_tx.clone(),
      self.command_tx.clone(),
      self.reconnect_tx.clone(),
      self.connect_tx.clone(),
      self.closed.clone(),
      self.remote_tx.clone()
    );

    policy.reset_attempts();
    debug!("Connecting to Redis server with reconnect policy.");

    multiplexer::init_with_policy(client, handle, config, state, closed, error_tx, message_tx, command_tx, reconnect_tx, connect_tx, remote_tx, policy)
  }

  /// Listen for successful reconnection notifications. When using a config with a `ReconnectPolicy` the future
  /// returned by `connect_with_policy` will not resolve until `max_attempts` is reached, potentially running forever
  /// if set to 0. This function can be used to receive notifications whenever the client successfully reconnects
  /// in order to select the right database again, re-subscribe to channels, etc. A reconnection event is also
  /// triggered upon first connecting.
  pub fn on_reconnect(&self) -> Box<Stream<Item=Self, Error=RedisError>> {
    let (tx, rx) = unbounded();

    {
      let mut reconnect_ref = self.reconnect_tx.borrow_mut();
      reconnect_ref.push_back(tx);
    }

    Box::new(rx.from_err::<RedisError>())
  }

  /// Returns a future that resolves when the client connects to the server.
  /// If the client is already connected this future will resolve immediately.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs
  /// to occur only on the next connection vs subsequent connections.
  pub fn on_connect(&self) -> Box<Future<Item=Self, Error=RedisError>> {
    if utils::read_client_state(&self.state) == ClientState::Connected {
      return utils::future_ok(self.clone());
    }

    let (tx, rx) = oneshot_channel();

    {
      let mut connect_ref = self.connect_tx.borrow_mut();
      connect_ref.push_back(tx);
    }

    Box::new(rx.from_err::<RedisError>().flatten())
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// Similar to `on_message`, this function does not need to be called again if the connection goes down.
  pub fn on_error(&self) -> Box<Stream<Item=RedisError, Error=RedisError>> {
    let (tx, rx) = unbounded();

    {
      let mut error_tx_ref = self.error_tx.borrow_mut();
      error_tx_ref.push_back(tx);
    }

    Box::new(rx.from_err::<RedisError>())
  }

  /// Listen for `(channel, message)` tuples on the PubSub interface.
  ///
  /// If the connection to the Redis server goes down for any reason this function does *not* need to be called again.
  /// Messages will start appearing on the original stream after `subscribe` is called again.
  pub fn on_message(&self) -> Box<Stream<Item=(String, RedisValue), Error=RedisError>> {
    let (tx, rx) = unbounded();

    {
      let mut message_tx_ref = self.message_tx.borrow_mut();
      message_tx_ref.push_back(tx);
    }

    Box::new(rx.from_err::<RedisError>())
  }

  /// Whether or not the client is using a clustered Redis deployment.
  pub fn is_clustered(&self) -> bool {
    utils::is_clustered(&self.config)
  }

  /// Split a clustered redis client into a list of centralized clients for each master node in the cluster.
  ///
  /// This is an expensive operation and should not be used frequently, if possible.
  pub fn split_cluster(&self, handle: &Handle) -> Box<Future<Item=Vec<(RedisClient, RedisConfig)>, Error=RedisError>> {
    if utils::is_clustered(&self.config) {
      utils::split(&self.command_tx, &self.config, handle)
    }else{
      utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, "Client is not using a clustered deployment."
      ))
    }
  }

  /// Select the database this client should use.
  ///
  /// https://redis.io/commands/select
  pub fn select(self, db: u8) -> Box<Future<Item=Self, Error=RedisError>> {
    debug!("Selecting Redis database {}", db);

    Box::new(utils::request_response(&self.command_tx, &self.state, || {
      Ok((RedisCommandKind::Select, vec![RedisValue::from(db)]))
    }).and_then(|frame| {
      match frame.into_single_result() {
        Ok(_) => Ok(self),
        Err(e) => Err(e)
      }
    }))
  }

  /// Read info about the Redis server.
  ///
  /// https://redis.io/commands/info
  pub fn info(self, section: Option<InfoKind>) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    let section = section.map(|k| k.to_string());

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let vec = match section {
        Some(s) => vec![RedisValue::from(s)],
        None => vec![]
      };

      Ok((RedisCommandKind::Info, vec))
    }).and_then(|frame| {
      match frame.into_single_result() {
        Ok(resp) => {
          let kind = resp.kind();

          match resp.into_string() {
            Some(s) => Ok((self, s)),
            None => Err(RedisError::new(
              RedisErrorKind::Unknown, format!("Invalid INFO response. Expected String, found {:?}", kind)
            ))
          }
        },
        Err(e) => Err(e)
      }
    }))
  }

  /// Ping the Redis server.
  ///
  /// https://redis.io/commands/ping
  pub fn ping(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    debug!("Pinging Redis server.");

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Ping, vec![]))
    }).and_then(|frame| {
      debug!("Received Redis ping response.");

      match frame.into_single_result() {
        Ok(resp) => {
          let kind = resp.kind();

          match resp.into_string() {
            Some(s) => Ok((self, s)),
            None => Err(RedisError::new(
              RedisErrorKind::Unknown, format!("Invalid PING response. Expected String, found {:?}", kind)
            ))
          }
        },
        Err(e) => Err(e)
      }
    }))
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// https://redis.io/commands/subscribe
  pub fn subscribe<T: Into<String>>(self, channel: T) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    // note: if this ever changes to take in more than one channel then some additional work must be done
    // in the multiplexer to associate multiple responses with a single request
    let channel = channel.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Subscribe, vec![channel.into()]))
    }).and_then(|frame| {
      let mut results = frame.into_results()?;

      // last value in the array is number of channels
      let count = match results.pop() {
        Some(c) => match c.into_u64() {
          Some(i) => i,
          None => return Err(RedisError::new(
            RedisErrorKind::Unknown, "Invalid SUBSCRIBE channel count response."
          ))
        },
        None => return Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid SUBSCRIBE response."
        ))
      };

      Ok((self, count as usize))
    }))
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// https://redis.io/commands/unsubscribe
  pub fn unsubscribe<T: Into<String>>(self, channel: T) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    // note: if this ever changes to take in more than one channel then some additional work must be done
    // in the multiplexer to associate mutliple responses with a single request

    let channel = channel.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Unsubscribe, vec![channel.into()]))
    }).and_then(|frame| {
      let mut results = frame.into_results()?;

      // last value in the array is number of channels
      let count = match results.pop() {
        Some(c) => match c.into_u64() {
          Some(i) => i,
          None => return Err(RedisError::new(
            RedisErrorKind::Unknown, "Invalid UNSUBSCRIBE channel count response."
          ))
        },
        None => return Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid UNSUBSCRIBE response."
        ))
      };

      Ok((self, count as usize))
    }))
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// https://redis.io/commands/publish
  pub fn publish<T: Into<String>, V: Into<RedisValue>>(self, channel: T, message: V) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    let channel = channel.into();
    let message = message.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      let count = match resp.into_i64() {
        Some(c) => c,
        None => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid PUBLISH response."
        ))
      };

      Ok((self, count))
    }))
  }

  /// Read a value from Redis at `key`.
  ///
  /// https://redis.io/commands/get
  pub fn get<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    let _guard = flame_start!("redis:get:1");
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Get, vec![key.into()]))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:get:2");
      let resp = frame.into_single_result()?;

      let resp = if resp.kind() == RedisValueKind::Null {
        None
      } else {
        Some(resp)
      };

      Ok((self, resp))
    }))
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// https://redis.io/commands/set
  pub fn set<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    let _guard = flame_start!("redis:set:1");
    let (key, value) = (key.into(), value.into());

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let _guard = flame_start!("redis:set:2");
      let mut args = vec![key.into(), value.into()];

      if let Some(expire) = expire {
        let (k, v) = expire.into_args();
        args.push(k.into());
        args.push(v.into());
      }
      if let Some(options) = options {
        args.push(options.to_string().into());
      }

      Ok((RedisCommandKind::Set, args))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:set:3");
      let resp = frame.into_single_result()?;

      Ok((self, resp.kind() != RedisValueKind::Null))
    }))
  }

  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// https://redis.io/commands/auth
  pub fn auth<V: Into<String>>(self, value: V) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    let value = value.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Auth, vec![value.into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp.into_string() {
        Some(s) => Ok((self, s)),
        None => Err(RedisError::new(
          RedisErrorKind::Auth, "AUTH denied."
        ))
      }
    }))
  }

  /// Instruct Redis to start an Append Only File rewrite process. Returns ok.
  ///
  /// https://redis.io/commands/bgrewriteaof
  pub fn bgrewriteaof(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::BgreWriteAof, vec![]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp.into_string() {
        Some(s) => Ok((self, s)),
        None => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid BGREWRITEAOF."
        ))
      }
    }))
  }


  /// Save the DB in background. Returns ok.
  ///
  /// https://redis.io/commands/bgsave
  pub fn bgsave(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::BgSave, vec![]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp.into_string() {
        Some(s) => Ok((self, s)),
        None => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid BgSave response."
        ))
      }
    }))
  }

  /// Returns information and statistics about the client connections.
  ///
  /// https://redis.io/commands/client-list
  pub fn client_list(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args = vec![];

      Ok((RedisCommandKind::ClientList, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp.into_string() {
        Some(s) => Ok((self, s)),
        None => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid CLIENTLIST response."
        ))
      }
    }))
  }

  /// Returns the name of the current connection as a string, or None if no name is set.
  ///
  /// https://redis.io/commands/client-getname
  pub fn client_getname(self) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>> {
    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::ClientGetName, vec![]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp.into_string() {
        Some(s) => Ok((self, Some(s))),
        None => Ok((self, None))
      }
    }))
  }

  /// Assigns a name to the current connection. Returns ok if successful, None otherwise.
  ///
  /// https://redis.io/commands/client-setname
  pub fn client_setname<V: Into<String>>(self, name: V) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>> {
    let name = name.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::ClientSetname, vec![name.into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp.into_string() {
        Some(s) => Ok((self, Some(s))),
        None => Ok((self, None))
      }
    }))
  }

  /// Return the number of keys in the currently-selected database.
  ///
  /// https://redis.io/commands/dbsize
  pub fn dbsize(self) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::DBSize, vec![]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid DBsize response."
        ))
      }
    }))
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// https://redis.io/commands/decr
  pub fn decr<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Decr, vec![key.into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as i64)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid DECR response."
        ))
      }
    }))
  }

  /// Decrements the number stored at key by value argument. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// https://redis.io/commands/decrby
  pub fn decrby<V: Into<RedisValue>, K: Into<RedisKey>>(self, key: K, value: V) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args = vec![key.into(), value.into()];

      Ok((RedisCommandKind::DecrBy, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as i64)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid DECRBY response."
        ))
      }
    }))
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// https://redis.io/commands/del
  pub fn del<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let _guard = flame_start!("redis:del:1");
    let mut keys = keys.into().inner();
    let args: Vec<RedisValue> = keys.drain(..).map(|k| {
      k.into()
    }).collect();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Del, args))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:del:2");

      let resp = frame.into_single_result()?;

      let res = match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid DEL response."
        ))
      };

      res
    }))
  }

  /// Serialize the value stored at key in a Redis-specific format and return it as bulk string.
  /// If key does not exist None is returned
  ///
  /// https://redis.io/commands/dump
  pub fn dump<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Dump, vec![key.into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => Ok((self, Some(s))),
        RedisValue::Null => Ok((self, None)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid DUMP response."
        ))
      }
    }))
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// https://redis.io/commands/exists
  pub fn exists<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let mut keys = keys.into().inner();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = keys.drain(..).map(|k| k.into()).collect();

      Ok((RedisCommandKind::Exists, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid EXISTS response."
        ))
      }
    }))
  }

  /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// https://redis.io/commands/expire
  pub fn expire<K: Into<RedisKey>>(self, key: K, seconds: i64) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Expire, vec![
        key.into(),
        seconds.into()
      ]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => match num {
          0 => Ok((self, false)),
          1 => Ok((self, true)),
          _ => Err(RedisError::new(
            RedisErrorKind::ProtocolError, "Invalid EXPIRE response value."
          ))
        },
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid EXPIRE response."
        ))
      }
    }))
  }

  /// Set a timeout on key based on a UNIX timestamp. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// https://redis.io/commands/expireat
  pub fn expire_at<K: Into<RedisKey>>(self, key: K, timestamp: i64) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args = vec![key.into(), timestamp.into()];

      Ok((RedisCommandKind::ExpireAt, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => match num {
          0 => Ok((self, false)),
          1 => Ok((self, true)),
          _ => Err(RedisError::new(
            RedisErrorKind::ProtocolError, "Invalid EXPIREAT response value."
          ))
        },
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid EXPIREAT response."
        ))
      }
    }))
  }

  /// Delete the keys in all databases.
  /// Returns a string reply.
  ///
  /// https://redis.io/commands/flushall
  pub fn flushall(self, async: bool) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    let args = if async {
      vec![ASYNC.into()]
    }else{
      Vec::new()
    };

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::FlushAll, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => Ok((self, s)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid FLUSHALL response."
        ))
      }
    }))
  }

  /// Delete all the keys in the currently selected database.
  /// Returns a string reply.
  ///
  /// https://redis.io/commands/flushalldb
  pub fn flushdb(self, async: bool) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    let args = if async {
      vec![ASYNC.into()]
    }else{
      Vec::new()
    };

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::FlushDB, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => Ok((self, s)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid FLUSHALLDB response."
        ))
      }
    }))
  }

  /// Returns the substring of the string value stored at key, determined by the offsets start and end (both inclusive).
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// https://redis.io/commands/getrange
  pub fn getrange<K: Into<RedisKey>> (self, key: K, start: usize, end: usize) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    let key = key.into();
    let start = fry!(RedisValue::from_usize(start));
    let end = fry!(RedisValue::from_usize(end));

    let args = vec![
      key.into(),
      start,
      end
    ];

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::GetRange, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => Ok((self, s)),
        _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid GETRANGE response."
        ))
      }
    }))
  }

  /// Atomically sets key to value and returns the old value stored at key.
  /// Returns error if key does not hold string value. Returns None if key does not exist.
  ///
  /// https://redis.io/commands/getset
  pub fn getset<V: Into<RedisValue>, K: Into<RedisKey>> (self, key: K, value: V) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    let (key, value) = (key.into(), value.into());

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = vec![key.into(), value.into()];

      Ok((RedisCommandKind::GetSet, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Null => Ok((self, None)),
        _ => Ok((self, Some(resp)))
      }
    }))
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// https://redis.io/commands/hdel
  pub fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>> (self, key: K, fields: F) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let _guard = flame_start!("redis:hdel:1");

    let key = key.into();
    let mut fields = fields.into().inner();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let _guard = flame_start!("redis:hdel:2");

      let mut args: Vec<RedisValue> = Vec::with_capacity(fields.len() + 1);
      args.push(key.into());

      for field in fields.drain(..) {
        args.push(field.into());
      }

      Ok((RedisCommandKind::HDel, args))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:hdel:3");
      let resp = frame.into_single_result()?;

      let res = match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid HDEL response."
        ))
      };

      res
    }))
  }

  /// Returns `true` if `field` exists on `key`.
  ///
  /// https://redis.io/commands/hexists
  pub fn hexists<F: Into<RedisKey>, K: Into<RedisKey>> (self, key: K, field: F) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    let key = key.into();
    let field = field.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = vec![key.into(), field.into()];

      Ok((RedisCommandKind::HExists, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => match num {
          0 => Ok((self, false)),
          1 => Ok((self, true)),
          _ => Err(RedisError::new(
            RedisErrorKind::Unknown, "Invalid HEXISTS response value."
          ))
        },
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid HEXISTS response."
        ))
      }
    }))
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// https://redis.io/commands/hget
  pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>> (self, key: K, field: F) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    let _guard = flame_start!("redis:hget:1");
    let key = key.into();
    let field = field.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = vec![key.into(), field.into()];

      Ok((RedisCommandKind::HGet, args))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:hget:2");

      let resp = frame.into_single_result()?;

      let res = match resp {
        RedisValue::Null => Ok((self, None)),
        _ => Ok((self, Some(resp)))
      };

      res
    }))
  }

  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// https://redis.io/commands/hgetall
  pub fn hgetall<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, HashMap<String, RedisValue>), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = vec![key.into()];

      Ok((RedisCommandKind::HGetAll, args))
    }).and_then(|frame| {
      let mut resp = frame.into_results()?;

      let mut map: HashMap<String, RedisValue> = HashMap::with_capacity(resp.len() / 2);

      for mut chunk in resp.chunks_mut(2) {
        let (key, val) = (chunk[0].take(), chunk[1].take());
        let key = match key {
          RedisValue::String(s) => s,
          _ => return Err(RedisError::new(
            RedisErrorKind::ProtocolError, "Invalid HGETALL response."
          ))
        };

        map.insert(key, val);
      }

      Ok((self, map))
    }))
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `incr`. If key does not exist, a new key holding a hash is created.
  /// If field does not exist the value is set to 0 before the operation is performed.
  ///
  /// https://redis.io/commands/hincrby
  pub fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>> (self, key: K, field: F, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    let (key, field) = (key.into(), field.into());

    let args: Vec<RedisValue> = vec![
      key.into(),
      field.into(),
      incr.into()
    ];

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HIncrBy, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as i64)),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid HINCRBY response."
        ))
      }
    }))
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified increment.
  /// If the field does not exist, it is set to 0 before performing the operation.
  /// Returns an error if field value contains wrong type or content/increment are not parsable.
  ///
  /// https://redis.io/commands/hincrbyfloat
  pub fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>> (self, key: K, field: F, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>> {
    let (key, field) = (key.into(), field.into());

    let args = vec![
      key.into(),
      field.into(),
      incr.to_string().into()
    ];

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HIncrByFloat, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => match s.parse::<f64>() {
          Ok(f) => Ok((self, f)),
          Err(e) => Err(RedisError::new(
            RedisErrorKind::Unknown, format!("Invalid HINCRBYFLOAT response: {:?}", e)
          ))
        },
        _ => Err(RedisError::new(
          RedisErrorKind::InvalidArgument, "Invalid HINCRBYFLOAT response."
        ))
      }
    }))
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// https://redis.io/commands/hkeys
  pub fn hkeys<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, Vec<String>), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HKeys, vec![key.into()]))
    }).and_then(|frame| {
      let mut resp = frame.into_results()?;

      let mut out = Vec::with_capacity(resp.len());
      for val in resp.drain(..) {
        let s = match val {
          RedisValue::Null => "nil".to_owned(),
          RedisValue::String(s) => s,
          _ => return Err(RedisError::new(
            RedisErrorKind::Unknown, "Invalid HKEYS response."
          ))
        };

        out.push(s);
      }

      Ok((self, out))
    }))
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// https://redis.io/commands/hlen
  pub fn hlen<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HLen, vec![key.into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid HLEN response."
        ))
      }
    }))
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// https://redis.io/commands/hmget
  pub fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>> (self, key: K, fields: F) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    let key = key.into();
    let mut fields = fields.into().inner();

    let mut args = Vec::with_capacity(fields.len() + 1);
    args.push(key.into());

    for field in fields.drain(..) {
      args.push(field.into());
    }

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HMGet, args))
    }).and_then(|frame| {
      Ok((self, frame.into_results()?))
    }))
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// https://redis.io/commands/hmset
  pub fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>> (self, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    let key = key.into();

    let mut args = Vec::with_capacity(values.len() * 2 + 1);
    args.push(key.into());

    for (field, value) in values.drain() {
      let field = field.into();
      args.push(field.into());
      args.push(value.into());
    }

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HMSet, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => Ok((self, s)),
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid HMSET response."
        ))
      }
    }))
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// https://redis.io/commands/hset
  pub fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let _guard = flame_start!("redis:hset:1");

    let key = key.into();
    let field = field.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let _guard = flame_start!("redis:hset:2");

      let args: Vec<RedisValue> = vec![key.into(), field.into(), value.into()];

      Ok((RedisCommandKind::HSet, args))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:hset:3");
      let resp = frame.into_single_result()?;

      let res = match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown , "Invalid HSET response."
        ))
      };

      res
    }))
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// https://redis.io/commands/hsetnx
  pub fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let (key, field, value) = (key.into(), field.into(), value.into());

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = vec![key.into(), field.into(), value];

      Ok((RedisCommandKind::HSetNx, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown , "Invalid HSETNX response."
        ))
      }
    }))
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// https://redis.io/commands/hstrlen
  pub fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>> (self, key: K, field: F) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    let (key, field) = (key.into(), field.into());

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      let args: Vec<RedisValue> = vec![key.into(), field.into()];

      Ok((RedisCommandKind::HStrLen, args))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(num) => Ok((self, num as usize)),
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown , "Invalid HSTRLEN response."
        ))
      }
    }))
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// https://redis.io/commands/hvals
  pub fn hvals<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::HVals, vec![key.into()]))
    }).and_then(|frame| {
      Ok((self, frame.into_results()?))
    }))
  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// https://redis.io/commands/incr
  pub fn incr<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>>  {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::Incr, vec![key.into()]))
    }).and_then(|frame| {
      let _guard = flame_start!("redis:incr:1");
      let resp = frame.into_single_result()?;

      let res = match resp {
        RedisValue::Integer(num) => Ok((self, num as i64)),
        _ => Err(RedisError::new(
          RedisErrorKind::InvalidArgument, "Invalid INCR response."
        ))
      };

      res
    }))
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// https://redis.io/commands/incrby
  pub fn incrby<K: Into<RedisKey>>(self, key: K, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::IncrBy, vec![key.into(), incr.into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::Integer(i) => Ok((self, i as i64)),
        _ => Err(RedisError::new(
          RedisErrorKind::InvalidArgument, "Invalid INCRBY response."
        ))
      }
    }))
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// https://redis.io/commands/incrbyfloat
  pub fn incrbyfloat<K: Into<RedisKey>>(self, key: K, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>> {
    let key = key.into();

    Box::new(utils::request_response(&self.command_tx, &self.state, move || {
      Ok((RedisCommandKind::IncrByFloat, vec![key.into(), incr.to_string().into()]))
    }).and_then(|frame| {
      let resp = frame.into_single_result()?;

      match resp {
        RedisValue::String(s) => match s.parse::<f64>() {
          Ok(f) => Ok((self, f)),
          Err(e) => Err(e.into())
        },
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid INCRBYFLOAT response."
        ))
      }
    }))
  }

  // TODO more commands...

}

  #[cfg(test)]
  mod tests {
    #![allow(dead_code)]
    #![allow(unused_imports)]
    #![allow(unused_variables)]
    #![allow(unused_mut)]
    #![allow(deprecated)]
    #![allow(unused_macros)]

    use super::*;
    use std::sync::Arc;
    use std::thread;


  }
