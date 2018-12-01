#![allow(unused_mut)]

use parking_lot::{
  RwLock
};

use std::sync::Arc;
use std::ops::{Deref, DerefMut};
use std::fmt;
use std::hash::Hash;

use futures::future;
use futures::Future;
use futures::sync::oneshot::{
  channel as oneshot_channel,
};
use futures::sync::mpsc::{
  UnboundedSender,
  unbounded
};
use futures::stream::{
  Stream
};
use futures::sink::Sink;

use boxfnonce::SendBoxFnOnce;

use std::collections::{
  HashMap,
  VecDeque
};

use ::error::*;
use ::types::*;
use ::utils as client_utils;
use ::RedisClient;

#[cfg(feature="metrics")]
use ::metrics;

use ::metrics::{
  SizeTracker,
  LatencyTracker
};

#[cfg(feature="metrics")]
use ::metrics::{
  LatencyMetrics,
  SizeMetrics
};

use super::utils;
use super::owned;
use super::commands;
use super::commands::{
  CommandFn,
  ConnectSender
};

/// A `Send` and `Sync` wrapper around a redis client that borrows `self` on each command, instead
/// of taking ownership over `self`. This pattern gives the caller more freedom to manage ownership
/// over the value, such as when the client is wrapped with another struct.
///
/// However, this pattern is more tedious to use when commands are chained together in a single
/// chain of futures as it requires the caller to either explicitly clone or move the client into
/// those callbacks. For that reason it is not the default interface. However, if you intend on
/// wrapping the client in another struct, or an `Arc`, or something that manages ownership
/// and mutability for the inner value, then this interface is probably what you want. You
/// will still have to manually move or clone the wrapper struct into callbacks as needed,
/// but with this interface the ownership requirements are removed on all commands.
///
/// For example, one use case for this interface would be as follows:
///
/// 1. Create several `RedisClient` instances on an event loop.
/// 2. Create `RedisClientRemote` wrappers for them.
/// 3. Move each of the `RedisClientRemote` instances into another wrapper struct `Foo`.
/// 4. Wrap `Foo` in an `Arc`, and send clones of the `Arc<Foo>` to different threads.
///
/// If the commands on the `RedisClientRemote` instances required taking ownership over `self`
/// then this pattern would be much more difficult to implement, since an `Arc` only allows for
/// using functions that borrow the inner value. This interface is primarily designed to support
/// this kind of usage.
///
/// See `examples/sync_borrowed.rs` for usage examples.
#[derive(Clone)]
pub struct RedisClientRemote {
  // ew
  state: Arc<RwLock<Arc<RwLock<ClientState>>>>,
  command_tx: Arc<RwLock<Option<UnboundedSender<CommandFn>>>>,
  // buffers for holding on_connect, on_error, and on_message mpsc senders created
  // before the underlying client is ready. upon calling init() these will be
  // drained and moved into the underlying client.
  connect_tx: Arc<RwLock<VecDeque<ConnectSender>>>,
  error_tx: Arc<RwLock<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Arc<RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  /// Latency metrics tracking, enabled with the feature `metrics`.
  latency_stats: Arc<RwLock<Option<Arc<RwLock<LatencyTracker>>>>>,
  /// Payload size metrics, enabled with the feature `metrics`.
  size_stats: Arc<RwLock<Option<Arc<RwLock<SizeTracker>>>>>,
}

impl fmt::Debug for RedisClientRemote {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisClientRemote]")
  }
}

impl RedisClientRemote {

  /// Create a new, empty `RedisClientRemote`.
  pub fn new() -> RedisClientRemote {
    RedisClientRemote {
      state: utils::init_state(),
      command_tx: Arc::new(RwLock::new(None)),
      connect_tx: Arc::new(RwLock::new(VecDeque::new())),
      error_tx: Arc::new(RwLock::new(VecDeque::new())),
      message_tx: Arc::new(RwLock::new(VecDeque::new())),
      latency_stats: Arc::new(RwLock::new(None)),
      size_stats: Arc::new(RwLock::new(None))
    }
  }

  /// Convert a clone of this `borrowed::RedisClientRemote` to an `owned::RedisClientRemote`.
  pub fn to_owned(&self) -> owned::RedisClientRemote {
    owned::RedisClientRemote::from_borrowed(self.clone())
  }

  /// Convert this `borrowed::RedisClientRemote` to an `owned::RedisClientRemote`.
  pub fn into_owned(self) -> owned::RedisClientRemote {
    owned::RedisClientRemote::from_borrowed(self)
  }

  /// Read the state of the underlying connection.
  pub fn state(&self) -> ClientState {
    utils::read_state(&self.state)
  }

  /// Read a clone of the internal connection state. Used internally by remote wrappers.
  #[doc(hidden)]
  pub fn state_cloned(&self) -> Arc<RwLock<Arc<RwLock<ClientState>>>> {
    self.state.clone()
  }

  #[cfg(feature="metrics")]
  /// Read latency metrics across all commands.
  pub fn read_latency_metrics(&self) -> Option<LatencyMetrics> {
    let latency_guard = self.latency_stats.read();

    if let Some(ref latency_stats) = *latency_guard.deref() {
      Some(metrics::read_latency_stats(latency_stats))
    }else{
      None
    }
  }

  #[cfg(feature="metrics")]
  /// Read and consume latency metrics, resetting their values afterwards.
  pub fn take_latency_metrics(&self) -> Option<LatencyMetrics> {
    let latency_guard = self.latency_stats.read();

    if let Some(ref latency_stats) = *latency_guard.deref() {
      Some(metrics::take_latency_stats(latency_stats))
    }else{
      None
    }
  }

  #[cfg(feature="metrics")]
  /// Read payload size metrics across all commands.
  pub fn read_size_metrics(&self) -> Option<SizeMetrics> {
    let size_guard = self.size_stats.read();

    if let Some(ref size_stats) = *size_guard.deref() {
      Some(metrics::read_size_stats(size_stats))
    }else{
      None
    }
  }

  #[cfg(feature="metrics")]
  /// Read and consume payload size metrics, resetting their values afterwards.
  pub fn take_size_metrics(&self) -> Option<SizeMetrics> {
    let size_guard = self.size_stats.read();

    if let Some(ref size_stats) = *size_guard.deref() {
      Some(metrics::take_size_stats(size_stats))
    }else{
      None
    }
  }

  #[doc(hidden)]
  pub fn read_metrics_trackers(&self) -> (&Arc<RwLock<Option<Arc<RwLock<LatencyTracker>>>>>, &Arc<RwLock<Option<Arc<RwLock<SizeTracker>>>>>) {
    (&self.latency_stats, &self.size_stats)
  }

  /// Read the underlying `connect_tx` senders. Used internally to convert to an owned remote client.
  #[doc(hidden)]
  pub fn read_connect_tx(&self) -> &Arc<RwLock<VecDeque<ConnectSender>>> {
    &self.connect_tx
  }

  /// Read the underlying `error_tx` senders. Used internally to convert to an owned remote client.
  #[doc(hidden)]
  pub fn read_error_tx(&self) -> &Arc<RwLock<VecDeque<UnboundedSender<RedisError>>>> {
    &self.error_tx
  }

  /// Read the underlying `message_tx` senders. Used internally to convert to an owned remote client.
  #[doc(hidden)]
  pub fn read_message_tx(&self) -> &Arc<RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>> {
    &self.message_tx
  }

  /// Initialize the remote interface with an existing `RedisClient`. This must be called on the same
  /// thread that initialized the `RedisClient`.
  ///
  /// The future returned by this function runs the message passing logic between this remote instance
  /// and the `RedisClient` argument on the event loop thread.
  pub fn init(&self, client: RedisClient) -> Box<Future<Item=RedisClient, Error=RedisError>> {
    let (tx, rx) = unbounded();

    {
      let mut command_guard = self.command_tx.write();
      let mut command_ref = command_guard.deref_mut();

      if let Some(mut tx) = command_ref.take() {
        let _ = tx.close();
      }

      *command_ref = Some(tx);
    }
    utils::replace_state(&self.state, client.state_cloned());

    let (latency, size) = client.metrics_trackers_cloned();
    {
      let mut latency_guard = self.latency_stats.write();
      let latency_ref = latency_guard.deref_mut();
      *latency_ref = Some(latency);

      let mut size_guard = self.size_stats.write();
      let size_ref = size_guard.deref_mut();
      *size_ref = Some(size);
    }

    let commands_ft = rx.from_err::<RedisError>().fold((client.clone(), client), |(backup, client), func| {
      // this only fails due to mpsc errors, command errors are handled by the command fn

      func.call_tuple((client,)).and_then(move |client: Option<RedisClient>| {

        // since commands on a `RedisClient` take ownership over `self` if an error occurs the original client instance is dropped.
        // for this reason a backup clone of the client is stored so subsequent commands can still be processed
        match client {
          Some(c) => Ok((backup, c)),
          None => Ok((backup.clone(), backup))
        }
      })
    })
    .map(|(_, client)| client);

    utils::register_callbacks(&self.command_tx, &self.connect_tx, &self.error_tx, &self.message_tx);

    Box::new(commands_ft)
  }

  /// Flush and close the Sender channel this instance receives messages through.
  pub fn close(&mut self) {
    let mut tx_guard = self.command_tx.write();
    let mut tx_ref = tx_guard.deref_mut();

    if let Some(ref mut tx) = *tx_ref {
      let _ = tx.close();
    }
  }

  /// Returns a future that resolves when the underlying client connects to the server. This
  /// function can act as a convenient way of notifying a separate thread when the client has
  /// connected to the server and can begin processing commands.
  ///
  /// This can be called before `init` if needed, however the callback will not be registered
  /// on the underlying client until `init` is called. For this reason it's best to call `init`
  /// with a client that has not yet starting running its connection future.
  ///
  /// See the `examples/sync_borrowed.rs` file for usage examples.
  pub fn on_connect(&self) -> Box<Future<Item=(), Error=RedisError>> {
    let is_initialized = {
      let command_tx_guard = self.command_tx.read();
      command_tx_guard.deref().is_some()
    };

    let (tx, rx) = oneshot_channel();
    let out = Box::new(rx.from_err::<RedisError>().flatten());

    // make sure there's not a race condition here...
    if is_initialized {
      let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
        client.register_connect_callback(tx);
        client_utils::future_ok(Some(client))
      });

      match utils::send_command(&self.command_tx, func) {
        Ok(_) => out,
        Err(e) => client_utils::future_error(e)
      }
    }else{
      let mut connect_tx_guard = self.connect_tx.write();
      let mut connect_tx_refs = connect_tx_guard.deref_mut();
      connect_tx_refs.push_back(tx);

      out
    }
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// Similar to `on_message`, this function does not need to be called again if the connection goes down.
  pub fn on_error(&self) -> Box<Stream<Item=RedisError, Error=RedisError>> {
    let is_initialized = {
      let command_tx_guard = self.command_tx.read();
      command_tx_guard.deref().is_some()
    };

    let (tx, rx) = unbounded();
    let out = Box::new(rx.from_err::<RedisError>());

    if is_initialized {
      let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
        utils::transfer_sender(client.errors_cloned(), tx);
        client_utils::future_ok(Some(client))
      });

      match utils::send_command(&self.command_tx, func) {
        Ok(_) => out,
        Err(e) => Box::new(future::err(e).into_stream())
      }
    }else{
      let mut error_tx_guard = self.error_tx.write();
      let mut error_tx_ref = error_tx_guard.deref_mut();
      error_tx_ref.push_back(tx);

      out
    }
  }

  /// Listen for `(channel, message)` tuples on the PubSub interface.
  ///
  /// If the connection to the Redis server goes down for any reason this function does *not* need to be called again.
  /// Messages will start appearing on the original stream after `subscribe` is called again.
  pub fn on_message(&self) -> Box<Stream<Item=(String, RedisValue), Error=RedisError>> {
    let is_initialized = {
      let command_tx_guard = self.command_tx.read();
      command_tx_guard.deref().is_some()
    };

    let (tx, rx) = unbounded();
    let out = Box::new(rx.from_err::<RedisError>());

    if is_initialized {
      let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
        utils::transfer_sender(client.messages_cloned(), tx);
        client_utils::future_ok(Some(client))
      });

      match utils::send_command(&self.command_tx, func) {
        Ok(_) => out,
        Err(e) => Box::new(future::err(e).into_stream())
      }
    }else{
      let mut message_tx_guard = self.message_tx.write();
      let mut message_tx_ref = message_tx_guard.deref_mut();
      message_tx_ref.push_back(tx);

      out
    }
  }

  /// Select the database this client should use.
  ///
  /// https://redis.io/commands/select
  pub fn select(&self, db: u8) -> Box<Future<Item=(), Error=RedisError>> {
    let (tx, rx) = oneshot_channel();

    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      commands::select(client, tx, db)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// https://redis.io/commands/subscribe
  pub fn subscribe<K: Into<String>>(&self, channel: K) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let channel = channel.into();

    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      commands::subscribe(client, tx, channel)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// https://redis.io/commands/unsubscribe
  pub fn unsubscribe<K: Into<String>>(&self, channel: K) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let channel = channel.into();

    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      commands::unsubscribe(client, tx, channel)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// https://redis.io/commands/publish
  pub fn publish<K: Into<String>, V: Into<RedisValue>>(&self, channel: K, message: V) -> Box<Future<Item=i64, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let channel = channel.into();
    let message = message.into();

    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      commands::publish(client, tx, channel, message)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Read a value from Redis at `key`.
  ///
  /// https://redis.io/commands/get
  pub fn get<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key: RedisKey = key.into();

    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      commands::get(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// https://redis.io/commands/set
  pub fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let (key, value) = (key.into(), value.into());

    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      commands::set(client, tx, key, value, expire, options)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// https://redis.io/commands/del
  pub fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let keys = keys.into().inner();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::del(client, tx, keys)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// https://redis.io/commands/expire
  pub fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Box<Future<Item=bool, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::expire(client, tx, key, seconds)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// https://redis.io/commands/decr
  pub fn decr<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::decr(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the value at key is of wrong type.
  ///
  /// https://redis.io/commands/incr
  pub fn incr<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::incr(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// https://redis.io/commands/incrby
  pub fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Box<Future<Item=i64, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::incrby(client, tx, key, incr)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// https://redis.io/commands/incrbyfloat
  pub fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Box<Future<Item=f64, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::incrbyfloat(client, tx, key, incr)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// https://redis.io/commands/hget
  pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let (key, field) = (key.into(), field.into());

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hget(client, tx, key, field)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }


  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// https://redis.io/commands/hgetall
  pub fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=HashMap<String, RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hgetall(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// https://redis.io/commands/hset
  pub fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let (key, field, value) = (key.into(), field.into(), value.into());

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hset(client, tx, key, field, value)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// https://redis.io/commands/hdel
  pub fn hdel<K: Into<RedisKey>, F: Into<MultipleKeys>>(&self, key: K, fields: F) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();
    let fields = fields.into().inner();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hdel(client, tx, key, fields)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// https://redis.io/commands/hlen
  pub fn hlen<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hlen(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// https://redis.io/commands/hmget
  pub fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>> (&self, key: K, fields: F) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();
    let fields = fields.into().inner();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hmget(client, tx, key, fields)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// https://redis.io/commands/hmset
  pub fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>> (&self, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=String, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let mut owned_values = HashMap::with_capacity(values.len());
    for (key, val) in values.drain() {
      owned_values.insert(key.into(), val.into());
    }

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hmset(client, tx, key, owned_values)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// https://redis.io/commands/hsetnx
  pub fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (&self, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let (key, field, value) = (key.into(), field.into(), value.into());

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hsetnx(client, tx, key, field, value)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// https://redis.io/commands/hstrlen
  pub fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>> (&self, key: K, field: F) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let (key, field) = (key.into(), field.into());

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hstrlen(client, tx, key, field)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// https://redis.io/commands/hvals
  pub fn hvals<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hvals(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// https://redis.io/commands/hkeys
  pub fn hkeys<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=Vec<String>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::hkeys(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// https://redis.io/commands/llen
  pub fn llen<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::llen(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Insert all the specified values at the head of the list stored at key
  ///
  /// https://redis.io/commands/lpush
  pub fn lpush<K: Into<RedisKey>, V: Into<RedisValue>> (&self, key: K, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();
    let value = value.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::lpush(client, tx, key, value)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Removes and returns the first element of the list stored at key.
  ///
  /// https://redis.io/commands/lpop
  pub fn lpop<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::lpop(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Add all the specified values to set stored at key
  ///
  /// https://redis.io/commands/sadd
  pub fn sadd<K: Into<RedisKey>, V: Into<RedisValue>> (&self, key: K, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();
    let value = value.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::sadd(client, tx, key, value)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Remove all the specified values to set stored at key
  ///
  /// https://redis.io/commands/srem
  pub fn srem<K: Into<RedisKey>, V: Into<RedisValue>> (&self, key: K, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();
    let value = value.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::srem(client, tx, key, value)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  /// Returns all the members of the set value stored at key.
  ///
  /// https://redis.io/commands/smembers
  pub fn smembers<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
    let (tx, rx) = oneshot_channel();
    let key = key.into();

    let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
      commands::smembers(client, tx, key)
    });

    match utils::send_command(&self.command_tx, func) {
      Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
      Err(e) => client_utils::future_error(e)
    }
  }

  // TODO implement the rest of the commands on RedisClient

}
