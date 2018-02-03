#![allow(unused_mut)]

use parking_lot::{
  RwLock
};

use std::sync::Arc;

use std::ops::Deref;
use std::ops::DerefMut;
use std::hash::Hash;

use std::fmt;

use std::collections::{
  VecDeque,
  HashMap
};

use futures::{
  Future,
  Stream
};
use futures::sync::oneshot::{
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  UnboundedSender,
  unbounded
};

use ::error::*;
use ::types::*;
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
use super::borrowed;
use super::borrowed::RedisClientRemote as RedisClientBorrowed;

use super::commands::ConnectSender;

/// A `Send` and `Sync` wrapper around a `RedisClient`.
///
/// This module exposes the same interface as the `RedisClient` struct, but can be safely
/// sent and used across threads.
///
/// The underlying implementation uses message passing patterns to communicate with the
/// `RedisClient` instance on the event loop thread. As a result all commands, with the
/// exception of `state()`, have some added overhead due to the inter-thread communication
/// logic. The only synchronized code path on each command is reading the client's state,
/// which is wrapped in a [RwLock](https://amanieu.github.io/parking_lot/parking_lot/struct.RwLock.html).
///
/// See `examples/sync_owned.rs` for usage examples.
#[derive(Clone)]
pub struct RedisClientRemote {
  state: Arc<RwLock<Arc<RwLock<ClientState>>>>,
  // use the borrowed interface under the hood
  borrowed: Arc<RwLock<Option<RedisClientBorrowed>>>,
  // buffers for holding on_connect, on_error, and on_message mpsc senders created
  // before the underlying client is ready. upon calling init() these will be
  // drained and moved into the underlying client.
  connect_tx: Arc<RwLock<VecDeque<ConnectSender>>>,
  error_tx: Arc<RwLock<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Arc<RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  /// Latency metrics tracking, enabled with the feature `metrics`.
  latency_stats: Arc<RwLock<Option<Arc<RwLock<LatencyTracker>>>>>,
  /// Payload size metrics, enabled with the feature `metrics`.
  size_stats: Arc<RwLock<Option<Arc<RwLock<SizeTracker>>>>>
}

impl fmt::Debug for RedisClientRemote {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisClientRemote]")
  }
}

impl RedisClientRemote {

  /// Create a new, empty RedisClientRemote.
  pub fn new() -> RedisClientRemote {
    RedisClientRemote {
      state: utils::init_state(),
      borrowed: Arc::new(RwLock::new(None)),
      connect_tx: Arc::new(RwLock::new(VecDeque::new())),
      error_tx: Arc::new(RwLock::new(VecDeque::new())),
      message_tx: Arc::new(RwLock::new(VecDeque::new())),
      latency_stats: Arc::new(RwLock::new(None)),
      size_stats: Arc::new(RwLock::new(None))
    }
  }

  /// Create from a borrowed instance.
  pub fn from_borrowed(client: borrowed::RedisClientRemote) -> RedisClientRemote {
    let connect_tx = client.read_connect_tx().clone();
    let error_tx = client.read_error_tx().clone();
    let message_tx = client.read_message_tx().clone();

    let (latency, size) = {
      let (latency, size) = client.read_metrics_trackers();
      (latency.clone(), size.clone())
    };

    RedisClientRemote {
      state: client.state_cloned(),
      borrowed: Arc::new(RwLock::new(Some(client))),
      latency_stats: latency,
      size_stats: size,
      connect_tx: connect_tx,
      error_tx: error_tx,
      message_tx: message_tx
    }
  }

  /// Attempt to convert to a borrowed instance. This returns `None` if `init` has not yet been called.
  pub fn to_borrowed(&self) -> Option<borrowed::RedisClientRemote> {
    let borrowed_guard = self.borrowed.read();
    borrowed_guard.deref().clone()
  }

  /// Attempt to convert this owned instance into a borrowed instance.
  pub fn into_borrowed(self) -> Option<borrowed::RedisClientRemote> {
    let mut borrowed_guard = self.borrowed.write();
    borrowed_guard.deref_mut().take()
  }

  // Read a reference to the underlying borrowed instance.
  pub fn inner_borrowed(&self) -> &Arc<RwLock<Option<RedisClientBorrowed>>> {
    &self.borrowed
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

  /// Initialize the remote interface.
  ///
  /// This function must run on the same thread that created the `RedisClient`.
  pub fn init(&self, client: RedisClient) -> Box<Future<Item=RedisClient, Error=RedisError>> {
    let borrowed = borrowed::RedisClientRemote::new();
    utils::replace_state(&self.state, client.state_cloned());

    utils::transfer_senders(&self.connect_tx, borrowed.read_connect_tx());
    utils::transfer_senders(&self.error_tx, borrowed.read_error_tx());
    utils::transfer_senders(&self.message_tx, borrowed.read_message_tx());

    {
      let mut borrowed_guard = self.borrowed.write();
      let borrowed_ref = borrowed_guard.deref_mut();
      *borrowed_ref = Some(borrowed.clone());
    }

    {
      let (latency, size) = client.metrics_trackers_cloned();

      let mut latency_guard = self.latency_stats.write();
      let latency_ref = latency_guard.deref_mut();
      *latency_ref = Some(latency);

      let mut size_guard = self.size_stats.write();
      let size_ref = size_guard.deref_mut();
      *size_ref = Some(size);
    }

    borrowed.init(client)
  }

  /// Flush and close the Sender channel this instance receives messages through.
  pub fn close(&mut self) {
    let mut borr_guard = self.borrowed.write();
    let mut borr_ref = borr_guard.deref_mut();
    match *borr_ref {
      Some(ref mut borr) => borr.close(),
      None => {}
    };
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
      let borrowed_guard = self.borrowed.read();
      borrowed_guard.deref().is_some()
    };

    if is_initialized {
      let borrowed_guard = self.borrowed.read();
      let borrowed_ref = borrowed_guard.deref();

      let borrowed = borrowed_ref.as_ref().unwrap();
      borrowed.on_connect()
    }else{
      let (tx, rx) = oneshot_channel();

      let mut connect_tx_guard = self.connect_tx.write();
      let mut connect_tx_ref = connect_tx_guard.deref_mut();
      connect_tx_ref.push_back(tx);

      Box::new(rx.from_err::<RedisError>().flatten())
    }
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// Similar to `on_message`, this function does not need to be called again if the connection goes down.
  pub fn on_error(&self) -> Box<Stream<Item=RedisError, Error=RedisError>> {
    let is_initialized = {
      let borrowed_guard = self.borrowed.read();
      borrowed_guard.deref().is_some()
    };

    if is_initialized {
      let borrowed_guard = self.borrowed.read();
      let borrowed_ref = borrowed_guard.deref();

      let borrowed = borrowed_ref.as_ref().unwrap();
      borrowed.on_error()
    }else{
      let (tx, rx) = unbounded();

      let mut error_tx_guard = self.error_tx.write();
      let mut error_tx_ref = error_tx_guard.deref_mut();
      error_tx_ref.push_back(tx);

      Box::new(rx.from_err::<RedisError>())
    }
  }

  /// Listen for `(channel, message)` tuples on the PubSub interface.
  ///
  /// If the connection to the Redis server goes down for any reason this function does *not* need to be called again.
  /// Messages will start appearing on the original stream after `subscribe` is called again.
  pub fn on_message(&self) -> Box<Stream<Item=(String, RedisValue), Error=RedisError>> {
    let is_initialized = {
      let borrowed_guard = self.borrowed.read();
      borrowed_guard.deref().is_some()
    };

    if is_initialized {
      let borrowed_guard = self.borrowed.read();
      let borrowed_ref = borrowed_guard.deref();

      let borrowed = borrowed_ref.as_ref().unwrap();
      borrowed.on_message()
    }else{
      let (tx, rx) = unbounded();

      let mut message_tx_guard = self.message_tx.write();
      let mut message_tx_ref = message_tx_guard.deref_mut();
      message_tx_ref.push_back(tx);

      Box::new(rx.from_err::<RedisError>())
    }
  }

  /// Select the database this client should use.
  ///
  /// https://redis.io/commands/select
  pub fn select(self, db: u8) -> Box<Future<Item=Self, Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.select(db).and_then(move |_| {
        Ok(_self)
      }))
    })
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// https://redis.io/commands/subscribe
  pub fn subscribe<K: Into<String>>(self, channel: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.subscribe(channel).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// https://redis.io/commands/unsubscribe
  pub fn unsubscribe<K: Into<String>>(self, channel: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.unsubscribe(channel).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// https://redis.io/commands/publish
  pub fn publish<K: Into<String>, V: Into<RedisValue>>(self, channel: K, message: V) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.publish(channel, message).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Read a value from Redis at `key`.
  ///
  /// https://redis.io/commands/get
  pub fn get<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.get(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// https://redis.io/commands/set
  pub fn set<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.set(key, value, expire, options).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// https://redis.io/commands/del
  pub fn del<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.del(keys).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// https://redis.io/commands/decr
  pub fn decr<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.decr(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })

  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the value at key is of wrong type.
  ///
  /// https://redis.io/commands/incr
  pub fn incr<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.incr(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// https://redis.io/commands/incrby
  pub fn incrby<K: Into<RedisKey>>(self, key: K, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.incrby(key, incr).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// https://redis.io/commands/incrbyfloat
  pub fn incrbyfloat<K: Into<RedisKey>>(self, key: K, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.incrbyfloat(key, incr).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// https://redis.io/commands/hget
  pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hget(key, field).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// https://redis.io/commands/hgetall
  pub fn hgetall<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, HashMap<String, RedisValue>), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hgetall(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// https://redis.io/commands/hset
  pub fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hset(key, field, value).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// https://redis.io/commands/hdel
  pub fn hdel<K: Into<RedisKey>, F: Into<MultipleKeys>>(self, key: K, fields: F) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hdel(key, fields).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// https://redis.io/commands/hlen
  pub fn hlen<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hlen(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// https://redis.io/commands/hmget
  pub fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>> (self, key: K, fields: F) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hmget(key, fields).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// https://redis.io/commands/hmset
  pub fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>> (self, key: K, values: HashMap<F, V>) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hmset(key, values).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// https://redis.io/commands/hsetnx
  pub fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hsetnx(key, field, value).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// https://redis.io/commands/hstrlen
  pub fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>> (self, key: K, field: F) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hstrlen(key, field).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// https://redis.io/commands/hvals
  pub fn hvals<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hvals(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// https://redis.io/commands/hkeys
  pub fn hkeys<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, Vec<String>), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hkeys(key).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

  // TODO more commands...

}
