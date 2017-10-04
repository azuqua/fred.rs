#![allow(unused_mut)]

use parking_lot::{
  RwLock
};

use std::sync::Arc;

use std::ops::Deref;
use std::ops::DerefMut;

use std::fmt;

use futures::Future;
use futures::sync::oneshot::{
  channel as oneshot_channel
};

use ::error::*;
use ::types::*;
use ::RedisClient;

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
  // use the borrowed interface under the hood
  borrowed: Arc<RwLock<Option<RedisClientBorrowed>>>,
  connect_tx: Arc<RwLock<Vec<ConnectSender>>>
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
      borrowed: Arc::new(RwLock::new(None)),
      connect_tx: Arc::new(RwLock::new(Vec::new()))
    }
  }

  /// Create from a borrowed instance.
  pub fn from_borrowed(client: borrowed::RedisClientRemote) -> RedisClientRemote {
    let connect_tx = client.read_connect_tx();
    RedisClientRemote {
      borrowed: Arc::new(RwLock::new(Some(client))),
      connect_tx: connect_tx
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

  /// Initialize the remote interface.
  ///
  /// This function must run on the same thread that created the `RedisClient`.
  pub fn init(&self, client: RedisClient) -> Box<Future<Item=RedisClient, Error=RedisError>> {
    let borrowed = borrowed::RedisClientRemote::new();
    {
      let mut connect_tx_guard = self.connect_tx.write();
      let mut connect_tx_ref = connect_tx_guard.deref_mut();

      let taken: Vec<ConnectSender> = connect_tx_ref.drain(..).collect();
      borrowed.set_connect_tx(taken);
    }
    {
      let mut borrowed_guard = self.borrowed.write();
      let mut borrowed_ref = borrowed_guard.deref_mut();
      *borrowed_ref = Some(borrowed.clone());
    }

    borrowed.init(client)
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
      connect_tx_ref.push(tx);

      Box::new(rx.from_err::<RedisError>().flatten())
    }
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
  pub fn del<K: Into<RedisKey>>(self, keys: Vec<K>) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
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
  pub fn hdel<K: Into<RedisKey>, F: Into<RedisKey>>(self, key: K, fields: Vec<F>) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    utils::run_borrowed(self, move |_self, borrowed| {
      Box::new(borrowed.hdel(key, fields).and_then(move |resp| {
        Ok((_self, resp))
      }))
    })
  }

}
