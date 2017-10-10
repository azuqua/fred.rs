#![allow(unused_mut)]

use parking_lot::{
  RwLock
};

use std::sync::Arc;
use std::ops::{Deref, DerefMut};
use std::fmt;
use std::hash::Hash;

use futures::Future;
use futures::sync::oneshot::{
  channel as oneshot_channel,
  Sender as OneshotSender
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

use std::collections::HashMap;

use ::error::*;
use ::types::*;
use ::utils as client_utils;
use ::RedisClient;

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
pub struct RedisClientRemote {
  command_tx: Arc<RwLock<Option<UnboundedSender<CommandFn>>>>,
  connect_tx: Arc<RwLock<Vec<ConnectSender>>>
}

impl Clone for RedisClientRemote {
  fn clone(&self) -> Self {
    RedisClientRemote {
      command_tx: self.command_tx.clone(),
      connect_tx: self.connect_tx.clone()
    }
  }
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
      command_tx: Arc::new(RwLock::new(None)),
      connect_tx: Arc::new(RwLock::new(Vec::new()))
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

  /// Read the underlying `connect_tx` senders. Used internally to convert to an owned remote client.
  #[doc(hidden)]
  pub fn read_connect_tx(&self) -> Arc<RwLock<Vec<OneshotSender<Result<(), RedisError>>>>> {
    self.connect_tx.clone()
  }

  /// Set the underlying `connect_tx` senders. Used internally by the owned variant of the remote wrapper.
  #[doc(hidden)]
  pub fn set_connect_tx(&self, mut senders: Vec<OneshotSender<Result<(), RedisError>>>) {
    let mut connect_tx_guard = self.connect_tx.write();
    let mut connect_tx_ref = connect_tx_guard.deref_mut();

    for tx in senders.drain(..) {
      connect_tx_ref.push(tx);
    }
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

    utils::register_connect_callbacks(&self.command_tx, &self.connect_tx);

    Box::new(commands_ft)
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
      connect_tx_refs.push(tx);

      out
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

  // TODO implement the rest of the commands on RedisClient

}