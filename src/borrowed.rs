//! An interface for the `RedisClient` that borrows `self` on each command.
//!
//! This pattern gives the caller more freedom to manage ownership
//! over the value, such as when the client is wrapped with another struct.
//!
//! However, this pattern is more tedious to use when commands are chained together in a single
//! chain of futures as it requires the caller to either explicitly clone or move the client into
//! those callbacks. For that reason it is not the default interface used in the examples. However,
//! if you intend on wrapping the client in another struct, or an `Arc`, or something that manages ownership
//! and mutability for the inner value, then this interface is probably what you want. You
//! will still have to manually move or clone the wrapper struct into callbacks as needed,
//! but with this interface the ownership requirements are removed on all commands.
//!
//! For example, one use case for this interface would be as follows:
//!
//! 1. Create several `RedisClient` instances on an event loop.
//! 2. Move each of the `RedisClient` instances into another wrapper struct `Foo`.
//! 3. Wrap `Foo` in an `Arc`, and send clones of the `Arc<Foo>` to different threads.
//!
//! If the commands on the `RedisClient` instances required taking ownership over `self`
//! then this pattern would be much more difficult to implement, since an `Arc` only allows for
//! using functions that borrow the inner value. This interface is primarily designed to support
//! this kind of usage.

use crate::types::*;
use crate::protocol::types::RedisCommandKind;

use futures::{
  Future,
  Stream
};
use crate::error::RedisError;

use crate::utils;
use crate::protocol::utils as protocol_utils;
use crate::client::RedisClient;

use crate::commands;

use tokio_core::reactor::Handle;
use crate::protocol::utils::command_args;

pub trait RedisClientBorrowed {

  fn quit(&self) -> Box<Future<Item=(), Error=RedisError>>;

  fn flushall(&self, _async: bool) -> Box<Future<Item=String, Error=RedisError>>;

  fn get<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>>;

  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>>;

  fn select(&self, db: u8) -> Box<Future<Item=(), Error=RedisError>>;

  fn info(&self, section: Option<InfoKind>) -> Box<Future<Item=String, Error=RedisError>>;

  fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Box<Future<Item=usize, Error=RedisError>>;

  fn subscribe<T: Into<String>>(&self, channel: T) -> Box<Future<Item=usize, Error=RedisError>>;

  fn unsubscribe<T: Into<String>>(&self, channel: T) -> Box<Future<Item=usize, Error=RedisError>>;

  fn publish<T: Into<String>, V: Into<RedisValue>>(&self, channel: T, message: V) -> Box<Future<Item=i64, Error=RedisError>>;

  fn decr<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=i64, Error=RedisError>>;

  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<Future<Item=i64, Error=RedisError>>;

  fn incr<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=i64, Error=RedisError>>;

  fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Box<Future<Item=i64, Error=RedisError>>;

  fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Box<Future<Item=f64, Error=RedisError>>;

}


impl RedisClientBorrowed for RedisClient {

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
  fn quit(&self) -> Box<Future<Item=(), Error=RedisError>> {
    commands::quit(&self.inner)
  }

  /// Delete the keys in all databases.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushall>
  fn flushall(&self, _async: bool) -> Box<Future<Item=String, Error=RedisError>> {
    commands::flushall(&self.inner, _async)
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// <https://redis.io/commands/set>
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>> {
    commands::set(&self.inner, key, value, expire, options)
  }

  /// Read a value from Redis at `key`.
  ///
  /// <https://redis.io/commands/get>
  fn get<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    commands::get(&self.inner, key)
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  fn select(&self, db: u8) -> Box<Future<Item=(), Error=RedisError>> {
    commands::select(&self.inner, db)
  }

  /// Read info about the Redis server.
  ///
  /// <https://redis.io/commands/info>
  fn info(&self, section: Option<InfoKind>) -> Box<Future<Item=String, Error=RedisError>> {
    commands::info(&self.inner, section)
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::del(&self.inner, keys)
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  fn subscribe<T: Into<String>>(&self, channel: T) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::subscribe(&self.inner, channel)
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// <https://redis.io/commands/unsubscribe>
  fn unsubscribe<T: Into<String>>(&self, channel: T) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::unsubscribe(&self.inner, channel)
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  fn publish<T: Into<String>, V: Into<RedisValue>>(&self, channel: T, message: V) -> Box<Future<Item=i64, Error=RedisError>> {
    commands::publish(&self.inner, channel, message)
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  fn decr<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
    commands::decr(&self.inner, key)
  }

  /// Decrements the number stored at key by value argument. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<Future<Item=i64, Error=RedisError>> {
    commands::decrby(&self.inner, key, value)
  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  fn incr<K: Into<RedisKey>> (&self, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
    commands::incr(&self.inner, key)
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Box<Future<Item=i64, Error=RedisError>> {
    commands::incrby(&self.inner, key, incr)
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Box<Future<Item=f64, Error=RedisError>> {
    commands::incrbyfloat(&self.inner, key, incr)
  }


}



