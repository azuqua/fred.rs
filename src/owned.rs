//! An interface for the `RedisClient` that takes ownership over `self` with each command.
//!
//! This interface is ideal for chaining commands together as the underlying implementation will
//! pass the client instance back to the caller after each command. However, the downside to this
//! interface is that it requires taking ownership over the client with each command, limiting the
//! use of wrapping structs or semantics that require limiting ownership over the client.

use crate::types::*;
use crate::protocol::types::RedisCommandKind;

use futures::{
  Future,
  Stream
};
use crate::error::RedisError;

use crate::utils;
use crate::protocol::utils as protocol_utils;
use crate::client::{RedisClient, RedisClientInner};
use std::sync::Arc;

use crate::commands;

fn run_borrowed_empty<T, F>(_self: RedisClient, func: F) -> Box<Future<Item=RedisClient, Error=RedisError>>
  where T: 'static,
        F: FnOnce(&Arc<RedisClientInner>) -> Box<Future<Item=T, Error=RedisError>>
{
  Box::new(func(&_self.inner).map(move |_| _self))
}

fn run_borrowed<T, F>(_self: RedisClient, func: F) -> Box<Future<Item=(RedisClient, T), Error=RedisError>>
  where T: 'static,
        F: FnOnce(&Arc<RedisClientInner>) -> Box<Future<Item=T, Error=RedisError>>
{
  Box::new(func(&_self.inner).map(move |result| (_self, result)))
}

pub trait RedisClientOwned: Sized {

  fn quit(self) -> Box<Future<Item=Self, Error=RedisError>>;

  fn flushall(self, _async: bool) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn get<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>>;

  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=(Self, bool), Error=RedisError>>;

  fn select(self, db: u8) -> Box<Future<Item=Self, Error=RedisError>>;

  fn info(self, section: Option<InfoKind>) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn del<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn subscribe<T: Into<String>>(self, channel: T) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn unsubscribe<T: Into<String>>(self, channel: T) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn publish<T: Into<String>, V: Into<RedisValue>>(self, channel: T, message: V) -> Box<Future<Item=(Self, i64), Error=RedisError>>;

  fn decr<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>>;

  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V) -> Box<Future<Item=(Self, i64), Error=RedisError>>;

  fn incr<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>>;

  fn incrby<K: Into<RedisKey>>(self, key: K, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>>;

  fn incrbyfloat<K: Into<RedisKey>>(self, key: K, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>>;

}


impl RedisClientOwned for RedisClient {

  /// Read a value from Redis at `key`.
  ///
  /// <https://redis.io/commands/get>
  fn get<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::get(inner, key))
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
  fn quit(self) -> Box<Future<Item=Self, Error=RedisError>> {
    run_borrowed_empty(self, |inner| commands::quit(inner))
  }

  /// Delete the keys in all databases.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushall>
  fn flushall(self, _async: bool) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::flushall(inner, _async))
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// <https://redis.io/commands/set>
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    run_borrowed(self, |inner| commands::set(inner, key, value, expire, options))
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  fn select(self, db: u8) -> Box<Future<Item=Self, Error=RedisError>> {
    run_borrowed_empty(self, |inner| commands::select(inner, db))
  }

  /// Read info about the Redis server.
  ///
  /// <https://redis.io/commands/info>
  fn info(self, section: Option<InfoKind>) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::info(inner, section))
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn del<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::del(inner, keys))
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  fn subscribe<T: Into<String>>(self, channel: T) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::subscribe(inner, channel))
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// <https://redis.io/commands/unsubscribe>
  fn unsubscribe<T: Into<String>>(self, channel: T) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::unsubscribe(inner, channel))
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  fn publish<T: Into<String>, V: Into<RedisValue>>(self, channel: T, message: V) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::publish(inner, channel, message))
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  fn decr<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::decr(inner, key))
  }

  /// Decrements the number stored at key by value argument. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::decrby(inner, key, value))
  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  fn incr<K: Into<RedisKey>> (self, key: K) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::incr(inner, key))
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  fn incrby<K: Into<RedisKey>>(self, key: K, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::incrby(inner, key, incr))
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  fn incrbyfloat<K: Into<RedisKey>>(self, key: K, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::incrbyfloat(inner, key, incr))
  }


}



