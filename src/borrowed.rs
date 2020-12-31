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

//use tokio_core::reactor::Handle;
use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;

pub trait RedisClientBorrowed {

  fn quit(&self) -> Pin<Box<dyn Future<Output=Result<(), RedisError>> + Send + '_>>;

  fn flushall(&self, _async: bool) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn get<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>>;

  fn set<'a, 'b, K: 'a + Into<RedisKey> + Send, V: 'b + Into<RedisValue> + Send>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>>;

  fn select(&self, db: u8) -> Pin<Box<dyn Future<Output=Result<(), RedisError>> + Send + '_>>;

  fn info(&self, section: Option<InfoKind>) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn subscribe<T: Into<String>>(&self, channel: T) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn unsubscribe<T: Into<String>>(&self, channel: T) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn publish<T: Into<String>, V: Into<RedisValue>>(&self, channel: T, message: V) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn decr<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn incr<K: Into<RedisKey>> (&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Pin<Box<dyn Future<Output=Result<f64, RedisError>> + Send + '_>>;

  fn ping(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn auth<V: Into<String>>(&self, value: V) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn bgrewriteaof(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn bgsave(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn client_list(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn client_getname(&self) -> Pin<Box<dyn Future<Output=Result<Option<String>, RedisError>> + Send + '_>>;

  fn client_setname<V: Into<String>>(&self, name: V) -> Pin<Box<dyn Future<Output=Result<Option<String>, RedisError>> + Send + '_>>;

  fn dbsize(&self) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn dump<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Option<String>, RedisError>> + Send + '_>>;

  fn exists<K: Into<MultipleKeys>>(&self, keys: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>>;

  fn expire_at<K: Into<RedisKey>>(&self, key: K, timestamp: i64) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>>;

  fn persist<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>>;

  fn flushdb(&self, _async: bool) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn getrange<K: Into<RedisKey>>(&self, key: K, start: usize, end: usize) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>>;

  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>>;

  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>>;

  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>>;

  fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<HashMap<String, RedisValue>, RedisError>> + Send + '_>>;

  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F, incr: i64) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F, incr: f64) -> Pin<Box<dyn Future<Output=Result<f64, RedisError>> + Send + '_>>;

  fn hkeys<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Vec<String>, RedisError>> + Send + '_>>;

  fn hlen<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn hmset<'a, V: Into<RedisValue> + Send + 'a, F: Into<RedisKey> + Hash + Eq + Send + 'a, K: Into<RedisKey> + Send + 'a>(&'a self, key: K, values: HashMap<F, V>) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + 'a>>;

  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn hvals<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn llen<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn lpop<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>>;

  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn smembers<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn psubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Pin<Box<dyn Future<Output=Result<Vec<usize>, RedisError>> + Send + '_>>;

  fn punsubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Pin<Box<dyn Future<Output=Result<Vec<usize>, RedisError>> + Send + '_>>;

  fn mget<K: Into<MultipleKeys>>(&self, keys: K) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zadd<K: Into<RedisKey>, V: Into<MultipleZaddValues>>(&self, key: K, options: Option<SetOptions>, changed: bool, incr: bool, values: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>>;

  fn zcard<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zcount<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zlexcount<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zincrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, incr: f64, value: V) -> Pin<Box<dyn Future<Output=Result<f64, RedisError>> + Send + '_>>;

  fn zrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zpopmax<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zpopmin<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>>;

  fn zrem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zremrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zremrangebyrank<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zremrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zrevrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zrevrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, max: M, min: N, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zrevrangebyscore<K: Into<RedisKey>>(&self, key: K, max: f64, min: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>>;

  fn zrevrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>>;

  fn zscore<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>>;

  fn zinterstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn zunionstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>;

  fn ttl<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

  fn pttl<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>>;

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
  fn quit(&self) -> Pin<Box<dyn Future<Output=Result<(), RedisError>> + Send + '_>> {
    Box::pin(commands::quit(&self.inner))
  }

  /// Delete the keys in all databases.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushall>
  fn flushall(&self, _async: bool) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::flushall(&self.inner, _async))
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// <https://redis.io/commands/set>
  fn set<'a, 'b, K: 'a + Into<RedisKey> + Send, V: 'b + Into<RedisValue> + Send>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>> {
    Box::pin(commands::set(&self.inner, key.into(), value.into(), expire, options))
  }

  /// Read a value from Redis at `key`.
  ///
  /// <https://redis.io/commands/get>
  fn get<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::get(&self.inner, key.into()))
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  fn select(&self, db: u8) -> Pin<Box<dyn Future<Output=Result<(), RedisError>> + Send + '_>> {
    Box::pin(commands::select(&self.inner, db))
  }

  /// Read info about the Redis server.
  ///
  /// <https://redis.io/commands/info>
  fn info(&self, section: Option<InfoKind>) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::info(&self.inner, section))
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::del(&self.inner, keys.into()))
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  fn subscribe<T: Into<String>>(&self, channel: T) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::subscribe(&self.inner, channel.into()))
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// <https://redis.io/commands/unsubscribe>
  fn unsubscribe<T: Into<String>>(&self, channel: T) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::unsubscribe(&self.inner, channel.into()))
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  fn publish<T: Into<String>, V: Into<RedisValue>>(&self, channel: T, message: V) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::publish(&self.inner, channel.into(), message.into()))
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  fn decr<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::decr(&self.inner, key.into()))
  }

  /// Decrements the number stored at key by value argument. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::decrby(&self.inner, key.into(), value.into()))
  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  fn incr<K: Into<RedisKey>> (&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::incr(&self.inner, key.into()))
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::incrby(&self.inner, key.into(), incr))
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Pin<Box<dyn Future<Output=Result<f64, RedisError>> + Send + '_>> {
    Box::pin(commands::incrbyfloat(&self.inner, key.into(), incr))
  }

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::ping(&self.inner))
  }

  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<V: Into<String>>(&self, value: V) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::auth(&self.inner, value.into()))
  }

  /// Instruct Redis to start an Append Only File rewrite process. Returns ok.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  fn bgrewriteaof(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::bgrewriteaof(&self.inner))
  }

  /// Save the DB in background. Returns ok.
  ///
  /// <https://redis.io/commands/bgsave>
  fn bgsave(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::bgsave(&self.inner))
  }

  /// Returns information and statistics about the client connections.
  ///
  /// <https://redis.io/commands/client-list>
  fn client_list(&self) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::client_list(&self.inner))
  }

  /// Returns the name of the current connection as a string, or None if no name is set.
  ///
  /// <https://redis.io/commands/client-getname>
  fn client_getname(&self) -> Pin<Box<dyn Future<Output=Result<Option<String>, RedisError>> + Send + '_>> {
    Box::pin(commands::client_getname(&self.inner))
  }

  /// Assigns a name to the current connection. Returns ok if successful, None otherwise.
  ///
  /// <https://redis.io/commands/client-setname>
  fn client_setname<V: Into<String>>(&self, name: V) -> Pin<Box<dyn Future<Output=Result<Option<String>, RedisError>> + Send + '_>> {
    Box::pin(commands::client_setname(&self.inner, name.into()))
  }

  /// Return the number of keys in the currently-selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  fn dbsize(&self) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::dbsize(&self.inner))
  }

  /// Serialize the value stored at key in a Redis-specific format and return it as bulk string.
  /// If key does not exist None is returned
  ///
  /// <https://redis.io/commands/dump>
  fn dump<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Option<String>, RedisError>> + Send + '_>> {
    Box::pin(commands::dump(&self.inner, key.into()))
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  fn exists<K: Into<MultipleKeys>>(&self, keys: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::exists(&self.inner, keys.into()))
  }

  /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expire>
  fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>> {
    Box::pin(commands::expire(&self.inner, key.into(), seconds))
  }

  /// Set a timeout on key based on a UNIX timestamp. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expireat>
  fn expire_at<K: Into<RedisKey>>(&self, key: K, timestamp: i64) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>> {
    Box::pin(commands::expire_at(&self.inner, key.into(), timestamp))
  }

  /// Remove the existing timeout on key, turning the key from volatile (a key with an expire set) 
  /// to persistent (a key that will never expire as no timeout is associated).
  /// Return `true` if timeout was removed, `false` if key does not exist
  /// 
  /// <https://redis.io/commands/persist>
  fn persist<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>>{
    Box::pin(commands::persist(&self.inner, key.into()))
  }

  /// Delete all the keys in the currently selected database.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushdb>
  fn flushdb(&self, _async: bool) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::flushdb(&self.inner, _async))
  }

  /// Returns the substring of the string value stored at key, determined by the offsets start and end (both inclusive).
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  fn getrange<K: Into<RedisKey>>(&self, key: K, start: usize, end: usize) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + '_>> {
    Box::pin(commands::getrange(&self.inner, key.into(), start, end))
  }

  /// Atomically sets key to value and returns the old value stored at key.
  /// Returns error if key does not hold string value. Returns None if key does not exist.
  ///
  /// <https://redis.io/commands/getset>
  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::getset(&self.inner, key.into(), value.into()))
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// <https://redis.io/commands/hdel>
  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::hdel(&self.inner, key.into(), fields.into()))
  }

  /// Returns `true` if `field` exists on `key`.
  ///
  /// <https://redis.io/commands/hexists>
  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Pin<Box<dyn Future<Output=Result<bool, RedisError>> + Send + '_>> {
    Box::pin(commands::hexists(&self.inner, key.into(), field.into()))
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// <https://redis.io/commands/hget>
  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::hget(&self.inner, key.into(), field.into()))
  }

  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// <https://redis.io/commands/hgetall>
  fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<HashMap<String, RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::hgetall(&self.inner, key.into()))
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `incr`. If key does not exist, a new key holding a hash is created.
  /// If field does not exist the value is set to 0 before the operation is performed.
  ///
  /// <https://redis.io/commands/hincrby>
  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F, incr: i64) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::hincrby(&self.inner, key.into(), field.into(), incr))
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified increment.
  /// If the field does not exist, it is set to 0 before performing the operation.
  /// Returns an error if field value contains wrong type or content/increment are not parsable.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F, incr: f64) -> Pin<Box<dyn Future<Output=Result<f64, RedisError>> + Send + '_>> {
    Box::pin(commands::hincrbyfloat(&self.inner, key.into(), field.into(), incr))
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// <https://redis.io/commands/hkeys>
  fn hkeys<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Vec<String>, RedisError>> + Send + '_>> {
    Box::pin(commands::hkeys(&self.inner, key.into()))
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// <https://redis.io/commands/hlen>
  fn hlen<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::hlen(&self.inner, key.into()))
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// <https://redis.io/commands/hmget>
  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::hmget(&self.inner, key.into(), fields.into()))
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// <https://redis.io/commands/hmset>
  fn hmset<'a, V: Into<RedisValue> + Send + 'a, F: Into<RedisKey> + Hash + Eq + Send + 'a, K: Into<RedisKey> + Send + 'a>(&'a self, key: K, mut values: HashMap<F, V>) -> Pin<Box<dyn Future<Output=Result<String, RedisError>> + Send + 'a>> {
    Box::pin(commands::hmset(&self.inner, key.into(), values.into()))
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// <https://redis.io/commands/hset>
  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::hset(&self.inner, key.into(), field.into(), value.into()))
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// <https://redis.io/commands/hsetnx>
  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::hsetnx(&self.inner, key.into(), field.into(), value.into()))
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// <https://redis.io/commands/hstrlen>
  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::hstrlen(&self.inner, key.into(), field.into()))
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// <https://redis.io/commands/hvals>
  fn hvals<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::hvals(&self.inner, key.into()))
  }

  /// Returns the length of the list stored at key. If key does not exist, it is interpreted as an
  /// empty list and 0 is returned. An error is returned when the value stored at key is not a
  /// list.
  ///
  /// <https://redis.io/commands/llen>
  fn llen<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::llen(&self.inner, key.into()))
  }

  /// Insert the specified value at the head of the list stored at key. If key does not exist,
  /// it is created as empty list before performing the push operations. When key holds a value
  /// that is not a list, an error is returned.
  ///
  /// <https://redis.io/commands/lpush>
  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::lpush(&self.inner, key.into(), value.into()))
  }

  /// Removes and returns the first element of the list stored at key.
  ///
  /// <https://redis.io/commands/lpop>
  fn lpop<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::lpop(&self.inner, key.into()))
  }

  /// Add the specified value to the set stored at key. Values that are already a member of this set are ignored.
  /// If key does not exist, a new set is created before adding the specified value.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/sadd>
  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::sadd(&self.inner, key.into(), values.into()))
  }

  /// Remove the specified value from the set stored at key. Values that are not a member of this set are ignored.
  /// If key does not exist, it is treated as an empty set and this command returns 0.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/srem>
  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::srem(&self.inner, key.into(), values.into()))
  }

  /// Returns all the members of the set value stored at key.
  /// This has the same effect as running SINTER with one argument key.
  ///
  /// <https://redis.io/commands/smembers>
  fn smembers<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::smembers(&self.inner, key.into()))
  }

  /// Subscribes the client to the given patterns.
  ///
  /// Returns the subscription count for each of the provided patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  fn psubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Pin<Box<dyn Future<Output=Result<Vec<usize>, RedisError>> + Send + '_>> {
    Box::pin(commands::psubscribe(&self.inner, patterns.into()))
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// Returns the subscription count for each of the provided patterns.
  ///
  /// <https://redis.io/commands/punsubscribe>
  fn punsubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Pin<Box<dyn Future<Output=Result<Vec<usize>, RedisError>> + Send + '_>> {
    Box::pin(commands::punsubscribe(&self.inner, patterns.into()))
  }

  /// Returns the values of all specified keys.
  ///
  /// <https://redis.io/commands/mget>
  fn mget<K: Into<MultipleKeys>>(&self, keys: K) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::mget(&self.inner, keys.into()))
  }

  /// Adds all the specified members with the specified scores to the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zadd>
  fn zadd<K: Into<RedisKey>, V: Into<MultipleZaddValues>>(&self, key: K, options: Option<SetOptions>, changed: bool, incr: bool, values: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>> {
    Box::pin(commands::zadd(&self.inner, key.into(), options, changed, incr, values.into()))
  }

  /// Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zcard>
  fn zcard<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zcard(&self.inner, key.into()))
  }

  /// Returns the number of elements in the sorted set at key with a score between min and max.
  ///
  /// <https://redis.io/commands/zcount>
  fn zcount<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zcount(&self.inner, key.into(), min, max))
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns the number of elements in the sorted set at key with a value between min and max.
  ///
  /// <https://redis.io/commands/zlexcount>
  fn zlexcount<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zlexcount(&self.inner, key.into(), min.into(), max.into()))
  }

  /// Increments the score of member in the sorted set stored at key by increment.
  ///
  /// <https://redis.io/commands/zincrby>
  fn zincrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, incr: f64, value: V) -> Pin<Box<dyn Future<Output=Result<f64, RedisError>> + Send + '_>> {
    Box::pin(commands::zincrby(&self.inner, key.into(), incr, value.into()))
  }

  /// Returns the specified range of elements in the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zrange>
  fn zrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zrange(&self.inner, key.into(), start, stop, with_scores))
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.
  ///
  /// <https://redis.io/commands/zrangebylex>
  fn zrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zrangebylex(&self.inner, key.into(), min.into(), max.into(), limit))
  }

  /// Returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
  ///
  /// <https://redis.io/commands/zrangebyscore>
  fn zrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zrangebyscore(&self.inner, key.into(), min, max, with_scores, limit))
  }

  /// Removes and returns up to count members with the highest scores in the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zpopmax>
  fn zpopmax<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zpopmax(&self.inner, key.into(), count))
  }

  /// Removes and returns up to count members with the lowest scores in the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zpopmin>
  fn zpopmin<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zpopmin(&self.inner, key.into(), count))
  }

  /// Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
  ///
  /// <https://redis.io/commands/zrank>
  fn zrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>> {
    Box::pin(commands::zrank(&self.inner, key.into(), value.into()))
  }

  /// Removes the specified members from the sorted set stored at key. Non existing members are ignored.
  ///
  /// <https://redis.io/commands/zrem>
  fn zrem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zrem(&self.inner, key.into(), values.into()))
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command removes all elements in the sorted set stored at key between the lexicographical range specified by min and max.
  ///
  /// <https://redis.io/commands/zremrangebylex>
  fn zremrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zremrangebylex(&self.inner, key.into(), min.into(), max.into()))
  }

  /// Removes all elements in the sorted set stored at key with rank between start and stop.
  ///
  /// <https://redis.io/commands/zremrangebyrank>
  fn zremrangebyrank<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zremrangebyrank(&self.inner, key.into(), start, stop))
  }

  /// Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
  ///
  /// <https://redis.io/commands/zremrangebyscore>
  fn zremrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>> {
    Box::pin(commands::zremrangebyscore(&self.inner, key.into(), min, max))
  }

  /// Returns the specified range of elements in the sorted set stored at key. The elements are considered to be ordered from the highest to the lowest score
  ///
  /// <https://redis.io/commands/zrevrange>
  fn zrevrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zrevrange(&self.inner, key.into(), start, stop, with_scores))
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between max and min.
  ///
  /// <https://redis.io/commands/zrevrangebylex>
  fn zrevrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, max: M, min: N, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zrevrangebylex(&self.inner, key.into(), max.into(), min.into(), limit))
  }

  /// Returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min). In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
  ///
  /// <https://redis.io/commands/zrevrangebyscore>
  fn zrevrangebyscore<K: Into<RedisKey>>(&self, key: K, max: f64, min: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Pin<Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>> + Send + '_>> {
    Box::pin(commands::zrevrangebyscore(&self.inner, key.into(), max, min, with_scores, limit))
  }

  /// Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
  ///
  /// <https://redis.io/commands/zrevrank>
  fn zrevrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>> {
    Box::pin(commands::zrevrank(&self.inner, key.into(), value.into()))
  }

  /// Returns the score of member in the sorted set at key.
  ///
  /// <https://redis.io/commands/zscore>
  fn zscore<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output=Result<RedisValue, RedisError>> + Send + '_>> {
    Box::pin(commands::zscore(&self.inner, key.into(), value.into()))
  }

  /// Computes the intersection of numkeys sorted sets given by the specified keys, and stores the result in destination.
  ///
  /// <https://redis.io/commands/zinterstore>
  fn zinterstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>
  {
    Box::pin(commands::zinterstore(&self.inner, destination.into(), keys.into(), weights.into(), aggregate))
  }

  /// Computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination.
  ///
  /// <https://redis.io/commands/zunionstore>
  fn zunionstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Pin<Box<dyn Future<Output=Result<usize, RedisError>> + Send + '_>>
  {
    Box::pin(commands::zunionstore(&self.inner, destination.into(), keys.into(), weights.into(), aggregate))
  }

  /// Returns the remaining time to live of a key that has a timeout. This introspection capability allows a Redis client to check how many seconds a given key will continue to be part of the dataset.
  ///
  /// <https://redis.io/commands/ttl>
  fn ttl<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::ttl(&self.inner, key.into()))
  }

  /// Like TTL this command returns the remaining time to live of a key that has an expire set, with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds.
  ///
  /// <https://redis.io/commands/pttl>
  fn pttl<K: Into<RedisKey>>(&self, key: K) -> Pin<Box<dyn Future<Output=Result<i64, RedisError>> + Send + '_>> {
    Box::pin(commands::pttl(&self.inner, key.into()))
  }

}



