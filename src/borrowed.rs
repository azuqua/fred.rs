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

pub trait RedisClientBorrowed {

  fn quit(&self) -> Box<dyn Future<Output=Result<(), RedisError>>>;

  fn flushall(&self, _async: bool) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn get<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>>;

  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<dyn Future<Output=Result<bool, RedisError>>>;

  fn select(&self, db: u8) -> Box<dyn Future<Output=Result<(), RedisError>>>;

  fn info(&self, section: Option<InfoKind>) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn subscribe<T: Into<String>>(&self, channel: T) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn unsubscribe<T: Into<String>>(&self, channel: T) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn publish<T: Into<String>, V: Into<RedisValue>>(&self, channel: T, message: V) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn decr<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn incr<K: Into<RedisKey>> (&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Box<dyn Future<Output=Result<f64, RedisError>>>;

  fn ping(&self) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn auth<V: Into<String>>(&self, value: V) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn bgrewriteaof(&self) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn bgsave(&self) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn client_list(&self) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn client_getname(&self) -> Box<dyn Future<Output=Result<Option<String>, RedisError>>>;

  fn client_setname<V: Into<String>>(&self, name: V) -> Box<dyn Future<Output=Result<Option<String>, RedisError>>>;

  fn dbsize(&self) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn dump<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Option<String>, RedisError>>>;

  fn exists<K: Into<MultipleKeys>>(&self, keys: K) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Box<dyn Future<Output=Result<bool, RedisError>>>;

  fn expire_at<K: Into<RedisKey>>(&self, key: K, timestamp: i64) -> Box<dyn Future<Output=Result<bool, RedisError>>>;

  fn persist<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<bool, RedisError>>>;

  fn flushdb(&self, _async: bool) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn getrange<K: Into<RedisKey>>(&self, key: K, start: usize, end: usize) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>>;

  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<dyn Future<Output=Result<bool, RedisError>>>;

  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>>;

  fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<HashMap<String, RedisValue>, RedisError>>>;

  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F, incr: i64) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F, incr: f64) -> Box<dyn Future<Output=Result<f64, RedisError>>>;

  fn hkeys<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Vec<String>, RedisError>>>;

  fn hlen<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>>(&self, key: K, values: HashMap<F, V>) -> Box<dyn Future<Output=Result<String, RedisError>>>;

  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn hvals<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn llen<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn lpop<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>>;

  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn smembers<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn psubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Box<dyn Future<Output=Result<Vec<usize>, RedisError>>>;

  fn punsubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Box<dyn Future<Output=Result<Vec<usize>, RedisError>>>;

  fn mget<K: Into<MultipleKeys>>(&self, keys: K) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zadd<K: Into<RedisKey>, V: Into<MultipleZaddValues>>(&self, key: K, options: Option<SetOptions>, changed: bool, incr: bool, values: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>>;

  fn zcard<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zcount<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zlexcount<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zincrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, incr: f64, value: V) -> Box<dyn Future<Output=Result<f64, RedisError>>>;

  fn zrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zpopmax<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zpopmin<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>>;

  fn zrem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zremrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zremrangebyrank<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zremrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zrevrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zrevrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, max: M, min: N, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zrevrangebyscore<K: Into<RedisKey>>(&self, key: K, max: f64, min: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>>;

  fn zrevrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>>;

  fn zscore<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>>;

  fn zinterstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn zunionstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Box<dyn Future<Output=Result<usize, RedisError>>>;

  fn ttl<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

  fn pttl<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>>;

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
  fn quit(&self) -> Box<dyn Future<Output=Result<(), RedisError>>> {
    commands::quit(&self.inner)
  }

  /// Delete the keys in all databases.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushall>
  fn flushall(&self, _async: bool) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::flushall(&self.inner, _async)
  }

  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// <https://redis.io/commands/set>
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<dyn Future<Output=Result<bool, RedisError>>> {
    commands::set(&self.inner, key, value, expire, options)
  }

  /// Read a value from Redis at `key`.
  ///
  /// <https://redis.io/commands/get>
  fn get<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>> {
    commands::get(&self.inner, key)
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  fn select(&self, db: u8) -> Box<dyn Future<Output=Result<(), RedisError>>> {
    commands::select(&self.inner, db)
  }

  /// Read info about the Redis server.
  ///
  /// <https://redis.io/commands/info>
  fn info(&self, section: Option<InfoKind>) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::info(&self.inner, section)
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn del<K: Into<MultipleKeys>>(&self, keys: K) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::del(&self.inner, keys)
  }

  /// Subscribe to a channel on the PubSub interface. Any messages received before `on_message` is called will be discarded, so it's
  /// usually best to call `on_message` before calling `subscribe` for the first time. The `usize` returned here is the number of
  /// channels to which the client is currently subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  fn subscribe<T: Into<String>>(&self, channel: T) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::subscribe(&self.inner, channel)
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// <https://redis.io/commands/unsubscribe>
  fn unsubscribe<T: Into<String>>(&self, channel: T) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::unsubscribe(&self.inner, channel)
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  fn publish<T: Into<String>, V: Into<RedisValue>>(&self, channel: T, message: V) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::publish(&self.inner, channel, message)
  }

  /// Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  fn decr<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::decr(&self.inner, key)
  }

  /// Decrements the number stored at key by value argument. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  fn decrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::decrby(&self.inner, key, value)
  }

  /// Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  fn incr<K: Into<RedisKey>> (&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::incr(&self.inner, key)
  }

  /// Increments the number stored at key by incr. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  fn incrby<K: Into<RedisKey>>(&self, key: K, incr: i64) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::incrby(&self.inner, key, incr)
  }

  /// Increment the string representing a floating point number stored at key by the argument value. If the key does not exist, it is set to 0 before performing the operation.
  /// Returns error if key value is wrong type or if the current value or increment value are not parseable as float value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  fn incrbyfloat<K: Into<RedisKey>>(&self, key: K, incr: f64) -> Box<dyn Future<Output=Result<f64, RedisError>>> {
    commands::incrbyfloat(&self.inner, key, incr)
  }

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping(&self) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::ping(&self.inner)
  }

  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<V: Into<String>>(&self, value: V) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::auth(&self.inner, value)
  }

  /// Instruct Redis to start an Append Only File rewrite process. Returns ok.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  fn bgrewriteaof(&self) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::bgrewriteaof(&self.inner)
  }

  /// Save the DB in background. Returns ok.
  ///
  /// <https://redis.io/commands/bgsave>
  fn bgsave(&self) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::bgsave(&self.inner)
  }

  /// Returns information and statistics about the client connections.
  ///
  /// <https://redis.io/commands/client-list>
  fn client_list(&self) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::client_list(&self.inner)
  }

  /// Returns the name of the current connection as a string, or None if no name is set.
  ///
  /// <https://redis.io/commands/client-getname>
  fn client_getname(&self) -> Box<dyn Future<Output=Result<Option<String>, RedisError>>> {
    commands::client_getname(&self.inner)
  }

  /// Assigns a name to the current connection. Returns ok if successful, None otherwise.
  ///
  /// <https://redis.io/commands/client-setname>
  fn client_setname<V: Into<String>>(&self, name: V) -> Box<dyn Future<Output=Result<Option<String>, RedisError>>> {
    commands::client_setname(&self.inner, name)
  }

  /// Return the number of keys in the currently-selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  fn dbsize(&self) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::dbsize(&self.inner)
  }

  /// Serialize the value stored at key in a Redis-specific format and return it as bulk string.
  /// If key does not exist None is returned
  ///
  /// <https://redis.io/commands/dump>
  fn dump<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Option<String>, RedisError>>> {
    commands::dump(&self.inner, key)
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  fn exists<K: Into<MultipleKeys>>(&self, keys: K) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::exists(&self.inner, keys)
  }

  /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expire>
  fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Box<dyn Future<Output=Result<bool, RedisError>>> {
    commands::expire(&self.inner, key, seconds)
  }

  /// Set a timeout on key based on a UNIX timestamp. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expireat>
  fn expire_at<K: Into<RedisKey>>(&self, key: K, timestamp: i64) -> Box<dyn Future<Output=Result<bool, RedisError>>> {
    commands::expire_at(&self.inner, key, timestamp)
  }

  /// Remove the existing timeout on key, turning the key from volatile (a key with an expire set) 
  /// to persistent (a key that will never expire as no timeout is associated).
  /// Return `true` if timeout was removed, `false` if key does not exist
  /// 
  /// <https://redis.io/commands/persist>
  fn persist<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<bool, RedisError>>>{
    commands::persist(&self.inner, key)
  }

  /// Delete all the keys in the currently selected database.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushdb>
  fn flushdb(&self, _async: bool) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::flushdb(&self.inner, _async)
  }

  /// Returns the substring of the string value stored at key, determined by the offsets start and end (both inclusive).
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  fn getrange<K: Into<RedisKey>>(&self, key: K, start: usize, end: usize) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::getrange(&self.inner, key, start, end)
  }

  /// Atomically sets key to value and returns the old value stored at key.
  /// Returns error if key does not hold string value. Returns None if key does not exist.
  ///
  /// <https://redis.io/commands/getset>
  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>> {
    commands::getset(&self.inner, key, value)
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// <https://redis.io/commands/hdel>
  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::hdel(&self.inner, key, fields)
  }

  /// Returns `true` if `field` exists on `key`.
  ///
  /// <https://redis.io/commands/hexists>
  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<dyn Future<Output=Result<bool, RedisError>>> {
    commands::hexists(&self.inner, key, field)
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// <https://redis.io/commands/hget>
  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>> {
    commands::hget(&self.inner, key, field)
  }

  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// <https://redis.io/commands/hgetall>
  fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<HashMap<String, RedisValue>, RedisError>>> {
    commands::hgetall(&self.inner, key)
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `incr`. If key does not exist, a new key holding a hash is created.
  /// If field does not exist the value is set to 0 before the operation is performed.
  ///
  /// <https://redis.io/commands/hincrby>
  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F, incr: i64) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::hincrby(&self.inner, key, field, incr)
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified increment.
  /// If the field does not exist, it is set to 0 before performing the operation.
  /// Returns an error if field value contains wrong type or content/increment are not parsable.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F, incr: f64) -> Box<dyn Future<Output=Result<f64, RedisError>>> {
    commands::hincrbyfloat(&self.inner, key, field, incr)
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// <https://redis.io/commands/hkeys>
  fn hkeys<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Vec<String>, RedisError>>> {
    commands::hkeys(&self.inner, key)
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// <https://redis.io/commands/hlen>
  fn hlen<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::hlen(&self.inner, key)
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// <https://redis.io/commands/hmget>
  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::hmget(&self.inner, key, fields)
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// <https://redis.io/commands/hmset>
  fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>>(&self, key: K, mut values: HashMap<F, V>) -> Box<dyn Future<Output=Result<String, RedisError>>> {
    commands::hmset(&self.inner, key, values)
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// <https://redis.io/commands/hset>
  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::hset(&self.inner, key, field, value)
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// <https://redis.io/commands/hsetnx>
  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::hsetnx(&self.inner, key, field, value)
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// <https://redis.io/commands/hstrlen>
  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::hstrlen(&self.inner, key, field)
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// <https://redis.io/commands/hvals>
  fn hvals<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::hvals(&self.inner, key)
  }

  /// Returns the length of the list stored at key. If key does not exist, it is interpreted as an
  /// empty list and 0 is returned. An error is returned when the value stored at key is not a
  /// list.
  ///
  /// <https://redis.io/commands/llen>
  fn llen<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::llen(&self.inner, key)
  }

  /// Insert the specified value at the head of the list stored at key. If key does not exist,
  /// it is created as empty list before performing the push operations. When key holds a value
  /// that is not a list, an error is returned.
  ///
  /// <https://redis.io/commands/lpush>
  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::lpush(&self.inner, key, value)
  }

  /// Removes and returns the first element of the list stored at key.
  ///
  /// <https://redis.io/commands/lpop>
  fn lpop<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>>> {
    commands::lpop(&self.inner, key)
  }

  /// Add the specified value to the set stored at key. Values that are already a member of this set are ignored.
  /// If key does not exist, a new set is created before adding the specified value.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/sadd>
  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::sadd(&self.inner, key, values)
  }

  /// Remove the specified value from the set stored at key. Values that are not a member of this set are ignored.
  /// If key does not exist, it is treated as an empty set and this command returns 0.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/srem>
  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::srem(&self.inner, key, values)
  }

  /// Returns all the members of the set value stored at key.
  /// This has the same effect as running SINTER with one argument key.
  ///
  /// <https://redis.io/commands/smembers>
  fn smembers<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::smembers(&self.inner, key)
  }

  /// Subscribes the client to the given patterns.
  ///
  /// Returns the subscription count for each of the provided patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  fn psubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Box<dyn Future<Output=Result<Vec<usize>, RedisError>>> {
    commands::psubscribe(&self.inner, patterns)
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// Returns the subscription count for each of the provided patterns.
  ///
  /// <https://redis.io/commands/punsubscribe>
  fn punsubscribe<K: Into<MultipleKeys>>(&self, patterns: K) -> Box<dyn Future<Output=Result<Vec<usize>, RedisError>>> {
    commands::punsubscribe(&self.inner, patterns)
  }

  /// Returns the values of all specified keys.
  ///
  /// <https://redis.io/commands/mget>
  fn mget<K: Into<MultipleKeys>>(&self, keys: K) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::mget(&self.inner, keys)
  }

  /// Adds all the specified members with the specified scores to the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zadd>
  fn zadd<K: Into<RedisKey>, V: Into<MultipleZaddValues>>(&self, key: K, options: Option<SetOptions>, changed: bool, incr: bool, values: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>> {
    commands::zadd(&self.inner, key, options, changed, incr, values)
  }

  /// Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zcard>
  fn zcard<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zcard(&self.inner, key)
  }

  /// Returns the number of elements in the sorted set at key with a score between min and max.
  ///
  /// <https://redis.io/commands/zcount>
  fn zcount<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zcount(&self.inner, key, min, max)
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns the number of elements in the sorted set at key with a value between min and max.
  ///
  /// <https://redis.io/commands/zlexcount>
  fn zlexcount<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zlexcount(&self.inner, key, min, max)
  }

  /// Increments the score of member in the sorted set stored at key by increment.
  ///
  /// <https://redis.io/commands/zincrby>
  fn zincrby<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, incr: f64, value: V) -> Box<dyn Future<Output=Result<f64, RedisError>>> {
    commands::zincrby(&self.inner, key, incr, value)
  }

  /// Returns the specified range of elements in the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zrange>
  fn zrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zrange(&self.inner, key, start, stop, with_scores)
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between min and max.
  ///
  /// <https://redis.io/commands/zrangebylex>
  fn zrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zrangebylex(&self.inner, key, min, max, limit)
  }

  /// Returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
  ///
  /// <https://redis.io/commands/zrangebyscore>
  fn zrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zrangebyscore(&self.inner, key, min, max, with_scores, limit)
  }

  /// Removes and returns up to count members with the highest scores in the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zpopmax>
  fn zpopmax<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zpopmax(&self.inner, key, count)
  }

  /// Removes and returns up to count members with the lowest scores in the sorted set stored at key.
  ///
  /// <https://redis.io/commands/zpopmin>
  fn zpopmin<K: Into<RedisKey>>(&self, key: K, count: Option<usize>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zpopmin(&self.inner, key, count)
  }

  /// Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
  ///
  /// <https://redis.io/commands/zrank>
  fn zrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>> {
    commands::zrank(&self.inner, key, value)
  }

  /// Removes the specified members from the sorted set stored at key. Non existing members are ignored.
  ///
  /// <https://redis.io/commands/zrem>
  fn zrem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zrem(&self.inner, key, values)
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command removes all elements in the sorted set stored at key between the lexicographical range specified by min and max.
  ///
  /// <https://redis.io/commands/zremrangebylex>
  fn zremrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, min: M, max: N) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zremrangebylex(&self.inner, key, min, max)
  }

  /// Removes all elements in the sorted set stored at key with rank between start and stop.
  ///
  /// <https://redis.io/commands/zremrangebyrank>
  fn zremrangebyrank<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zremrangebyrank(&self.inner, key, start, stop)
  }

  /// Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
  ///
  /// <https://redis.io/commands/zremrangebyscore>
  fn zremrangebyscore<K: Into<RedisKey>>(&self, key: K, min: f64, max: f64) -> Box<dyn Future<Output=Result<usize, RedisError>>> {
    commands::zremrangebyscore(&self.inner, key, min, max)
  }

  /// Returns the specified range of elements in the sorted set stored at key. The elements are considered to be ordered from the highest to the lowest score
  ///
  /// <https://redis.io/commands/zrevrange>
  fn zrevrange<K: Into<RedisKey>>(&self, key: K, start: i64, stop: i64, with_scores: bool) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zrevrange(&self.inner, key, start, stop, with_scores)
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the sorted set at key with a value between max and min.
  ///
  /// <https://redis.io/commands/zrevrangebylex>
  fn zrevrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(&self, key: K, max: M, min: N, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zrevrangebylex(&self.inner, key, max, min, limit)
  }

  /// Returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min). In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
  ///
  /// <https://redis.io/commands/zrevrangebyscore>
  fn zrevrangebyscore<K: Into<RedisKey>>(&self, key: K, max: f64, min: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Box<dyn Future<Output=Result<Vec<RedisValue>, RedisError>>> {
    commands::zrevrangebyscore(&self.inner, key, max, min, with_scores, limit)
  }

  /// Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
  ///
  /// <https://redis.io/commands/zrevrank>
  fn zrevrank<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>> {
    commands::zrevrank(&self.inner, key, value)
  }

  /// Returns the score of member in the sorted set at key.
  ///
  /// <https://redis.io/commands/zscore>
  fn zscore<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<dyn Future<Output=Result<RedisValue, RedisError>>> {
    commands::zscore(&self.inner, key, value)
  }

  /// Computes the intersection of numkeys sorted sets given by the specified keys, and stores the result in destination.
  ///
  /// <https://redis.io/commands/zinterstore>
  fn zinterstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Box<dyn Future<Output=Result<usize, RedisError>>>
  {
    commands::zinterstore(&self.inner, destination, keys, weights, aggregate)
  }

  /// Computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination.
  ///
  /// <https://redis.io/commands/zunionstore>
  fn zunionstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(&self,
                                                                                     destination: D,
                                                                                     keys: K,
                                                                                     weights: W,
                                                                                     aggregate: Option<AggregateOptions>)
    -> Box<dyn Future<Output=Result<usize, RedisError>>>
  {
    commands::zunionstore(&self.inner, destination, keys, weights, aggregate)
  }

  /// Returns the remaining time to live of a key that has a timeout. This introspection capability allows a Redis client to check how many seconds a given key will continue to be part of the dataset.
  ///
  /// <https://redis.io/commands/ttl>
  fn ttl<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::ttl(&self.inner, key)
  }

  /// Like TTL this command returns the remaining time to live of a key that has an expire set, with the sole difference that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds.
  ///
  /// <https://redis.io/commands/pttl>
  fn pttl<K: Into<RedisKey>>(&self, key: K) -> Box<dyn Future<Output=Result<i64, RedisError>>> {
    commands::pttl(&self.inner, key)
  }

}



