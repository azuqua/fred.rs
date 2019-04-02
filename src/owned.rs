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

use std::collections::HashMap;
use std::hash::Hash;

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

  fn ping(self) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn auth<V: Into<String>>(self, value: V) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn bgrewriteaof(self) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn bgsave(self) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn client_list(self) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn client_getname(self) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>>;

  fn client_setname<V: Into<String>>(self, name: V) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>>;

  fn dbsize(self) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn dump<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>>;

  fn exists<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn expire<K: Into<RedisKey>>(self, key: K, seconds: i64) -> Box<Future<Item=(Self, bool), Error=RedisError>>;

  fn expire_at<K: Into<RedisKey>>(self, key: K, timestamp: i64) -> Box<Future<Item=(Self, bool), Error=RedisError>>;

  fn flushdb(self, _async: bool) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn getrange<K: Into<RedisKey>>(self, key: K, start: usize, end: usize) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(self, key: K, value: V) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>>;

  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(self, key: K, fields: F) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, bool), Error=RedisError>>;

  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>>;

  fn hgetall<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, HashMap<String, RedisValue>), Error=RedisError>>;

  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>>;

  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(self, key: K, field: F, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>>;

  fn hkeys<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Vec<String>), Error=RedisError>>;

  fn hlen<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(self, key: K, fields: F) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>>;

  fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>>(self, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=(Self, String), Error=RedisError>>;

  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn hvals<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>>;

  fn llen<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn lpop<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>>;

  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(self, key: K, values: V) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(self, key: K, values: V) -> Box<Future<Item=(Self, usize), Error=RedisError>>;

  fn smembers<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>>;

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

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::ping(inner))
  }

  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<V: Into<String>>(self, value: V) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::auth(inner, value))
  }

  /// Instruct Redis to start an Append Only File rewrite process. Returns ok.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  fn bgrewriteaof(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::bgrewriteaof(inner))
  }

  /// Save the DB in background. Returns ok.
  ///
  /// <https://redis.io/commands/bgsave>
  fn bgsave(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::bgsave(inner))
  }

  /// Returns information and statistics about the client connections.
  ///
  /// <https://redis.io/commands/client-list>
  fn client_list(self) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::client_list(inner))
  }

  /// Returns the name of the current connection as a string, or None if no name is set.
  ///
  /// <https://redis.io/commands/client-getname>
  fn client_getname(self) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::client_getname(inner))
  }

  /// Assigns a name to the current connection. Returns ok if successful, None otherwise.
  ///
  /// <https://redis.io/commands/client-setname>
  fn client_setname<V: Into<String>>(self, name: V) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::client_setname(inner, name))
  }

  /// Return the number of keys in the currently-selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  fn dbsize(self) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::dbsize(inner))
  }

  /// Serialize the value stored at key in a Redis-specific format and return it as bulk string.
  /// If key does not exist None is returned
  ///
  /// <https://redis.io/commands/dump>
  fn dump<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<String>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::dump(inner, key))
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  fn exists<K: Into<MultipleKeys>>(self, keys: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::exists(inner, keys))
  }

  /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expire>
  fn expire<K: Into<RedisKey>>(self, key: K, seconds: i64) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    run_borrowed(self, |inner| commands::expire(inner, key, seconds))
  }

  /// Set a timeout on key based on a UNIX timestamp. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expireat>
  fn expire_at<K: Into<RedisKey>>(self, key: K, timestamp: i64) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    run_borrowed(self, |inner| commands::expire_at(inner, key, timestamp))
  }

  /// Delete all the keys in the currently selected database.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushdb>
  fn flushdb(self, _async: bool) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::flushdb(inner, _async))
  }

  /// Returns the substring of the string value stored at key, determined by the offsets start and end (both inclusive).
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  fn getrange<K: Into<RedisKey>>(self, key: K, start: usize, end: usize) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::getrange(inner, key, start, end))
  }

  /// Atomically sets key to value and returns the old value stored at key.
  /// Returns error if key does not hold string value. Returns None if key does not exist.
  ///
  /// <https://redis.io/commands/getset>
  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(self, key: K, value: V) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::getset(inner, key, value))
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// <https://redis.io/commands/hdel>
  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(self, key: K, fields: F) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hdel(inner, key, fields))
  }

  /// Returns `true` if `field` exists on `key`.
  ///
  /// <https://redis.io/commands/hexists>
  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hexists(inner, key, field))
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// <https://redis.io/commands/hget>
  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hget(inner, key, field))
  }

  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// <https://redis.io/commands/hgetall>
  fn hgetall<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, HashMap<String, RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hgetall(inner, key))
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `incr`. If key does not exist, a new key holding a hash is created.
  /// If field does not exist the value is set to 0 before the operation is performed.
  ///
  /// <https://redis.io/commands/hincrby>
  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F, incr: i64) -> Box<Future<Item=(Self, i64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hincrby(inner, key, field, incr))
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified increment.
  /// If the field does not exist, it is set to 0 before performing the operation.
  /// Returns an error if field value contains wrong type or content/increment are not parsable.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(self, key: K, field: F, incr: f64) -> Box<Future<Item=(Self, f64), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hincrbyfloat(inner, key, field, incr))
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// <https://redis.io/commands/hkeys>
  fn hkeys<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Vec<String>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hkeys(inner, key))
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// <https://redis.io/commands/hlen>
  fn hlen<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hlen(inner, key))
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// <https://redis.io/commands/hmget>
  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(self, key: K, fields: F) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hmget(inner, key, fields))
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// <https://redis.io/commands/hmset>
  fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>>(self, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=(Self, String), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hmset(inner, key, values))
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// <https://redis.io/commands/hset>
  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hset(inner, key, field, value))
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// <https://redis.io/commands/hsetnx>
  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, field: F, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hsetnx(inner, key, field, value))
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// <https://redis.io/commands/hstrlen>
  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hstrlen(inner, key, field))
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// <https://redis.io/commands/hvals>
  fn hvals<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::hvals(inner, key))
  }

  /// Returns the length of the list stored at key. If key does not exist, it is interpreted as an
  /// empty list and 0 is returned. An error is returned when the value stored at key is not a
  /// list.
  ///
  /// <https://redis.io/commands/llen>
  fn llen<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::llen(inner, key))
  }

  /// Insert the specified value at the head of the list stored at key. If key does not exist,
  /// it is created as empty list before performing the push operations. When key holds a value
  /// that is not a list, an error is returned.
  ///
  /// <https://redis.io/commands/lpush>
  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::lpush(inner, key, value))
  }

  /// Removes and returns the first element of the list stored at key.
  ///
  /// <https://redis.io/commands/lpop>
  fn lpop<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::lpop(inner, key))
  }

  /// Add the specified value to the set stored at key. Values that are already a member of this set are ignored.
  /// If key does not exist, a new set is created before adding the specified value.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/sadd>
  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(self, key: K, values: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::sadd(inner, key, values))
  }

  /// Remove the specified value from the set stored at key. Values that are not a member of this set are ignored.
  /// If key does not exist, it is treated as an empty set and this command returns 0.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/srem>
  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(self, key: K, values: V) -> Box<Future<Item=(Self, usize), Error=RedisError>> {
    run_borrowed(self, |inner| commands::srem(inner, key, values))
  }

  /// Returns all the members of the set value stored at key.
  /// This has the same effect as running SINTER with one argument key.
  ///
  /// <https://redis.io/commands/smembers>
  fn smembers<K: Into<RedisKey>>(self, key: K) -> Box<Future<Item=(Self, Vec<RedisValue>), Error=RedisError>> {
    run_borrowed(self, |inner| commands::smembers(inner, key))
  }

}



