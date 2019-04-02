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
use std::collections::HashMap;
use std::hash::Hash;

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

  fn ping(&self) -> Box<Future<Item=String, Error=RedisError>>;

  fn auth<V: Into<String>>(&self, value: V) -> Box<Future<Item=String, Error=RedisError>>;

  fn bgrewriteaof(&self) -> Box<Future<Item=String, Error=RedisError>>;

  fn bgsave(&self) -> Box<Future<Item=String, Error=RedisError>>;

  fn client_list(&self) -> Box<Future<Item=String, Error=RedisError>>;

  fn client_getname(&self) -> Box<Future<Item=Option<String>, Error=RedisError>>;

  fn client_setname<V: Into<String>>(&self, name: V) -> Box<Future<Item=Option<String>, Error=RedisError>>;

  fn dbsize(&self) -> Box<Future<Item=usize, Error=RedisError>>;

  fn dump<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<String>, Error=RedisError>>;

  fn exists<K: Into<MultipleKeys>>(&self, keys: K) -> Box<Future<Item=usize, Error=RedisError>>;

  fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Box<Future<Item=bool, Error=RedisError>>;

  fn expire_at<K: Into<RedisKey>>(&self, key: K, timestamp: i64) -> Box<Future<Item=bool, Error=RedisError>>;

  fn flushdb(&self, _async: bool) -> Box<Future<Item=String, Error=RedisError>>;

  fn getrange<K: Into<RedisKey>>(&self, key: K, start: usize, end: usize) -> Box<Future<Item=String, Error=RedisError>>;

  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(&self, key: K, value: V) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>>;

  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<Future<Item=usize, Error=RedisError>>;

  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=bool, Error=RedisError>>;

  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>>;

  fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=HashMap<String, RedisValue>, Error=RedisError>>;

  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F, incr: i64) -> Box<Future<Item=i64, Error=RedisError>>;

  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F, incr: f64) -> Box<Future<Item=f64, Error=RedisError>>;

  fn hkeys<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Vec<String>, Error=RedisError>>;

  fn hlen<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=usize, Error=RedisError>>;

  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>>;

  fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>>(&self, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=String, Error=RedisError>>;

  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>>;

  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>>;

  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=usize, Error=RedisError>>;

  fn hvals<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>>;

  fn llen<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=usize, Error=RedisError>>;

  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<Future<Item=usize, Error=RedisError>>;

  fn lpop<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>>;

  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>>;

  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>>;

  fn smembers<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>>;

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

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping(&self) -> Box<Future<Item=String, Error=RedisError>> {
    commands::ping(&self.inner)
  }

  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<V: Into<String>>(&self, value: V) -> Box<Future<Item=String, Error=RedisError>> {
    commands::auth(&self.inner, value)
  }

  /// Instruct Redis to start an Append Only File rewrite process. Returns ok.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  fn bgrewriteaof(&self) -> Box<Future<Item=String, Error=RedisError>> {
    commands::bgrewriteaof(&self.inner)
  }

  /// Save the DB in background. Returns ok.
  ///
  /// <https://redis.io/commands/bgsave>
  fn bgsave(&self) -> Box<Future<Item=String, Error=RedisError>> {
    commands::bgsave(&self.inner)
  }

  /// Returns information and statistics about the client connections.
  ///
  /// <https://redis.io/commands/client-list>
  fn client_list(&self) -> Box<Future<Item=String, Error=RedisError>> {
    commands::client_list(&self.inner)
  }

  /// Returns the name of the current connection as a string, or None if no name is set.
  ///
  /// <https://redis.io/commands/client-getname>
  fn client_getname(&self) -> Box<Future<Item=Option<String>, Error=RedisError>> {
    commands::client_getname(&self.inner)
  }

  /// Assigns a name to the current connection. Returns ok if successful, None otherwise.
  ///
  /// <https://redis.io/commands/client-setname>
  fn client_setname<V: Into<String>>(&self, name: V) -> Box<Future<Item=Option<String>, Error=RedisError>> {
    commands::client_setname(&self.inner, name)
  }

  /// Return the number of keys in the currently-selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  fn dbsize(&self) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::dbsize(&self.inner)
  }

  /// Serialize the value stored at key in a Redis-specific format and return it as bulk string.
  /// If key does not exist None is returned
  ///
  /// <https://redis.io/commands/dump>
  fn dump<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<String>, Error=RedisError>> {
    commands::dump(&self.inner, key)
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  fn exists<K: Into<MultipleKeys>>(&self, keys: K) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::exists(&self.inner, keys)
  }

  /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expire>
  fn expire<K: Into<RedisKey>>(&self, key: K, seconds: i64) -> Box<Future<Item=bool, Error=RedisError>> {
    commands::expire(&self.inner, key, seconds)
  }

  /// Set a timeout on key based on a UNIX timestamp. After the timeout has expired, the key will automatically be deleted.
  /// Returns `true` if timeout set, `false` if key does not exist.
  ///
  /// <https://redis.io/commands/expireat>
  fn expire_at<K: Into<RedisKey>>(&self, key: K, timestamp: i64) -> Box<Future<Item=bool, Error=RedisError>> {
    commands::expire_at(&self.inner, key, timestamp)
  }

  /// Delete all the keys in the currently selected database.
  /// Returns a string reply.
  ///
  /// <https://redis.io/commands/flushdb>
  fn flushdb(&self, _async: bool) -> Box<Future<Item=String, Error=RedisError>> {
    commands::flushdb(&self.inner, _async)
  }

  /// Returns the substring of the string value stored at key, determined by the offsets start and end (both inclusive).
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  fn getrange<K: Into<RedisKey>>(&self, key: K, start: usize, end: usize) -> Box<Future<Item=String, Error=RedisError>> {
    commands::getrange(&self.inner, key, start, end)
  }

  /// Atomically sets key to value and returns the old value stored at key.
  /// Returns error if key does not hold string value. Returns None if key does not exist.
  ///
  /// <https://redis.io/commands/getset>
  fn getset<V: Into<RedisValue>, K: Into<RedisKey>>(&self, key: K, value: V) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    commands::getset(&self.inner, key, value)
  }

  /// Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are ignored.
  /// If key does not exist, it is treated as an empty hash and this command returns 0.
  ///
  /// <https://redis.io/commands/hdel>
  fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::hdel(&self.inner, key, fields)
  }

  /// Returns `true` if `field` exists on `key`.
  ///
  /// <https://redis.io/commands/hexists>
  fn hexists<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=bool, Error=RedisError>> {
    commands::hexists(&self.inner, key, field)
  }

  /// Returns the value associated with field in the hash stored at key.
  ///
  /// <https://redis.io/commands/hget>
  fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    commands::hget(&self.inner, key, field)
  }

  /// Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its value
  /// Returns an empty hashmap if hash is empty.
  ///
  /// <https://redis.io/commands/hgetall>
  fn hgetall<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=HashMap<String, RedisValue>, Error=RedisError>> {
    commands::hgetall(&self.inner, key)
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `incr`. If key does not exist, a new key holding a hash is created.
  /// If field does not exist the value is set to 0 before the operation is performed.
  ///
  /// <https://redis.io/commands/hincrby>
  fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F, incr: i64) -> Box<Future<Item=i64, Error=RedisError>> {
    commands::hincrby(&self.inner, key, field, incr)
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified increment.
  /// If the field does not exist, it is set to 0 before performing the operation.
  /// Returns an error if field value contains wrong type or content/increment are not parsable.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F, incr: f64) -> Box<Future<Item=f64, Error=RedisError>> {
    commands::hincrbyfloat(&self.inner, key, field, incr)
  }

  /// Returns all field names in the hash stored at key.
  /// Returns an empty vec if the list is empty.
  /// Null fields are converted to "nil".
  ///
  /// <https://redis.io/commands/hkeys>
  fn hkeys<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Vec<String>, Error=RedisError>> {
    commands::hkeys(&self.inner, key)
  }

  /// Returns the number of fields contained in the hash stored at key.
  ///
  /// <https://redis.io/commands/hlen>
  fn hlen<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::hlen(&self.inner, key)
  }

  /// Returns the values associated with the specified fields in the hash stored at key.
  /// Values in a returned list may be null.
  ///
  /// <https://redis.io/commands/hmget>
  fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>>(&self, key: K, fields: F) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
    commands::hmget(&self.inner, key, fields)
  }

  /// Sets the specified fields to their respective values in the hash stored at key. This command overwrites any specified fields already existing in the hash.
  /// If key does not exist, a new key holding a hash is created.
  ///
  /// <https://redis.io/commands/hmset>
  fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>>(&self, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=String, Error=RedisError>> {
    commands::hmset(&self.inner, key, values)
  }

  /// Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
  /// If field already exists in the hash, it is overwritten.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means field already exists and was overwritten.
  ///
  /// <https://redis.io/commands/hset>
  fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::hset(&self.inner, key, field, value)
  }

  /// Sets field in the hash stored at key to value, only if field does not yet exist.
  /// If key does not exist, a new key holding a hash is created.
  /// Note: Return value of 1 means new field was created and set. Return of 0 means no operation performed.
  ///
  /// <https://redis.io/commands/hsetnx>
  fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::hsetnx(&self.inner, key, field, value)
  }

  /// Returns the string length of the value associated with field in the hash stored at key.
  /// If the key or the field do not exist, 0 is returned.
  ///
  /// <https://redis.io/commands/hstrlen>
  fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::hstrlen(&self.inner, key, field)
  }

  /// Returns all values in the hash stored at key.
  /// Returns an empty vector if the list is empty.
  ///
  /// <https://redis.io/commands/hvals>
  fn hvals<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
    commands::hvals(&self.inner, key)
  }

  /// Returns the length of the list stored at key. If key does not exist, it is interpreted as an
  /// empty list and 0 is returned. An error is returned when the value stored at key is not a
  /// list.
  ///
  /// <https://redis.io/commands/llen>
  fn llen<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::llen(&self.inner, key)
  }

  /// Insert the specified value at the head of the list stored at key. If key does not exist,
  /// it is created as empty list before performing the push operations. When key holds a value
  /// that is not a list, an error is returned.
  ///
  /// <https://redis.io/commands/lpush>
  fn lpush<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::lpush(&self.inner, key, value)
  }

  /// Removes and returns the first element of the list stored at key.
  ///
  /// <https://redis.io/commands/lpop>
  fn lpop<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
    commands::lpop(&self.inner, key)
  }

  /// Add the specified value to the set stored at key. Values that are already a member of this set are ignored.
  /// If key does not exist, a new set is created before adding the specified value.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/sadd>
  fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::sadd(&self.inner, key, values)
  }

  /// Remove the specified value from the set stored at key. Values that are not a member of this set are ignored.
  /// If key does not exist, it is treated as an empty set and this command returns 0.
  /// An error is returned when the value stored at key is not a set.
  ///
  /// <https://redis.io/commands/srem>
  fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(&self, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>> {
    commands::srem(&self.inner, key, values)
  }

  /// Returns all the members of the set value stored at key.
  /// This has the same effect as running SINTER with one argument key.
  ///
  /// <https://redis.io/commands/smembers>
  fn smembers<K: Into<RedisKey>>(&self, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
    commands::smembers(&self.inner, key)
  }

}



