

use std::collections::{
  HashMap,
  HashSet,
  VecDeque
};

use std::cmp;

use std::hash::Hash;
use std::hash::Hasher;
use std::mem;

use super::error::*;

use utils;
use protocol::types::*;

pub use loop_serve::{
  ConnectionFuture
};

pub static ASYNC: &'static str = "ASYNC";

/// Options for the [info](https://redis.io/commands/info) command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InfoKind {
  Default,
  All,
  Keyspace,
  Cluster,
  CommandStats,
  Cpu,
  Replication,
  Stats,
  Persistence,
  Memory,
  Clients,
  Server
}

impl InfoKind {

  pub fn to_string(&self) -> &'static str {
    match *self {
      InfoKind::Default      => "default",
      InfoKind::All          => "all",
      InfoKind::Keyspace     => "keyspace",
      InfoKind::Cluster      => "cluster",
      InfoKind::CommandStats => "commandstats",
      InfoKind::Cpu          => "cpu",
      InfoKind::Replication  => "replication",
      InfoKind::Stats        => "stats",
      InfoKind::Persistence  => "persistence",
      InfoKind::Memory       => "memory",
      InfoKind::Clients      => "clients",
      InfoKind::Server       => "server"
    }
  }

}

/// The type of reconnection policy to use. This will apply to every connection used by the client.
///
/// `attempts` should be initialized with 0 but it doesn't really matter, the client will reset it as needed.
///
/// `min` and `max` refer to the min and max delay to use when reconnecting. These can be used to put bounds
/// around the amount of time between attempts.
///
/// Use a `max_attempts` value of `0` to retry forever.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconnectPolicy {
  /// Wait a constant amount of time between reconnect attempts, in ms.
  Constant {
    attempts: u32,
    max_attempts: u32,
    delay: u32,
  },
  /// Backoff reconnection attempts linearly, adding `delay` each time.
  Linear {
    attempts: u32,
    max_attempts: u32,
    max: u32,
    delay: u32
  },
  /// Backoff reconnection attempts exponentially, multiplying the last delay by `mult` each time.
  Exponential {
    attempts: u32,
    max_attempts: u32,
    min: u32,
    max: u32,
    mult: u32
  }
}

impl ReconnectPolicy {

  /// Reset the number of reconnection attempts. It's unlikely users will need to call this.
  pub fn reset_attempts(&mut self) {
    match *self {
      ReconnectPolicy::Constant { ref mut attempts, .. } => {
        *attempts = 0;
      },
      ReconnectPolicy::Linear { ref mut attempts, .. } => {
        *attempts = 0;
      },
      ReconnectPolicy::Exponential { ref mut attempts, .. } => {
        *attempts = 0;
      }
    }
  }

  /// Read the number of reconnection attempts.
  pub fn attempts(&self) -> u32 {
    match *self {
      ReconnectPolicy::Constant { ref attempts, .. } => attempts.clone(),
      ReconnectPolicy::Linear { ref attempts, .. } => attempts.clone(),
      ReconnectPolicy::Exponential { ref attempts, .. } => attempts.clone()
    }
  }

  /// Calculate the next delay, incrementing `attempts` in the process. It's unlikely users will need to call this.
  pub fn next_delay(&mut self) -> Option<u32> {
    match *self {
      ReconnectPolicy::Constant { ref mut attempts, delay, max_attempts } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None 
        };

        Some(delay)
      },
      ReconnectPolicy::Linear { ref mut attempts, max, max_attempts, delay } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None 
        };

        Some(cmp::min(max, delay.saturating_mul(*attempts)))
      },
      ReconnectPolicy::Exponential { ref mut attempts, min, max, max_attempts, mult } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None 
        };

        Some(cmp::min(max, mult.pow(*attempts - 1).saturating_mul(min)))
      }
    }
  }

}

/// Connection configuration for the Redis server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisConfig {
  Centralized {
    /// The hostname or IP address of the Redis server.
    host: String,
    /// The port on which the Redis server is listening. 
    port: u16,
    /// An optional authentication key to use after connecting.
    key: Option<String>,
    /// The maximum number of bytes that can be allocated for a value, or `None` for no limit.
    max_value_size: Option<usize>
  },
  Clustered {
    /// A vector of (Host, Port) tuples for nodes in the cluster. Only a subset of nodes in the cluster need to be provided here,
    /// the rest will be discovered via the CLUSTER NODES command.
    hosts: Vec<(String, u16)>,
    /// An optional authentication key to use after connecting.
    key: Option<String>,
    /// The maximum number of bytes that can be allocated for a value, or `None` for no limit.
    max_value_size: Option<usize>
  }
}

impl Default for RedisConfig {
  fn default() -> RedisConfig {
    RedisConfig::default_centralized()
  }
}

impl RedisConfig {

  pub fn new_centralized(host: String, port: u16, key: Option<String>) -> RedisConfig {
    RedisConfig::Centralized {
      host: host,
      port: port,
      key: key,
      max_value_size: None
    }
  }

  pub fn new_clustered(hosts: Vec<(String, u16)>, key: Option<String>) -> RedisConfig {
    RedisConfig::Clustered {
      hosts: hosts,
      key: key,
      max_value_size: None
    }
  }

  /// Create a centralized config with default settings for a local deployment.
  pub fn default_centralized() -> RedisConfig {
    RedisConfig::Centralized {
      host: "127.0.0.1".to_owned(),
      port: 6379,
      key: None,
      max_value_size: None
    }
  }

  /// Create a clustered config with the same defaults as specified in the `create-cluster` script provided by Redis.
  pub fn default_clustered() -> RedisConfig {
    RedisConfig::Clustered {
      hosts: vec![
        ("127.0.0.1".to_owned(), 30001),
        ("127.0.0.1".to_owned(), 30002),
        ("127.0.0.1".to_owned(), 30003),
      ],
      key: None,
      max_value_size: None
    }
  }

  /// Overwrite the auth key on this config.
  pub fn set_key<T: Into<String>>(&mut self, new_key: Option<T>) {
    match *self {
      RedisConfig::Centralized { ref mut key, .. } => {
        *key = new_key.map(|t| t.into())
      }, 
      RedisConfig::Clustered { ref mut key, .. } => {
        *key = new_key.map(|t| t.into())
      }
    }
  }

  /// Set the max size of values received over the socket, or `None` for no limit.
  pub fn set_max_size(&mut self, size: Option<usize>) {
    match *self {
      RedisConfig::Centralized {ref mut max_value_size, ..} => {
        *max_value_size = size;
      },
      RedisConfig::Clustered {ref mut max_value_size, ..} => {
        *max_value_size = size;
      }
    }
  }

  /// Read a copy of the `max_value_size`.
  pub fn get_max_size(&self) -> Option<usize> {
    match *self {
      RedisConfig::Centralized {ref max_value_size, ..} => max_value_size.clone(),
      RedisConfig::Clustered {ref max_value_size, ..} => max_value_size.clone()
    }
  }

}

/// Options for the `set` command.
///
/// https://redis.io/commands/set
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SetOptions {
  NX,
  XX
}

impl SetOptions {

  pub fn to_string(&self) -> &'static str {
    match *self {
      SetOptions::NX => "NX",
      SetOptions::XX => "XX"
    }
  }

}

/// Expiration options for the `set` command.
///
/// https://redis.io/commands/set
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expiration {
  EX(i64),
  PX(i64)
}

impl Expiration {

  pub fn into_args(self) -> (&'static str, i64) {
    match self {
      Expiration::EX(i) => ("EX", i),
      Expiration::PX(i) => ("PX", i)
    }
  }

}

/// The state of the underlying connection to the Redis server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientState {
  Disconnected,
  Disconnecting,
  Connected,
  Connecting
}

/// A key in Redis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisKey {
  key: String
}

impl RedisKey {

  pub fn new(key: String) -> RedisKey {
    RedisKey { key: key }
  }

  pub fn to_string(&self) -> &str {
    &self.key
  }

  pub fn into_string(self) -> String {
    self.key
  }

  pub fn take(&mut self) -> String {
    mem::replace(&mut self.key, String::new())
  }

}

impl From<String> for RedisKey {
  fn from(s: String) -> RedisKey {
    RedisKey { key: s }
  }
}

impl<'a> From<&'a str> for RedisKey {
  fn from(s: &'a str) -> RedisKey {
    RedisKey { key: s.to_owned() }
  }
}

impl<'a> From<&'a String> for RedisKey {
  fn from(s: &'a String) -> RedisKey {
    RedisKey { key: s.clone() }
  }
}

impl From<u8> for RedisKey {
  fn from(i: u8) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<u16> for RedisKey {
  fn from(i: u16) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<u32> for RedisKey {
  fn from(i: u32) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<u64> for RedisKey {
  fn from(i: u64) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<i8> for RedisKey {
  fn from(i: i8) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<i16> for RedisKey {
  fn from(i: i16) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<i32> for RedisKey {
  fn from(i: i32) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<i64> for RedisKey {
  fn from(i: i64) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<f32> for RedisKey {
  fn from(i: f32) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl From<f64> for RedisKey {
  fn from(i: f64) -> Self {
    RedisKey { key: i.to_string() }
  }
}

impl Hash for RedisKey {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}

/// Convenience struct for commands that take 1 or more keys.
pub struct MultipleKeys {
  keys: Vec<RedisKey>
}

impl MultipleKeys {

  pub fn new() -> MultipleKeys {
    MultipleKeys { keys: Vec::new() }
  }

  pub fn inner(self) -> Vec<RedisKey> {
    self.keys
  }

}

impl<T: Into<RedisKey>> From<T> for MultipleKeys {
  fn from(d: T) -> Self {
    MultipleKeys {
      keys: vec![d.into()]
    }
  }
}

impl<T: Into<RedisKey>> From<Vec<T>> for MultipleKeys {
  fn from(mut d: Vec<T>) -> Self {
    MultipleKeys {
      keys: d.drain(..).map(|k| k.into()).collect()
    }
  }
}

impl<T: Into<RedisKey>> From<VecDeque<T>> for MultipleKeys {
  fn from(mut d: VecDeque<T>) -> Self {
    MultipleKeys {
      keys: d.drain(..).map(|k| k.into()).collect()
    }
  }
}

/// The kind of value from Redis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisValueKind {
  Integer,
  String,
  Null,
  #[doc(hidden)]
  List,
  #[doc(hidden)]
  Set,
  #[doc(hidden)]
  Map
}

/// A value used in a Redis command.
#[derive(Clone, Debug)]
pub enum RedisValue {
  Integer(i64),
  String(String),
  Null,
  #[doc(hidden)]
  List(Vec<RedisValue>),
  #[doc(hidden)]
  Set(HashSet<RedisValue>),
  #[doc(hidden)]
  Map(HashMap<RedisKey, RedisValue>)
}

impl RedisValue {

  /// Returns the original string as an error if the parsing fails, otherwise this consumes the original string.
  pub fn into_integer(self) -> Result<RedisValue, RedisValue> {
    match self {
      RedisValue::String(s) => {
        match s.parse::<i64>() {
          Ok(d) => Ok(RedisValue::Integer(d)),
          Err(_) => Err(RedisValue::String(s))
        }
      },
      _ => Err(self)
    }
  }

  /// Check the specific data type used to represent the value.
  pub fn kind(&self) -> RedisValueKind {
    match *self {
      RedisValue::Integer(_)   => RedisValueKind::Integer,
      RedisValue::String(_)    => RedisValueKind::String,
      RedisValue::Set(_)       => RedisValueKind::Set,
      RedisValue::List(_)      => RedisValueKind::List,
      RedisValue::Map(_)       => RedisValueKind::Map,
      RedisValue::Null         => RedisValueKind::Null
    }
  }

  /// Check if the value is null.
  pub fn is_null(&self) -> bool {
    match *self {
      RedisValue::Null => true,
      _ => false
    }
  }

  /// Check if the value is an integer.
  pub fn is_integer(&self) -> bool {
    match *self {
      RedisValue::Integer(_) => true,
      _ => false
    }
  }

  /// Check if the value is a string.
  pub fn is_string(&self) -> bool {
    match *self {
      RedisValue::String(_) => true,
      _ => false
    }
  }

  /// Check if the inner string value can be coerced to an `f64`.
  pub fn is_float(&self) -> bool {
    match *self {
      RedisValue::String(ref s) => match s.parse::<f64>() {
        Ok(_) => true,
        Err(_) => false,
      },
      _ => false
    }
  }

  /// Read and return the inner value as a `u64`, if possible.
  pub fn into_u64(self) -> Option<u64> {
    match self{
      RedisValue::Integer(i) => Some(i as u64),
      _ => None
    }
  }

  ///  Read and return the inner value as a `i64`, if possible.
  pub fn into_i64(self) -> Option<i64> {
    match self{
      RedisValue::Integer(i) => Some(i),
      _ => None
    }
  }

  ///  Read and return the inner value as a `f64`, if possible.
  pub fn into_f64(self) -> Option<f64> {
    match self{
      RedisValue::String(s) => match s.parse::<f64>() {
        Ok(f) => Some(f),
        Err(_) => None
      },
      _ => None
    }
  }

  /// Read and return the inner `String` if the value is a `RedisValue::String`.
  pub fn into_string(self) -> Option<String> {
    match self {
      RedisValue::String(s) => Some(s),
      RedisValue::Integer(i) => Some(i.to_string()),
      _ => None
    }
  }

  /// Convert from a `u64` to the `i64` representation used by Redis. This can fail due to overflow so it is not implemented via the From trait.
  pub fn from_u64(d: u64) -> Result<RedisValue, RedisError> {
    if d >= (i64::max_value() as u64) {
      return Err(RedisError::new(
        RedisErrorKind::Unknown, "Unsigned integer too large."
      ));
    }

    Ok((d as i64).into())  
  }

  pub fn from_usize(d: usize) -> Result<RedisValue, RedisError> {
    if d >= (i64::max_value() as usize) {
      return Err(RedisError::new(
        RedisErrorKind::Unknown, "Unsigned integer too large."
      ));
    }

    Ok((d as i64).into())
  }

  /// Replace this `RedisValue` instance with `RedisValue::Null`, returning the original value.
  pub fn take(&mut self) -> RedisValue {
    mem::replace(self, RedisValue::Null)
  }

}

impl PartialEq for RedisValue {
  
  fn eq(&self, other: &RedisValue) -> bool {
    match *self {
      RedisValue::Integer(d) => match *other {
        RedisValue::Integer(_d) => d == _d,
        _ => false
      },
      RedisValue::String(ref d) => match *other {
        RedisValue::String(ref _d) => d == _d,
        _ => false
      },
      RedisValue::Null => match *other {
        RedisValue::Null => true,
        _ => false
      },
      RedisValue::Set(ref s) => match *other {
        RedisValue::Set(ref _s) => s == _s,
        _ => false
      },
      RedisValue::List(ref s) => match *other {
        RedisValue::List(ref _s) => s == _s,
        _ => false
      },
      RedisValue::Map(ref s) => match *other {
        RedisValue::Map(ref _s) => s == _s,
        _ => false
      }
    }
  }
}

impl Eq for RedisValue {}

impl Hash for RedisValue {
  fn hash<H: Hasher>(&self, state: &mut H) {
    match *self {
      RedisValue::Integer(d)     => d.hash(state),
      RedisValue::String(ref s)  => s.hash(state),
      RedisValue::Null           => NULL.hash(state),
      RedisValue::Set(_)         => panic!("unreachable. this is a bug."),
      RedisValue::List(_)        => panic!("unreachable. this is a bug."),
      RedisValue::Map(_)         => panic!("unreachable. this is a bug.")
    }
  }
}

impl From<u8> for RedisValue {
  fn from(d: u8) -> RedisValue {
    RedisValue::Integer(d as i64)
  }
}

impl From<u16> for RedisValue {
  fn from(d: u16) -> RedisValue {
    RedisValue::Integer(d as i64)
  }
}

impl From<u32> for RedisValue {
  fn from(d: u32) -> RedisValue {
    RedisValue::Integer(d as i64)
  }
}

impl From<i8> for RedisValue {
  fn from(d: i8) -> RedisValue {
    RedisValue::Integer(d as i64)
  }
}

impl From<i16> for RedisValue {
  fn from(d: i16) -> RedisValue {
    RedisValue::Integer(d as i64)
  }
}

impl From<i32> for RedisValue {
  fn from(d: i32) -> RedisValue {
    RedisValue::Integer(d as i64)
  }
}

impl From<i64> for RedisValue {
  fn from(d: i64) -> RedisValue {
    RedisValue::Integer(d)
  }
}

impl From<f32> for RedisValue {
  fn from(d: f32) -> RedisValue {
    RedisValue::String(d.to_string())
  }
}

impl From<f64> for RedisValue {
  fn from(d: f64) -> RedisValue {
    RedisValue::String(d.to_string())
  }
}

impl From<String> for RedisValue {
  fn from(d: String) -> RedisValue {
    RedisValue::String(d)
  }
}

impl<'a> From<&'a str> for RedisValue {
  fn from(d: &'a str) -> RedisValue {
    RedisValue::String(d.to_owned())
  }
}

impl<'a> From<&'a String> for RedisValue {
  fn from(s: &'a String) -> RedisValue {
    RedisValue::String(s.clone())
  }
}

impl<T: Into<RedisValue>> From<Option<T>> for RedisValue {
  fn from(d: Option<T>) -> Self {
    match d {
      Some(i) => i.into(),
      None => RedisValue::Null
    }
  }
}

impl From<RedisKey> for RedisValue {
  fn from(d: RedisKey) -> RedisValue {
    RedisValue::String(d.into_string())
  }
}

impl<T: Into<RedisValue>> From<Vec<T>> for RedisValue {
  fn from(mut d: Vec<T>) -> Self {
    let val: Vec<RedisValue> = d.drain(..)
      .map(|c| c.into())
      .collect();

    RedisValue::List(val)
  }
}

impl<T: Hash + Eq + Into<RedisValue>> From<HashSet<T>> for RedisValue {
  fn from(mut s: HashSet<T>) -> Self {
    let mut out: HashSet<RedisValue> = HashSet::with_capacity(s.len());

    for val in s.drain() {
      out.insert(val.into());
    }

    RedisValue::Set(out)
  }
}

impl<K: Hash + Eq + Into<RedisKey>, V: Into<RedisValue>> From<HashMap<K, V>> for RedisValue {
  fn from(mut m: HashMap<K, V>) -> Self {
    let mut out: HashMap<RedisKey, RedisValue> = HashMap::with_capacity(m.len());

    for (key, val) in m.drain() {
      out.insert(key.into(), val.into());
    }

    RedisValue::Map(out)
  }
}

// --------------------------

#[cfg(test)]
mod test {
  #![allow(dead_code)]
  #![allow(unused_imports)]
  #![allow(unused_variables)]
  #![allow(unused_mut)]
  #![allow(deprecated)]
  #![allow(unused_macros)]

  use super::*;



}