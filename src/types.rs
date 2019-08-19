
use futures::{
  Future,
  Stream
};

use crate::error::*;

use std::collections::{
  HashMap,
  HashSet,
  VecDeque
};

use std::cmp;

use std::hash::Hash;
use std::hash::Hasher;
use std::mem;

use crate::error::*;

use crate::{utils, RedisClient};

use redis_protocol::types::*;
use redis_protocol::NULL;
use crate::multiplexer::types::*;

pub use redis_protocol::types::Frame;

use std::cmp::Ordering;
use crate::utils::send_command;
use crate::client::RedisClientInner;

use std::sync::Arc;
use crate::protocol::types::{RedisCommand, KeyScanInner, RedisCommandKind, ValueScanInner};

#[doc(hidden)]
pub static ASYNC: &'static str = "ASYNC";

/// The types of values supported by the [type](https://redis.io/commands/type) command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanType {
  Set,
  String,
  ZSet,
  List,
  Hash,
  Stream
}

impl ScanType {

  pub fn to_str(&self) -> &'static str {
    match *self {
      ScanType::Set    => "set",
      ScanType::String => "string",
      ScanType::List   => "list",
      ScanType::ZSet   => "zset",
      ScanType::Hash   => "hash",
      ScanType::Stream => "stream"
    }
  }

}

/// The result of a SCAN operation.
pub struct ScanResult {
  pub(crate) results: Option<Vec<RedisKey>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: KeyScanInner,
  pub(crate) can_continue: bool
}

impl ScanResult {

  /// Read the current cursor from the SCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the SCAN operation.
  pub fn results(&self) -> &Option<Vec<RedisKey>> {
    &self.results
  }

  /// Take ownership over the results of the SCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<Vec<RedisKey>> {
    self.results.take()
  }

  /// Move on to the next page of results from the SCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since a common use case is to read values while scanning, and due to network latency this
  /// could cause the internal buffer backing the stream to grow out of control very quickly. By requiring the caller to call `next` this interface provides a mechanism for throttling the throughput of the SCAN call.
  /// If this struct is dropped without calling this the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error has occurred.
  /// If this happens the error will appear on the wrapping stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let cmd = RedisCommand {
      kind: RedisCommandKind::Scan(self.scan_state),
      args: self.args,
      attempted: 0,
      tx: None
    };

    send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the SCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `scan` again on the client will initiate a new SCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient { inner: self.inner.clone() }
  }

}

/// The result of a HSCAN operation.
pub struct HScanResult {
  pub(crate) results: Option<HashMap<RedisKey, RedisValue>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: ValueScanInner,
  pub(crate) can_continue: bool
}

impl HScanResult {

  /// Read the current cursor from the SCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the HSCAN operation.
  pub fn results(&self) -> &Option<HashMap<RedisKey, RedisValue>> {
    &self.results
  }

  /// Take ownership over the results of the HSCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<HashMap<RedisKey, RedisValue>> {
    self.results.take()
  }

  /// Move on to the next page of results from the HSCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since a common use case is to read values while scanning, and due to network latency this
  /// could cause the internal buffer backing the stream to grow out of control very quickly. By requiring the caller to call `next` this interface provides a mechanism for throttling the throughput of the SCAN call.
  /// If this struct is dropped without calling this the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error has occurred.
  /// If this happens the error will appear on the wrapping stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let cmd = RedisCommand {
      kind: RedisCommandKind::Hscan(self.scan_state),
      args: self.args,
      attempted: 0,
      tx: None
    };

    send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the HSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `hscan` again on the client will initiate a new HSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient { inner: self.inner.clone() }
  }

}

/// The result of a SCAN operation.
pub struct SScanResult {
  pub(crate) results: Option<Vec<RedisValue>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: ValueScanInner,
  pub(crate) can_continue: bool
}

impl SScanResult {

  /// Read the current cursor from the SSCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the SCAN operation.
  pub fn results(&self) -> &Option<Vec<RedisValue>> {
    &self.results
  }

  /// Take ownership over the results of the SSCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<Vec<RedisValue>> {
    self.results.take()
  }

  /// Move on to the next page of results from the SSCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since a common use case is to read values while scanning, and due to network latency this
  /// could cause the internal buffer backing the stream to grow out of control very quickly. By requiring the caller to call `next` this interface provides a mechanism for throttling the throughput of the SCAN call.
  /// If this struct is dropped without calling this the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error has occurred.
  /// If this happens the error will appear on the wrapping stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let cmd = RedisCommand {
      kind: RedisCommandKind::Sscan(self.scan_state),
      args: self.args,
      attempted: 0,
      tx: None
    };

    send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the SSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `sscan` again on the client will initiate a new SSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient { inner: self.inner.clone() }
  }

}

/// The result of a SCAN operation.
pub struct ZScanResult {
  pub(crate) results: Option<Vec<(RedisValue, f64)>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: ValueScanInner,
  pub(crate) can_continue: bool
}

impl ZScanResult {

  /// Read the current cursor from the ZSCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the ZSCAN operation.
  pub fn results(&self) -> &Option<Vec<(RedisValue, f64)>> {
    &self.results
  }

  /// Take ownership over the results of the ZSCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<Vec<(RedisValue, f64)>> {
    self.results.take()
  }

  /// Move on to the next page of results from the ZSCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since a common use case is to read values while scanning, and due to network latency this
  /// could cause the internal buffer backing the stream to grow out of control very quickly. By requiring the caller to call `next` this interface provides a mechanism for throttling the throughput of the SCAN call.
  /// If this struct is dropped without calling this the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error has occurred.
  /// If this happens the error will appear on the wrapping stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let cmd = RedisCommand {
      kind: RedisCommandKind::Zscan(self.scan_state),
      args: self.args,
      attempted: 0,
      tx: None
    };

    send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the ZSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `zscan` again on the client will initiate a new ZSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient { inner: self.inner.clone() }
  }

}

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

  pub fn to_str(&self) -> &'static str {
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
    /// Whether or not to use TLS. This will only take effect when `enable-tls` is used.
    tls: bool
  },
  Clustered {
    /// A vector of (Host, Port) tuples for nodes in the cluster. Only a subset of nodes in the cluster need to be provided here,
    /// the rest will be discovered via the CLUSTER NODES command.
    hosts: Vec<(String, u16)>,
    /// An optional authentication key to use after connecting.
    key: Option<String>,
    /// Whether or not to use TLS. This will only take effect when `enable-tls` is used.
    tls: bool
  }
}

impl Default for RedisConfig {
  fn default() -> RedisConfig {
    RedisConfig::default_centralized()
  }
}

impl RedisConfig {

  pub fn new_centralized<S: Into<String>>(host: S, port: u16, key: Option<String>) -> RedisConfig {
    RedisConfig::Centralized {
      host: host.into(),
      port: port,
      key: key,
      tls: false
    }
  }

  pub fn new_clustered<S: Into<String>>(mut hosts: Vec<(S, u16)>, key: Option<String>) -> RedisConfig {
    let hosts = hosts.drain(..)
      .map(|(s, p)| (s.into(), p))
      .collect();

    RedisConfig::Clustered {
      hosts: hosts,
      key: key,
      tls: false
    }
  }

  /// Create a centralized config with default settings for a local deployment.
  pub fn default_centralized() -> RedisConfig {
    RedisConfig::Centralized {
      host: "127.0.0.1".to_owned(),
      port: 6379,
      key: None,
      tls: false
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
      tls: false
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

  /// Check if the config is for a clustered Redis deployment.
  pub fn is_clustered(&self) -> bool {
    match *self {
      RedisConfig::Centralized {..} => false,
      RedisConfig::Clustered {..} => true
    }
  }

  /// Whether or not the config uses TLS.
  #[cfg(feature="enable-tls")]
  pub fn tls(&self) -> bool {
    match *self {
      RedisConfig::Centralized {ref tls,..} => *tls,
      RedisConfig::Clustered {ref tls,..} => *tls
    }
  }

  /// Whether or not the config uses TLS.
  #[cfg(not(feature="enable-tls"))]
  pub fn tls(&self) -> bool {
    false
  }

  /// Change the TLS flag.
  pub fn set_tls(&mut self, flag: bool) {
    match *self {
      RedisConfig::Centralized {ref mut tls,..} => { *tls = flag; },
      RedisConfig::Clustered {ref mut tls,..} => { *tls = flag; }
    }
  }

}


/// Options for the [set](https://redis.io/commands/set) command.
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

/// Expiration options for the [set](https://redis.io/commands/set) command.
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
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RedisKey {
  pub key: String
}

impl RedisKey {

  pub fn new<S: Into<String>>(key: S) -> RedisKey {
    RedisKey { key: key.into() }
  }

  pub fn as_str(&self) -> &str {
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
    RedisKey { key: s.to_owned() }
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
      keys: d.into_iter().map(|k| k.into()).collect()
    }
  }
}

impl<T: Into<RedisKey>> From<VecDeque<T>> for MultipleKeys {
  fn from(mut d: VecDeque<T>) -> Self {
    MultipleKeys {
      keys: d.into_iter().map(|k| k.into()).collect()
    }
  }
}


/// Convenience struct for commands that take 1 or more values.
pub struct MultipleValues {
  values: Vec<RedisValue>
}

impl MultipleValues {

  pub fn new() -> MultipleValues {
    MultipleValues { values: Vec::new() }
  }

  pub fn inner(self) -> Vec<RedisValue> {
    self.values
  }

  pub fn len(&self) -> usize {
    self.values.len()
  }

}

impl<T: Into<RedisValue>> From<T> for MultipleValues {
  fn from(d: T) -> Self {
    MultipleValues {
      values: vec![d.into()]
    }
  }
}

impl<T: Into<RedisValue>> From<Vec<T>> for MultipleValues {
  fn from(mut d: Vec<T>) -> Self {
    MultipleValues {
      values: d.into_iter().map(|k| k.into()).collect()
    }
  }
}

impl<T: Into<RedisValue>> From<VecDeque<T>> for MultipleValues {
  fn from(mut d: VecDeque<T>) -> Self {
    MultipleValues {
      values: d.into_iter().map(|k| k.into()).collect()
    }
  }
}

/// Convenience struct for the ZADD command to accept 1 or more `(score, value)` arguments.
pub struct MultipleZaddValues {
  values: Vec<(f64, RedisValue)>
}

impl MultipleZaddValues {

  pub fn new() -> MultipleZaddValues {
    MultipleZaddValues { values: Vec::new() }
  }

  pub fn inner(self) -> Vec<(f64, RedisValue)> {
    self.values
  }

  pub fn len(&self) -> usize {
    self.values.len()
  }

}

impl<T: Into<RedisValue>> From<(f64, T)> for MultipleZaddValues {
  fn from((s, v): (f64, T)) -> Self {
    MultipleZaddValues { values: vec![(s, v.into())] }
  }
}

impl<T: Into<RedisValue>> From<Vec<(f64, T)>> for MultipleZaddValues {
  fn from(d: Vec<(f64, T)>) -> Self {
    MultipleZaddValues {
      values: d.into_iter().map(|(s, v)| (s, v.into())).collect()
    }
  }
}

impl<T: Into<RedisValue>> From<VecDeque<(f64, T)>> for MultipleZaddValues {
  fn from(d: VecDeque<(f64, T)>) -> Self {
    MultipleZaddValues {
      values: d.into_iter().map(|(s, v)| (s, v.into())).collect()
    }
  }
}

/// The kind of value from Redis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisValueKind {
  Integer,
  String,
  Null
}

/// A value used in a Redis command.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RedisValue {
  Integer(i64),
  String(String),
  Null
}

impl RedisValue {

  /// Attempt to convert the value into an integer, returning the original string as an error if the parsing fails, otherwise this consumes the original string.
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
      RedisValue::String(ref s) => s.parse::<f64>().is_ok(),
      _ => false
    }
  }

  /// Read and return the inner value as a `u64`, if possible.
  pub fn as_u64(&self) -> Option<u64> {
    match self{
      RedisValue::Integer(ref i) => if *i >= 0 {
        Some(*i as u64)
      }else{
        None
      },
      RedisValue::String(ref s) => s.parse::<u64>().ok(),
      _ => None
    }
  }

  ///  Read and return the inner value as a `i64`, if possible.
  pub fn as_i64(&self) -> Option<i64> {
    match self{
      RedisValue::Integer(ref i) => Some(*i),
      RedisValue::String(ref s) => s.parse::<i64>().ok(),
      _ => None
    }
  }

  ///  Read and return the inner value as a `f64`, if possible.
  pub fn as_f64(&self) -> Option<f64> {
    match self{
      RedisValue::String(ref s) => s.parse::<f64>().ok(),
      RedisValue::Integer(ref i) => Some(*i as f64),
      _ => None
    }
  }

  /// Read and return the inner `String` if the value is a string or integer.
  pub fn into_string(self) -> Option<String> {
    match self {
      RedisValue::String(s) => Some(s),
      RedisValue::Integer(i) => Some(i.to_string()),
      _ => None
    }
  }
  /// Read and return the inner `String` if the value is a string or integer.
  pub fn as_string(&self) -> Option<String> {
    match self {
      RedisValue::String(ref s) => Some(s.to_owned()),
      RedisValue::Integer(ref i) => Some(i.to_string()),
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

impl Hash for RedisValue {
  fn hash<H: Hasher>(&self, state: &mut H) {
    match *self {
      RedisValue::Integer(d)     => d.hash(state),
      RedisValue::String(ref s)  => s.hash(state),
      RedisValue::Null           => NULL.hash(state),
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
