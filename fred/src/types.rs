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
use std::borrow::Cow;

use crate::error::*;

use crate::{utils, RedisClient};
pub use redis_protocol::types::Frame;

use redis_protocol::types::*;
use redis_protocol::NULL;
use crate::multiplexer::types::*;

use std::cmp::Ordering;
use crate::utils::send_command;
use crate::client::RedisClientInner;

use std::sync::Arc;
use crate::protocol::types::{RedisCommand, KeyScanInner, RedisCommandKind, ValueScanInner};

pub use fred_types::types::*;

#[doc(hidden)]
pub static ASYNC: &'static str = "ASYNC";

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
  /// If this struct is dropped without calling this function the stream will close without an error.
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
  /// If this struct is dropped without calling this function the stream will close without an error.
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
  /// If this struct is dropped without calling this function the stream will close without an error.
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
  /// If this struct is dropped without calling this function the stream will close without an error.
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
