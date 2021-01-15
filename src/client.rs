
use std::sync::Arc;
use parking_lot::RwLock;

use futures::{
  Future,
  Stream,
  lazy
};
use futures::sync::mpsc::{
  unbounded,
  UnboundedSender
};
use futures::sync::oneshot::{
  channel as oneshot_channel,
  Sender as OneshotSender
};

use std::collections::VecDeque;

use std::fmt;

use std::ops::{
  Deref,
  DerefMut
};

use tokio_core::reactor::Handle;

use crate::types::{ClientState, RedisConfig, RedisValue, ReconnectPolicy, ScanType, RedisKey, ScanResult, HScanResult, SScanResult, ZScanResult};
use crate::error::{RedisError, RedisErrorKind};
use crate::protocol::types::RedisCommand;
use crate::metrics::{LatencyStats, SizeStats, DistributionStats};
use crate::multiplexer::init;

use std::sync::atomic::AtomicUsize;

use std::borrow::Cow;
use std::mem;

use crate::commands;

const EMPTY_STR: &'static str = "";
const SPLIT_TIMEOUT_MS: u64 = 30_000;

#[macro_use]
use crate::utils;

#[doc(hidden)]
pub type CommandSender = UnboundedSender<RedisCommand>;

pub use utils::redis_string_to_f64;
pub use utils::f64_to_redis_string;

/// A patched version of `tokio-timer` to avoid a `panic` under heavy load.
///
/// See [tokio-timer-patched](https://crates.io/crates/tokio-timer-patched).
pub use tokio_timer::Timer;

/// A future representing the connection to a Redis server.
pub type ConnectionFuture = Box<Future<Item=Option<RedisError>, Error=RedisError>>;

#[doc(hidden)]
pub struct RedisClientInner {
  /// The client ID as seen by the server.
  pub id: RwLock<String>,
  /// The state of the underlying connection.
  pub state: RwLock<ClientState>,
  /// The redis config used for initializing connections.
  pub config: RwLock<RedisConfig>,
  /// An optional reconnect policy.
  pub policy: RwLock<Option<ReconnectPolicy>>,
  /// An mpsc sender for errors to `on_error` streams.
  pub error_tx: RwLock<VecDeque<UnboundedSender<RedisError>>>,
  /// An mpsc sender for commands to the multiplexer.
  pub command_tx: RwLock<Option<CommandSender>>,
  /// An mpsc sender for pubsub messages to `on_message` streams.
  pub message_tx: RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>,
  /// An mpsc sender for reconnection events to `on_reconnect` streams.
  pub reconnect_tx: RwLock<VecDeque<UnboundedSender<RedisClient>>>,
  /// MPSC senders for `on_connect` futures.
  pub connect_tx: RwLock<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>,
  /// A flag used to determine if the client was intentionally closed. This is used in the multiplexer reconnect logic
  /// to determine if `quit` was called while the client was waiting to reconnect.
  pub closed: RwLock<bool>,
  /// Latency metrics tracking.
  pub latency_stats: RwLock<LatencyStats>,
  /// Payload size metrics tracking for requests.
  pub req_size_stats: Arc<RwLock<SizeStats>>,
  /// Payload size metrics tracking for responses.
  pub res_size_stats: Arc<RwLock<SizeStats>>,
  /// A timer for handling timeouts and reconnection delays.
  pub timer: Timer,
  /// Command queue buffer size.
  pub cmd_buffer_len: Arc<AtomicUsize>,
  /// Number of message redeliveries.
  pub redeliver_count: Arc<AtomicUsize>
}

impl RedisClientInner {

  pub fn log_client_name(&self, level: log::Level) -> Cow<'static, str> {
    if log_enabled!(level) {
      Cow::Owned(self.id.read().deref().to_owned())
    }else{
      Cow::Borrowed(EMPTY_STR)
    }
  }

  pub fn change_client_name(&self, id: String) {
    let mut guard = self.id.write();
    let mut guard_ref = guard.deref_mut();

    mem::replace(guard_ref, id);
  }

  pub fn client_name(&self) -> String {
    self.id.read().deref().clone()
  }

}

/// A Redis client struct.
///
/// See the documentation for the `Borrowed` and `Owned` traits for how to execute commands against the server. Depending on your use case it may be beneficial to use different interfaces for
/// different use cases, and this library supports both an owned interface and a borrowed interface via different traits. Both traits implement the same high level interface, but provide
/// flexibility for use cases where taking ownership over the client may be limited vs use cases where owning the client instance may be preferred.
#[derive(Clone)]
pub struct RedisClient {
  pub(crate) inner: Arc<RedisClientInner>
}

impl fmt::Debug for RedisClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisClient]")
  }
}

impl<'a> From<&'a Arc<RedisClientInner>> for RedisClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> RedisClient {
    RedisClient { inner: inner.clone() }
  }
}

impl RedisClient {

  /// Create a new `RedisClient` instance without connecting.
  pub fn new(config: RedisConfig, timer: Option<Timer>) -> RedisClient {
    let state = ClientState::Disconnected;
    let latency = LatencyStats::default();
    let req_size = SizeStats::default();
    let res_size = SizeStats::default();
    let init_id = format!("fred-{}", utils::random_string(10));

    let inner = Arc::new(RedisClientInner {
      id: RwLock::new(init_id),
      config: RwLock::new(config),
      policy: RwLock::new(None),
      state: RwLock::new(state),
      error_tx: RwLock::new(VecDeque::new()),
      message_tx: RwLock::new(VecDeque::new()),
      reconnect_tx: RwLock::new(VecDeque::new()),
      connect_tx: RwLock::new(VecDeque::new()),
      command_tx: RwLock::new(None),
      closed: RwLock::new(false),
      latency_stats: RwLock::new(latency),
      req_size_stats: Arc::new(RwLock::new(req_size)),
      res_size_stats: Arc::new(RwLock::new(res_size)),
      timer: timer.unwrap_or(Timer::default()),
      cmd_buffer_len: Arc::new(AtomicUsize::new(0)),
      redeliver_count: Arc::new(AtomicUsize::new(0))
    });

    RedisClient { inner }
  }

  /// Read the number of request redeliveries.
  ///
  /// This is the number of times a request had to be sent again due to a connection closing while waiting on a response.
  pub fn read_redelivery_count(&self) -> usize {
    utils::read_atomic(&self.inner.redeliver_count)
  }

  /// Read and reset the number of request redeliveries.
  pub fn take_redelivery_count(&self) -> usize {
    utils::set_atomic(&self.inner.redeliver_count, 0)
  }

  /// Read the state of the underlying connection.
  pub fn state(&self) -> ClientState {
    self.inner.state.read().deref().clone()
  }

  /// Read latency metrics across all commands.
  pub fn read_latency_metrics(&self) -> DistributionStats {
    self.inner.latency_stats.read().deref().read_metrics()
  }

  /// Read and consume latency metrics, resetting their values afterwards.
  pub fn take_latency_metrics(&self) -> DistributionStats {
    self.inner.latency_stats.write().deref_mut().take_metrics()
  }

  /// Read request payload size metrics across all commands.
  pub fn read_req_size_metrics(&self) -> DistributionStats {
    self.inner.req_size_stats.read().deref().read_metrics()
  }

  /// Read and consume request payload size metrics, resetting their values afterwards.
  pub fn take_req_size_metrics(&self) -> DistributionStats {
    self.inner.req_size_stats.write().deref_mut().take_metrics()
  }

  /// Read response payload size metrics across all commands.
  pub fn read_res_size_metrics(&self) -> DistributionStats {
    self.inner.res_size_stats.read().deref().read_metrics()
  }

  /// Read and consume response payload size metrics, resetting their values afterwards.
  pub fn take_res_size_metrics(&self) -> DistributionStats {
    self.inner.res_size_stats.write().deref_mut().take_metrics()
  }

  /// Read the number of buffered commands waiting to be sent to the server.
  pub fn command_queue_len(&self) -> usize {
    utils::read_atomic(&self.inner.cmd_buffer_len)
  }

  /// Connect to the Redis server. The returned future will resolve when the connection to the Redis server has been fully closed by both ends.
  ///
  /// The `on_connect` function can be used to be notified when the client first successfully connects.
  pub fn connect(&self, handle: &Handle) -> ConnectionFuture {
    fry!(utils::check_client_state(&self.inner.state, ClientState::Disconnected));
    fry!(utils::check_and_set_closed_flag(&self.inner.closed, false));

    debug!("{} Connecting to Redis server.", n!(self.inner));

    init::connect(handle, self.inner.clone())
  }

  /// Connect to the Redis server with a `ReconnectPolicy` to apply if the connection closes due to an error.
  /// The returned future will resolve when `max_attempts` is reached on the `ReconnectPolicy`.
  ///
  /// Use the `on_error` and `on_reconnect` functions to be notified when the connection dies or successfully reconnects.
  /// Note that when the client automatically reconnects it will *not* re-select the previously selected database,
  /// nor will it re-subscribe to any pubsub channels. Use `on_reconnect` to implement that functionality if needed.
  ///
  /// Additionally, `on_connect` can be used to be notified when the client first successfully connects, since sometimes
  /// some special initialization is needed upon first connecting.
  pub fn connect_with_policy(&self, handle: &Handle, mut policy: ReconnectPolicy) -> ConnectionFuture {
    fry!(utils::check_client_state(&self.inner.state, ClientState::Disconnected));
    fry!(utils::check_and_set_closed_flag(&self.inner.closed, false));

    policy.reset_attempts();
    utils::set_reconnect_policy(&self.inner.policy, policy);

    debug!("{} Connecting to Redis server with reconnect policy.", n!(self.inner));
    init::connect(handle, self.inner.clone())
  }

  /// Listen for successful reconnection notifications. When using a config with a `ReconnectPolicy` the future
  /// returned by `connect_with_policy` will not resolve until `max_attempts` is reached, potentially running forever
  /// if set to 0. This function can be used to receive notifications whenever the client successfully reconnects
  /// in order to select the right database again, re-subscribe to channels, etc. A reconnection event is also
  /// triggered upon first connecting.
  pub fn on_reconnect(&self) -> Box<Stream<Item=Self, Error=RedisError>> {
    let (tx, rx) = unbounded();
    self.inner.reconnect_tx.write().deref_mut().push_back(tx);

    Box::new(rx.from_err::<RedisError>())
  }

  /// Returns a future that resolves when the client connects to the server.
  /// If the client is already connected this future will resolve immediately.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs
  /// to occur only on the first connection vs subsequent connections.
  pub fn on_connect(&self) -> Box<Future<Item=Self, Error=RedisError>> {
    if utils::read_client_state(&self.inner.state) == ClientState::Connected {
      return utils::future_ok(self.clone());
    }

    let (tx, rx) = oneshot_channel();
    self.inner.connect_tx.write().deref_mut().push_back(tx);

    Box::new(rx.from_err::<RedisError>().flatten())
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// Similar to `on_message`, this function does not need to be called again if the connection goes down.
  pub fn on_error(&self) -> Box<Stream<Item=RedisError, Error=RedisError>> {
    let (tx, rx) = unbounded();
    self.inner.error_tx.write().deref_mut().push_back(tx);

    Box::new(rx.from_err::<RedisError>())
  }

  /// Listen for `(channel, message)` tuples on the PubSub interface.
  ///
  /// If the connection to the Redis server goes down for any reason this function does *not* need to be called again.
  /// Messages will start appearing on the original stream after `subscribe` is called again.
  pub fn on_message(&self) -> Box<Stream<Item=(String, RedisValue), Error=RedisError>> {
    let (tx, rx) = unbounded();
    self.inner.message_tx.write().deref_mut().push_back(tx);

    Box::new(rx.from_err::<RedisError>())
  }

  /// Whether or not the client is using a clustered Redis deployment.
  pub fn is_clustered(&self) -> bool {
    utils::is_clustered(&self.inner.config)
  }

  /// Split a clustered redis client into a list of centralized clients for each primary node in the cluster.
  ///
  /// This is an expensive operation and should not be used frequently.
  pub fn split_cluster(&self, handle: &Handle) -> Box<Future<Item=Vec<(RedisClient, RedisConfig)>, Error=RedisError>> {
    if utils::is_clustered(&self.inner.config) {
      utils::split(&self.inner, handle, SPLIT_TIMEOUT_MS)
    }else{
      utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, "Client is not using a clustered deployment."
      ))
    }
  }

  /// Scan the redis database for keys matching the given pattern, if any. The scan operation can be canceled by dropping the returned stream.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan<P: Into<String>>(&self, pattern: Option<P>, count: Option<usize>, _type: Option<ScanType>) -> Box<Stream<Item=ScanResult, Error=RedisError>> {
    commands::scan(&self.inner, pattern, count, _type)
  }

  /// Scan the redis database for keys within the given hash key, if any. The scan operation can be canceled by dropping the returned stream.
  ///
  /// <https://redis.io/commands/hscan>
  pub fn hscan<K: Into<RedisKey>, P: Into<String>>(&self, key: K, pattern: Option<P>, count: Option<usize>) -> Box<Stream<Item=HScanResult, Error=RedisError>> {
    commands::hscan(&self.inner, key, pattern, count)
  }

  /// Scan the redis database for keys within the given set key, if any. The scan operation can be canceled by dropping the returned stream.
  ///
  /// <https://redis.io/commands/sscan>
  pub fn sscan<K: Into<RedisKey>, P: Into<String>>(&self, key: K, pattern: Option<P>, count: Option<usize>) -> Box<Stream<Item=SScanResult, Error=RedisError>> {
    commands::sscan(&self.inner, key, pattern, count)
  }

  /// Scan the redis database for keys within the given sorted set key, if any. The scan operation can be canceled by dropping the returned stream.
  ///
  /// <https://redis.io/commands/zscan>
  pub fn zscan<K: Into<RedisKey>, P: Into<String>>(&self, key: K, pattern: Option<P>, count: Option<usize>) -> Box<Stream<Item=ZScanResult, Error=RedisError>> {
    commands::zscan(&self.inner, key, pattern, count)
  }


}