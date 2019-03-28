use futures::sync::oneshot::{
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  UnboundedSender
};
use futures::future::{
  self,
  Future,
  Either,
  FutureResult
};
use futures::Poll;
use futures::stream::{
  self,
  Stream
};

use std::time::Duration;

use std::i64;
use std::f64;
use float_cmp::ApproxEq;

use crate::types::*;
use crate::error::*;

use parking_lot::RwLock;

use std::sync::Arc;
use std::ops::{Deref, DerefMut};

use redis_protocol::types::*;

use crate::error::{
  RedisError,
  RedisErrorKind
};
use crate::client::{RedisClient, RedisClientInner};

use tokio_core::reactor::Handle;

use crate::protocol::types::{
  RedisCommand,
  RedisCommandKind
};
use crate::multiplexer::types::SplitCommand;

use redis_protocol::types::{
  Frame as ProtocolFrame,
  FrameKind as ProtocolFrameKind
};

use std::sync::atomic::{
  Ordering,
  AtomicUsize
};

use crate::multiplexer::utils as multiplexer_utils;
use crate::protocol::utils as protocol_utils;

use tokio_timer::Timer;


macro_rules! fry {
  ($expr:expr) => (match $expr {
    Ok(val) => val,
    Err(err) => return crate::utils::future_error(err.into())
  })
}


pub fn decr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_sub(1, Ordering::SeqCst).saturating_sub(1)
}

pub fn incr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
}

pub fn read_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.load(Ordering::SeqCst)
}

pub fn check_client_state(actual: &RwLock<ClientState>, expected: ClientState) -> Result<(), RedisError> {
  if actual.read().deref() == &expected {
    Ok(())
  }else{
    Err(RedisError::new(
      RedisErrorKind::Unknown, format!("Invalid client connection state. Expected {:?} but found {:?}.", expected, actual)
    ))
  }
}

pub fn set_client_state(state: &RwLock<ClientState>, new_state: ClientState) {
  let mut state_guard = state.write();
  let mut state_ref = state_guard.deref_mut();
  *state_ref = new_state;
}

pub fn read_client_state(state: &RwLock<ClientState>) -> ClientState {
  state.read().deref().clone()
}

pub fn future_error<T: 'static>(err: RedisError) -> Box<Future<Item = T, Error = RedisError>> {
  Box::new(future::err(err))
}

pub fn future_ok<T: 'static>(d: T) -> Box<Future<Item = T, Error = RedisError>> {
  Box::new(future::ok(d))
}

pub fn future_error_generic<T: 'static, E: 'static>(err: E) -> Box<Future<Item=T, Error=E>> {
  Box::new(future::err(err))
}

pub fn future_ok_generic<T: 'static, E: 'static>(d: T) -> Box<Future<Item=T, Error=E>> {
  Box::new(future::ok(d))
}

pub fn stream_error<T: 'static>(e: RedisError) -> Box<Stream<Item=T, Error=RedisError>> {
  Box::new(future::err(e).into_stream())
}

pub fn reset_reconnect_attempts(reconnect: &RwLock<Option<ReconnectPolicy>>) {
  if let Some(ref mut reconnect) = reconnect.write().deref_mut() {
    reconnect.reset_attempts();
  }
}

pub fn u64_to_i64_max(u: u64) -> i64 {
  if u >= (i64::max_value() as u64) {
    i64::max_value()
  } else {
    u as i64
  }
}

pub fn incr_with_max(curr: u32, max: u32) -> Option<u32> {
  if max == 0 {
    Some(max)
  }else if curr >= max {
    None
  }else{
    Some(curr + 1)
  }
}

pub fn compare_f64(lhs: &f64, rhs: &f64) -> bool {
  *lhs == *rhs
}

pub fn to_url_string(host: &str, port: u16) -> String {
  format!("{}:{}", host, port)
}

pub fn read_closed_flag(closed: &RwLock<bool>) -> bool {
  closed.read().deref().clone()
}

pub fn set_closed_flag(closed: &RwLock<bool>, flag: bool) {
  let mut closed_guard = closed.write();
  let mut closed_ref = closed_guard.deref_mut();
  *closed_ref = flag;
}

// called by connect() and connect_with_policy(), and verifies the client isn't already waiting to attempt a reconnect.
pub fn check_and_set_closed_flag(closed: &RwLock<bool>, flag: bool) -> Result<(), RedisError> {
  let mut closed_guard = closed.write();
  let mut closed_ref = closed_guard.deref_mut();

  if *closed_ref != false {
    Err(RedisError::new(
      RedisErrorKind::Unknown, "Cannot connect to Redis server while waiting to reconnect."
    ))
  }else{
    *closed_ref = flag;
    Ok(())
  }
}

pub fn send_command(inner: &Arc<RedisClientInner>, command: RedisCommand) -> Result<(), RedisError> {
  incr_atomic(&inner.cmd_buffer_len);

  if command.kind == RedisCommandKind::Quit {
    let mut command_guard = inner.command_tx.write();

    let command_opt = command_guard.deref_mut().take();
    match command_opt {
      Some(tx) => tx.unbounded_send(command).map_err(|e| {
        RedisError::new(RedisErrorKind::Unknown, format!("Error sending command: {}.", e))
      }),
      None => Err(RedisError::new(
        RedisErrorKind::InvalidCommand, "Client is not connected."
      ))
    }
  }else{
    let command_guard = inner.command_tx.read();

    match *command_guard.deref() {
      Some(ref tx) => tx.unbounded_send(command).map_err(|e| {
        RedisError::new(RedisErrorKind::Unknown, format!("Error sending command: {}.", e))
      }),
      None => Err(RedisError::new(
        RedisErrorKind::InvalidCommand, "Client is not connected."
      ))
    }
  }
}

pub fn request_response<F>(inner: &Arc<RedisClientInner>, func: F) -> Box<Future<Item=ProtocolFrame, Error=RedisError>>
  where F: FnOnce() -> Result<(RedisCommandKind, Vec<RedisValue>), RedisError>
{
  let _ = fry!(check_client_state(&inner.state, ClientState::Connected));
  let (kind, args) = fry!(func());

  let (tx, rx) = oneshot_channel();
  let command = RedisCommand::new(kind, args, Some(tx));

   match send_command(&inner, command) {
     Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
     Err(e) => future_error(e)
   }
}

pub fn is_clustered(config: &RwLock<RedisConfig>) -> bool {
  config.read().deref().is_clustered()
}

pub fn set_reconnect_policy(policy: &RwLock<Option<ReconnectPolicy>>, new_policy: ReconnectPolicy) {
  let mut guard = policy.write();
  let mut guard_ref = guard.deref_mut();
  *guard_ref = Some(new_policy);
}


pub fn split(inner: &Arc<RedisClientInner>, handle: &Handle, timeout: u64) -> Box<Future<Item=Vec<(RedisClient, RedisConfig)>, Error=RedisError>> {
  use crate::owned::RedisClientOwned;

  let timeout = Duration::from_millis(timeout);
  let (tx, rx) = oneshot_channel();
  let split_command = RedisCommandKind::_Split(Some(SplitCommand {
    tx: Arc::new(RwLock::new(Some(tx))),
    key: multiplexer_utils::read_auth_key(&inner.config)
  }));
  let command = RedisCommand::new(split_command, vec![], None);

  if let Err(e) = send_command(inner, command) {
    return future_error(e);
  }

  let uses_tls = inner.config.read().deref().tls();
  let handle = handle.clone();
  let timer = inner.timer.clone();

  let timout_ft = timer.sleep(timeout).from_err::<RedisError>();

  let connect_ft =rx.flatten().and_then(move |configs| {
    let all_len = configs.len();

    stream::iter_ok(configs.into_iter()).map(move |mut config| {
      // the underlying split() logic doesn't have the original tls flag, so it's copied above and restored here
      config.set_tls(uses_tls);

      let client = RedisClient::new(config.clone(), Some(timer.clone()));
      let err_client = client.clone();
      let client_ft = client.connect(&handle).map(|_| ()).map_err(|_| ());

      trace!("Creating split clustered client...");
      handle.spawn(client_ft);

      client.on_connect()
        .map(move |client| (client, config))
        .then(move |result| {
          match result {
            Ok(out) => future_ok(out),
            Err(e) => Box::new(err_client.quit().then(move |_| Err(e)))
          }
        })
    })
    .buffer_unordered(all_len)
    .fold(Vec::with_capacity(all_len), |mut memo, (client, config)| {
      memo.push((client, config));
      Ok::<_, RedisError>(memo)
    })
  });

  Box::new(timout_ft.select2(connect_ft).then(move |result| {
    match result {
      Ok(Either::A((_, init_ft))) => {
        // timer_ft finished first (timeout)
        future_error(RedisError::new_timeout())
      },
      Ok(Either::B((clients, timer_ft))) => {
        // initialization worked
        future_ok(clients)
      },
      Err(Either::A((timer_err, init_ft))) => {
        // timer had an error, try again without backoff
        warn!("Timer error splitting redis connections: {:?}", timer_err);
        future_error(timer_err)
      },
      Err(Either::B((init_err, timer_ft))) => {
        // initialization had an error
        future_error(init_err)
      }
    }
  }))
}

