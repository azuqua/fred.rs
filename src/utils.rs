use futures::channel::oneshot::{
  channel as oneshot_channel
};
use futures::channel::mpsc::{
  UnboundedSender
};
use futures::future::{
  self,
  Future,
  Either,
  FutureExt,
  TryFutureExt
  // FutureResult
};
// use futures::Poll;
use futures::select;
use futures::stream::{
  self,
  Stream,
  StreamExt
};

use rand;
use rand::Rng;

use std::time::Duration;

use std::pin::Pin;
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

//use tokio_core::reactor::Handle;

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

use crate::async_ng::*;

//use tokio_timer::Timer;
/*
macro_rules! fry {
  ($expr:expr) => (match $expr {
    Ok(val) => val,
    //Err(err) => return Err(err.into())
    Err(err) => return crate::utils::future_error(err.into())
  })
}
*/

macro_rules! n(
  ($inner:expr) => {
    $inner.log_client_name(log::Level::Debug)
  }
);

macro_rules! ne(
  ($inner:expr) => {
    $inner.log_client_name(log::Level::Error)
  }
);

macro_rules! nw(
  ($inner:expr) => {
    $inner.log_client_name(log::Level::Warn)
  }
);

pub fn decr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_sub(1, Ordering::SeqCst).saturating_sub(1)
}

pub fn incr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
}

pub fn read_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.load(Ordering::SeqCst)
}

pub fn set_atomic(size: &Arc<AtomicUsize>, val: usize) -> usize {
  size.swap(val, Ordering::SeqCst)
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

pub fn future_error<T: 'static>(err: RedisError) -> Box<dyn Future<Output=Result<T, RedisError>>> {
  Box::new(future::err(err))
}

pub fn future_ok<T: 'static>(d: T) -> Box<dyn Future<Output=Result<T, RedisError>>> {
  Box::new(future::ok(d))
}

pub fn future_error_generic<T: 'static, E: 'static>(err: E) -> Pin<Box<dyn Future<Output=Result<T, E>>>> {
  Box::pin(future::err(err))
}

pub fn future_ok_generic<T: 'static, E: 'static>(d: T) -> Box<dyn Future<Output=Result<T, E>>> {
  Box::new(future::ok(d))
}
/*
pub fn stream_error<T: 'static>(e: RedisError) -> Box<dyn Stream<Item=Result<T, RedisError>>> {
  Box::new(future::err(e).into_stream())
}
*/


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

/*
pub fn request_response<F>(inner: &Arc<RedisClientInner>, func: F) -> Pin<Box<dyn Future<Output=Result<ProtocolFrame, RedisError>>>>
  where F: FnOnce() -> Result<(RedisCommandKind, Vec<RedisValue>), RedisError>
{
  let _ = match check_client_state(&inner.state, ClientState::Connected) {
    Ok(_) => (),
    Err(e) => return Box::pin(future::err(e))
  };
  let (kind, args) = match func() { // FIXME: get fry back
    Ok(t) => t,
    Err(e) => return Box::pin(future::err(e))
  };


  let (tx, rx) = oneshot_channel();
  let command = RedisCommand::new(kind, args, Some(tx));

   match send_command(&inner, command) {
     Ok(_) => Box::pin(rx.err_into::<RedisError>().flatten()),
     Err(e) => future_error(e)
   }
}
*/

pub async fn request_response_ft(inner: &Arc<RedisClientInner>, kind: RedisCommandKind, args: Vec<RedisValue>) -> Result<ProtocolFrame, RedisError> {
  check_client_state(&inner.state, ClientState::Connected)?;

  let (tx, rx) = oneshot_channel();
  let command = RedisCommand::new(kind, args, Some(tx));

   match send_command(&inner, command) {
     Ok(_) => match rx.await {
       Ok(result) => result,
       Err(e) => Err(e.into())
     }
     Err(e) => Err(e)
   }
}

pub async fn request_response<F>(inner: &Arc<RedisClientInner>, func: F) -> Result<ProtocolFrame, RedisError>
  where F: FnOnce() -> Result<(RedisCommandKind, Vec<RedisValue>), RedisError>
{
  let _ = match check_client_state(&inner.state, ClientState::Connected) {
    Ok(_) => (),
    Err(e) => return Err(e.into())
  };
  let (kind, args) = match func() { // FIXME:  replace fry with ?
    Ok(t) => t,
    Err(e) => return Err(e)
  };

  let (tx, rx) = oneshot_channel();
  let command = RedisCommand::new(kind, args, Some(tx));

   match send_command(&inner, command) {
     Ok(_) => match rx.await {
       Ok(result) => result,
       Err(e) => Err(e.into())
     }
     Err(e) => Err(e)
   }
}

pub async fn request_response_async(inner: &Arc<RedisClientInner>, kind: RedisCommandKind, args: Vec<RedisValue>) -> Result<ProtocolFrame, RedisError>
{
  check_client_state(&inner.state, ClientState::Connected)?;

  let (tx, rx) = oneshot_channel();
  let command = RedisCommand::new(kind, args, Some(tx));

   match send_command(&inner, command) {
     Ok(_) => match rx.await {
       Ok(result) => result,
       Err(e) => Err(e.into())
     }
     Err(e) => Err(e)
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

//pub fn split(inner: &Arc<RedisClientInner>, spawner: &Spawner, timeout: u64) -> Box<dyn Future<Output=Result<Vec<(RedisClient, RedisConfig)>, RedisError>>> {
pub fn split(inner: &Arc<RedisClientInner>, spawner: &Spawner, timeout: u64) -> Pin<Box<dyn Future<Output=Result<Vec<(RedisClient, RedisConfig)>, RedisError>> + Send >> {
//pub async fn split(inner: &Arc<RedisClientInner>, spawner: &Spawner, timeout: u64) -> Result<Vec<(RedisClient, RedisConfig)>, RedisError> {
  //use crate::owned::RedisClientOwned;
  // unimplemented!(); // FIXME: just a little problem

  let timeout = Duration::from_millis(timeout);
  let (tx, rx) = oneshot_channel();
  let split_command = RedisCommandKind::_Split(Some(SplitCommand {
    tx: Arc::new(RwLock::new(Some(tx))),
    key: multiplexer_utils::read_auth_key(&inner.config)
  }));
  let command = RedisCommand::new(split_command, vec![], None);

  //send_command(inner, command)?;
  if let Err(e) = send_command(inner, command) {
    // return Err(e);
    // return future_error(e);
    return Box::pin(futures::future::err(e))
  }

  let uses_tls = inner.config.read().deref().tls();
  let spawner = spawner.clone();
  let timer = inner.timer.clone(); // FIXME: can use global now?

  // FIXME: do we want to use the original timer? for some reason err_into fails on it right now
  // let timeout_ft = timer.sleep(timeout).err_into::<RedisError>();
  let timeout_ft = tokio_02::time::delay_for(timeout);

  //let () = rx;
  let rx: futures::channel::oneshot::Receiver<std::result::Result<std::vec::Vec<RedisConfig>, RedisError>> = rx;
  //let () = rx.flatten();
  //let rxf: Box<dyn Future<Output=std::result::Result<std::vec::Vec<RedisConfig>, RedisError>>> = Box::new(&mut rx);
  let connect_ft = rx.err_into::<RedisError>()
    .and_then(|result| futures::future::ready(result)) // FIXME: should be a better way to flatten
    .map_ok(move |configs| {
    //let () = configs;
  //let connect_ft = rx.and_then(move |configs| {
    let all_len = configs.len();

    stream::iter(configs.into_iter()).map(move |mut config| {
      // the underlying split() logic doesn't have the original tls flag, so it's copied above and restored here
      config.set_tls(uses_tls);

      let client = RedisClient::new(config.clone(), Some(timer.clone()));
      let err_client = client.clone();
      let client_ft = client.connect(&spawner).map(|_| ()); // FIXME: still need to pass spanwer here?

      trace!("Creating split clustered client...");
      spawner.spawn_std(client_ft);

      client.on_connect()
        .then(move |result| {
          match result {
            Ok(client) => future_ok((client, config)),
            Err(e) => Box::pin(err_client.quit().then(move |_| Err(e)))
          }
        })
    })
    .buffer_unordered(all_len)
    .fold(Vec::with_capacity(all_len), |mut memo, (client, config)| {
      memo.push((client, config));
      Ok::<_, RedisError>(memo)
    })
  });
  unimplemented!();

  /*
  select! {
    timeout = timeout_ft => match timeout {
      Ok((_, init_ft)) => {
        // timer_ft finished first (timeout)
        return Err(RedisError::new_timeout())
      },
      Err((timer_err, init_ft)) => {
        // timer had an error, try again without backoff
        warn!("Timer error splitting redis connections: {:?}", timer_err);
        return Err(timer_err)
      }
    },
    connect_done = connect_ft => match connect_done {
      Ok((clients, _)) => {
        return Ok(clients)
      },
      Err((init_err, _)) => {
        return Err(init_err)
      }
    }
  }
  */
  /*
  Box::new(timout_ft.select2(connect_ft).then(move |result| {
    match result {
      Ok(Either::Left((_, init_ft))) => {
        // timer_ft finished first (timeout)
        future_error(RedisError::new_timeout())
      },
      Ok(Either::Right((clients, timer_ft))) => {
        // initialization worked
        future_ok(clients)
      },
      Err(Either::Left((timer_err, init_ft))) => {
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
  */
}

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .gen_ascii_chars()
    .take(len)
    .collect()
}

pub fn pattern_pubsub_counts(result: Vec<RedisValue>) -> Result<Vec<usize>, RedisError> {
  let mut out = Vec::with_capacity(result.len() / 3);

  if result.len() > 0 {
    let mut idx = 2;
    while idx < result.len() {
      out.push(match result[idx] {
        RedisValue::Integer(ref i) => if *i < 0 {
          return Err(RedisError::new(RedisErrorKind::Unknown, "Invalid pattern pubsub channel count response."));
        }else{
          *i as usize
        },
        _ => return Err(RedisError::new(RedisErrorKind::Unknown, "Invalid pattern pubsub response."))
      });
      idx += 3;
    }
  }

  Ok(out)
}

/// Convert an `f64` to a redis string, supporting "+inf" and "-inf".
pub fn f64_to_redis_string(d: f64) -> Result<RedisValue, RedisError> {
  if d.is_infinite() && d.is_sign_negative() {
    Ok("-inf".into())
  }else if d.is_infinite() {
    Ok("+inf".into())
  }else if d.is_nan() {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Cannot use NaN as sorted set score."
    ))
  }else{
    Ok(d.to_string().into())
  }
}

/// Convert a redis string to an `f64`, supporting "+inf" and "-inf".
pub fn redis_string_to_f64(s: &str) -> Result<f64, RedisError> {
  if s == "+inf" {
    Ok(f64::INFINITY)
  }else if s == "-inf" {
    Ok(f64::NEG_INFINITY)
  }else{
    s.parse::<f64>().map_err(|_| RedisError::new(
      RedisErrorKind::Unknown, format!("Could not convert {} to floating point value.", s)
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_convert_normal_f64_to_string() {
    let f = 1.2345678;
    assert_eq!(RedisValue::from(f.to_string()), f64_to_redis_string(f).unwrap());
  }

  #[test]
  fn should_fail_converting_nan_to_string() {
    assert!(f64_to_redis_string(f64::NAN).is_err());
  }

  #[test]
  fn should_convert_pos_inf_to_string() {
    assert_eq!(f64_to_redis_string(f64::INFINITY).unwrap(), "+inf".into());
  }

  #[test]
  fn should_convert_neg_inf_to_string() {
    assert_eq!(f64_to_redis_string(f64::NEG_INFINITY).unwrap(), "-inf".into());
  }

  #[test]
  fn should_convert_string_to_f64() {
    let f = "1.234567";
    assert_eq!(redis_string_to_f64(f).unwrap(), 1.234567);
  }

  #[test]
  fn should_convert_pos_inf_string_to_f64() {
    assert_eq!(redis_string_to_f64("+inf").unwrap(), f64::INFINITY);
  }

  #[test]
  fn should_convert_neg_inf_string_to_f64() {
    assert_eq!(redis_string_to_f64("-inf").unwrap(), f64::NEG_INFINITY);
  }

  #[test]
  fn should_fail_converting_bad_string_to_f64() {
    assert!(redis_string_to_f64("foobarbaz").is_err());
  }

}
