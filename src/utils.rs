#![allow(dead_code)]

use futures::future;
use futures::sync::oneshot::{
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  UnboundedSender
};
use futures::{
  Future,
  Stream,
  stream
};
use tokio_timer::Timer;
use std::time::Duration;

use std::i64;
use std::f64;

use types::*;

use parking_lot::RwLock;

use std::sync::Arc;
use std::ops::Deref;
use std::ops::DerefMut;

use protocol::types::*;

use error::{
  RedisError,
  RedisErrorKind
};
use ::RedisClient;
use ::utils as client_utils;
use ::multiplexer::utils as multiplexer_utils;

use std::rc::Rc;
use std::cell::RefCell;
use tokio_core::reactor::Handle;

macro_rules! fry {
  ($expr:expr) => (match $expr {
    Ok(val) => val,
    Err(err) => return Box::new(::futures::future::err(err.into()))
  })
}

pub fn client_state_error(expected: ClientState, actual: &ClientState) -> RedisError {
  RedisError::new(
    RedisErrorKind::Unknown, format!("Invalid client connection state. Expected {:?} but found {:?}.", expected, actual)
  )
}

pub fn check_client_state(expected: ClientState, actual: &Arc<RwLock<ClientState>>) -> Result<(), RedisError> {
  let state_guard = actual.read();
  let state_ref = state_guard.deref();

  if *state_ref != expected {
    Err(client_state_error(expected, state_ref))
  }else{
    Ok(())
  }
}

pub fn set_client_state(state: &Arc<RwLock<ClientState>>, new_state: ClientState) {
  let mut state_guard = state.write();
  let mut state_ref = state_guard.deref_mut();
  *state_ref = new_state;
}

pub fn check_connected(state: &Arc<RwLock<ClientState>>) -> Result<(), RedisError> {
  let state_guard = state.read();
  let state_ref = state_guard.deref();

  match *state_ref {
    ClientState::Connected { .. } => Ok(()),
    _ => Err(RedisError::new(
      RedisErrorKind::InvalidCommand, "Client is not connected."
    ))
  }
}

pub fn read_client_state(state: &Arc<RwLock<ClientState>>) -> ClientState {
  let state_guard = state.read();
  state_guard.deref().clone()
}

pub fn future_error<T: 'static>(err: RedisError) -> Box<Future<Item=T, Error=RedisError>> {
  Box::new(future::err(err))
}

pub fn future_ok<T: 'static>(d: T) -> Box<Future<Item=T, Error=RedisError>> {
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

pub fn reset_reconnect_attempts(reconnect: &Rc<RefCell<Option<ReconnectPolicy>>>) {
  let mut reconnect_ref = reconnect.borrow_mut();

  if let Some(ref mut reconnect) = *reconnect_ref {
    reconnect.reset_attempts();
  }
}

pub fn u64_to_i64(u: u64) -> i64 {
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
  (lhs - rhs).abs() <= f64::EPSILON
}

pub fn to_url_string(host: &str, port: u16) -> String {
  format!("{}:{}", host, port)
}

pub fn read_closed_flag(closed: &Arc<RwLock<bool>>) -> bool {
  let closed_guard = closed.read();
  closed_guard.deref().clone()
}

pub fn set_closed_flag(closed: &Arc<RwLock<bool>>, flag: bool) {
  let mut closed_guard = closed.write();
  let mut closed_ref = closed_guard.deref_mut();
  *closed_ref = flag;
}

// called by connect() and connect_with_policy(), and verifies the client isn't already waiting to attempt a reconnect.
pub fn check_and_set_closed_flag(closed: &Arc<RwLock<bool>>, flag: bool) -> Result<(), RedisError> {
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

pub fn send_command(command_tx: &Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>, command: RedisCommand) -> Result<(), RedisError> {
  if command.kind == RedisCommandKind::Quit {
    let mut command_ref = command_tx.borrow_mut();

    let command_opt = command_ref.take();
    match command_opt {
      Some(tx) => {
        tx.unbounded_send(command).map_err(|e| {
          RedisError::new(RedisErrorKind::Unknown, format!("Error sending command: {}.", e))
        })
      },
      None => {
        Err(RedisError::new(
          RedisErrorKind::InvalidCommand, "Client is not connected."
        ))
      }
    }
  }else{
    let command_ref = command_tx.borrow();

    match *command_ref {
      Some(ref tx) => {
        tx.unbounded_send(command).map_err(|e| {
          RedisError::new(RedisErrorKind::Unknown, format!("Error sending command: {}.", e))
        })
      }
      None => {
        Err(RedisError::new(
          RedisErrorKind::InvalidCommand, "Client is not connected."
        ))
      }
    }
  }
}

pub fn request_response<F>(
  command_tx: &Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  state: &Arc<RwLock<ClientState>>, 
  func: F
) -> Box<Future<Item=Frame, Error=RedisError>>
  where F: FnOnce() -> Result<(RedisCommandKind, Vec<RedisValue>), RedisError>
{
  let _ = fry!(check_connected(state));
  let (kind, args) = fry!(func());

  let (tx, rx) = oneshot_channel();
  let command = RedisCommand::new(kind, args, Some(tx));

  let res = match send_command(command_tx, command) {
    Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
    Err(e) => future_error(e)
  };

  res
}

pub fn is_clustered(config: &Rc<RefCell<RedisConfig>>) -> bool {
  let config_guard = config.borrow();
  config_guard.deref().is_clustered()
}

pub fn split(command_tx: &Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>, config: &Rc<RefCell<RedisConfig>>, handle: &Handle)
  -> Box<Future<Item=Vec<(RedisClient, RedisConfig)>, Error=RedisError>>
{
  let timer = Timer::default();
  let timeout = Duration::from_millis(10 * 1000);

  let (tx, rx) = oneshot_channel();
  let split_command = RedisCommandKind::_Split(Some(SplitCommand {
    tx: Arc::new(RwLock::new(Some(tx))),
    key: multiplexer_utils::read_auth_key(config)
  }));
  let command = RedisCommand::new(split_command, vec![], None);

  if let Err(e) = send_command(command_tx, command) {
    return client_utils::future_error(e);
  }

  let uses_tls = config.borrow().tls();
  let handle = handle.clone();
  Box::new(rx.flatten().and_then(move |configs| {
    let all_len = configs.len();

    stream::iter_ok(configs.into_iter()).map(move |mut config| {
      // the underlying split() logic doesn't have the original tls flag, so it's copied above and restored here
      config.set_tls(uses_tls);

      let client = RedisClient::new(config.clone());
      let err_client = client.clone();
      let client_ft = client.connect(&handle).map(|_| ()).map_err(|_| ());

      trace!("Creating split clustered client...");
      handle.spawn(client_ft);

      let timer_ft = timer.sleep(timeout.clone()).map_err(|_| RedisError::new(
        RedisErrorKind::Unknown, "Split command timeout connecting."
      ))
      .map(|_| None);

      client.on_connect()
        .map(move |client| Some((client, config)))
        .select(timer_ft)
        .map_err(|(e, _)| e)
        .and_then(move |(opt, _)| match opt {
          Some((client, config)) => Ok((client, config)),
          None => Err(RedisError::new(
            RedisErrorKind::Unknown, "Split command timeout connecting."
          ))
        })
        .then(move |result| {
          match result {
            Ok(out) => client_utils::future_ok(out),
            Err(e) => Box::new(err_client.quit().then(move |_| Err(e)))
          }
        })
    })
    .buffer_unordered(all_len)
    .fold(Vec::with_capacity(all_len), |mut memo, (client, config)| {
      memo.push((client, config));
      Ok::<_, RedisError>(memo)
    })
  }))
}