
use parking_lot::{
  RwLock,
};

use std::sync::Arc;
use std::ops::{Deref, DerefMut};

use futures::Future;
use futures::sync::mpsc::{
  UnboundedSender
};
use futures::sync::oneshot::{
  Sender as OneshotSender
};

use ::error::*;
use ::utils as client_utils;

use boxfnonce::SendBoxFnOnce;

use super::commands::{
  CommandFn,
  ConnectSender
};

use ::RedisClient;
use super::borrowed::RedisClientRemote as RedisClientBorrowed;
use super::owned::RedisClientRemote as RedisClientOwned;

pub fn send_normal_result<T>(tx: OneshotSender<Result<T, RedisError>>, result: Result<(RedisClient, T), RedisError>) -> Result<Option<RedisClient>, RedisError> {
  match result {
    Ok((client, count)) => {
      let _ = tx.send(Ok(count));
      Ok(Some(client))
    },
    Err(e) => {
      let _ = tx.send(Err(e));
      Ok(None)
    }
  }
}

pub fn send_command(tx: &Arc<RwLock<Option<UnboundedSender<CommandFn>>>>, func: CommandFn) -> Result<(), RedisError> {
  let tx_guard = tx.read();
  let tx_ref = tx_guard.deref();

  if let Some(ref tx) = *tx_ref {
    tx.unbounded_send(func).map_err(|_| RedisError::from(()))
  }else{
    Err(RedisError::new(
      RedisErrorKind::Unknown, "Remote client not initialized."
    ))
  }
}

pub fn run_borrowed<T, F>(client: RedisClientOwned, func: F) -> Box<Future<Item=T, Error=RedisError>>
  where T: 'static, F: FnOnce(RedisClientOwned, &RedisClientBorrowed) -> Box<Future<Item=T, Error=RedisError>>
{
  // not ideal having to clone an arc on each command...
  let inner_borrowed = client.inner_borrowed().clone();
  let borrowed_guard = inner_borrowed.read();
  let borrowed_opt = borrowed_guard.deref();

  let borrowed_ref = match *borrowed_opt {
    Some(ref b) => b,
    None => return client_utils::future_error(RedisError::new(
      RedisErrorKind::Unknown, "Remote redis client not initialized."
    ))
  };

  func(client, borrowed_ref)
}

pub fn register_connect_callbacks(
  command_tx: &Arc<RwLock<Option<UnboundedSender<CommandFn>>>>,
  connect_tx: &Arc<RwLock<Vec<ConnectSender>>>
)
{
  // not sure why the borrowchk says the collect expression borrows
  // `connect_tx_ref` longer than it appears to...
  let mut senders = {
    let mut connect_tx_guard = connect_tx.write();
    let mut connect_tx_refs = connect_tx_guard.deref_mut();

    let s: Vec<ConnectSender> = connect_tx_refs.drain(..).collect();
    s
  };

  for tx in senders.drain(..) {
    let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
      client.register_connect_callback(tx);
      client_utils::future_ok(Some(client))
    });

    let _ = send_command(command_tx, func);
  }
}