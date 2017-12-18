
use parking_lot::{
  RwLock,
};

use std::collections::VecDeque;
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
use ::types::RedisValue;
use super::borrowed::RedisClientRemote as RedisClientBorrowed;
use super::owned::RedisClientRemote as RedisClientOwned;

use std::rc::Rc;
use std::cell::RefCell;

pub fn transfer_senders<T>(src: &Arc<RwLock<VecDeque<T>>>, dest: &Arc<RwLock<VecDeque<T>>>) {
  flame_start!("redis:transfer_senders");

  let mut tmp: VecDeque<T> = {
    let mut src_guard = src.write();
    let mut src_ref = src_guard.deref_mut();
    let tmp = src_ref.drain(..).collect();

    tmp
  };

  let mut dest_guard = dest.write();
  let mut dest_ref = dest_guard.deref_mut();

  for tx in tmp.drain(..) {
    dest_ref.push_back(tx);
  }
  flame_end!("redis:transfer_senders");
}

pub fn send_normal_result<T>(tx: OneshotSender<Result<T, RedisError>>, result: Result<(RedisClient, T), RedisError>) -> Result<Option<RedisClient>, RedisError> {
  flame_start!("redis:send_normal_result");
  let res = match result {
    Ok((client, val)) => {
      let _ = tx.send(Ok(val));
      Ok(Some(client))
    },
    Err(e) => {
      let _ = tx.send(Err(e));
      Ok(None)
    }
  };

  flame_end!("redis:send_normal_result");
  res
}

pub fn send_empty_result(tx: OneshotSender<Result<(), RedisError>>, result: Result<RedisClient, RedisError>) -> Result<Option<RedisClient>, RedisError> {
  flame_start!("redis:send_empty_result");
  let res = match result {
    Ok(client) => {
      let _ = tx.send(Ok(()));
      Ok(Some(client))
    },
    Err(e) => {
      let _ = tx.send(Err(e));
      Ok(None)
    }
  };

  flame_end!("redis:send_empty_result");
  res
}

pub fn send_command(tx: &Arc<RwLock<Option<UnboundedSender<CommandFn>>>>, func: CommandFn) -> Result<(), RedisError> {
  flame_start!("redis:sync_send_command");
  let tx_guard = tx.read();
  let tx_ref = tx_guard.deref();

  let res = if let Some(ref tx) = *tx_ref {
    tx.unbounded_send(func).map_err(|_| RedisError::from(()))
  }else{
    Err(RedisError::new(
      RedisErrorKind::Unknown, "Remote client not initialized."
    ))
  };

  flame_end!("redis:sync_send_command");
  res
}

pub fn run_borrowed<T, F>(client: RedisClientOwned, func: F) -> Box<Future<Item=T, Error=RedisError>>
  where T: 'static, F: FnOnce(RedisClientOwned, &RedisClientBorrowed) -> Box<Future<Item=T, Error=RedisError>>
{
  flame_start!("redis:run_borrowed");

  // not ideal having to clone an arc on each command...
  let inner_borrowed = client.inner_borrowed().clone();
  let borrowed_guard = inner_borrowed.read();
  let borrowed_opt = borrowed_guard.deref();

  let borrowed_ref = match *borrowed_opt {
    Some(ref b) => b,
    None => {
      flame_end!("redis:run_borrowed");
      return client_utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, "Remote redis client not initialized."
      ))
    }
  };

  let res = func(client, borrowed_ref);
  flame_end!("redis:run_borrowed");
  res
}

pub fn transfer_sender<T>(client_tx: Rc<RefCell<VecDeque<T>>>, tx: T) {
  flame_start!("redis:transfer_sender");

  let mut client_error_tx_ref = client_tx.borrow_mut();
  let res = client_error_tx_ref.push_back(tx);

  flame_end!("redis:transfer_sender");
  res
}

pub fn register_callbacks(
  command_tx: &Arc<RwLock<Option<UnboundedSender<CommandFn>>>>,
  connect_tx: &Arc<RwLock<VecDeque<ConnectSender>>>,
  error_tx: &Arc<RwLock<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: &Arc<RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>>
)
{
  {
    // transfer on_connect senders
    let mut connect_tx_guard = connect_tx.write();
    let mut connect_tx_ref = connect_tx_guard.deref_mut();

    for tx in connect_tx_ref.drain(..) {
      let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
        client.register_connect_callback(tx);
        client_utils::future_ok(Some(client))
      });

      let _ = send_command(command_tx, func);
    }
  }
  {
    // transfer on_error senders
    let mut error_tx_guard = error_tx.write();
    let mut error_tx_ref = error_tx_guard.deref_mut();

    for tx in error_tx_ref.drain(..) {
      let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
        transfer_sender(client.errors_cloned(), tx);
        client_utils::future_ok(Some(client))
      });

      let _ = send_command(command_tx, func);
    }
  }
  {
    // transfer on_message senders
    let mut message_tx_guard = message_tx.write();
    let mut message_tx_ref = message_tx_guard.deref_mut();

    for tx in message_tx_ref.drain(..) {
      let func: CommandFn = SendBoxFnOnce::from(move |client: RedisClient| {
        transfer_sender(client.messages_cloned(), tx);
        client_utils::future_ok(Some(client))
      });

      let _ = send_command(command_tx, func);
    }
  }
}