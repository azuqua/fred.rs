#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use chrono;

use super::commands;

use futures::{
  Future,
  Stream
};

use futures::sync::oneshot::{
  Sender as OneshotSender,
  Receiver as OneshotReceiver,
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  UnboundedReceiver,
  UnboundedSender,
  unbounded
};

use std::rc::Rc;
use std::cell::RefCell;

use ::error::*;
use ::protocol::types::{
  RedisCommand,
  RedisCommandKind
};

use super::types::*;
use ::types::*;

// milliseconds since epoch
pub fn now() -> i64 {
  (chrono::Utc::now()).timestamp()
}

pub fn create_command_ft(rx: UnboundedReceiver<RedisCommand>) -> Box<Future<Item=(), Error=RedisError>> {
  let mut state = DataSet::default();

  Box::new(rx.from_err::<RedisError>().fold(state, |mut state, mut command: RedisCommand| {
    let tx = command.tx.take();

    let res = match command.kind {
      RedisCommandKind::Get    => commands::get(&mut state, command.args),
      RedisCommandKind::Set    => commands::set(&mut state, command.args),
      RedisCommandKind::Del    => commands::del(&mut state, command.args),
      RedisCommandKind::Expire => commands::expire(&mut state, command.args),
      RedisCommandKind::HGet   => commands::hget(&mut state, command.args),
      RedisCommandKind::HSet   => commands::hset(&mut state, command.args),
      RedisCommandKind::HDel   => commands::hdel(&mut state, command.args),

      RedisCommandKind::Quit => return Err(RedisError::new_canceled()),
      _ => commands::log_unimplemented()
    };

    if let Some(tx) = tx {
      let _ = tx.send(res);
    }

    Ok(state)
  })
  .map(|_| ()))
}


