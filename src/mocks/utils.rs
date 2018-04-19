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

use tokio_timer::Timer;
use std::time::Duration;

use std::ops::{
  Deref,
  DerefMut
};

use std::collections::{
  BTreeMap
};

fn cleanup_keys(tx: &UnboundedSender<RedisCommand>, mut expired: Vec<Rc<ExpireLog>>) {
  for expire_log in expired.into_iter() {
    let mut expire_ref = match Rc::try_unwrap(expire_log) {
      Ok(r) => r,
      Err(_) => {
        trace!("Error unwraping Rc!");
        continue;
      }
    };

    let (_, key) = match expire_ref.internal.take() {
      Some(inner) => inner,
      None => continue
    };

    let command = RedisCommand {
      kind: RedisCommandKind::Del,
      args: vec![key.as_ref().to_owned().into()],
      tx: None
    };

    let _ = tx.unbounded_send(command);
  }
}

pub fn clear_expirations(expirations: &Rc<RefCell<Expirations>>) {
  let mut expirations_ref = expirations.borrow_mut();

  expirations_ref.expirations.clear();
  expirations_ref.dirty.clear();
  expirations_ref.sorted.clear();
}

// milliseconds since epoch
pub fn now() -> i64 {
  (chrono::Utc::now()).timestamp()
}

pub fn create_command_ft(rx: UnboundedReceiver<RedisCommand>, tx: UnboundedSender<RedisCommand>) -> Box<Future<Item=(), Error=RedisError>> {
  let mut state = DataSet::default();
  let expirations = state.expirations.clone();

  let command_ft = rx.from_err::<RedisError>().fold(state, |mut state, mut command: RedisCommand| {
    let tx = command.tx.take();

    let res = match command.kind {
      RedisCommandKind::Get    => commands::get(&mut state, command.args),
      RedisCommandKind::Set    => commands::set(&mut state, command.args),
      RedisCommandKind::Del    => commands::del(&mut state, command.args),
      RedisCommandKind::Expire => commands::expire(&mut state, command.args),
      RedisCommandKind::HGet   => commands::hget(&mut state, command.args),
      RedisCommandKind::HSet   => commands::hset(&mut state, command.args),
      RedisCommandKind::HDel   => commands::hdel(&mut state, command.args),
      RedisCommandKind::Select => commands::select(&mut state, command.args),
      RedisCommandKind::Auth   => commands::auth(&mut state, command.args),

      RedisCommandKind::FlushAll => commands::flushall(&mut state, command.args),
      RedisCommandKind::Quit => return Err(RedisError::new_canceled()),
      _ => commands::log_unimplemented()
    };

    if let Some(tx) = tx {
      let _ = tx.send(res);
    }

    Ok(state)
  })
  .map(|_| ());

  let timer = Timer::default();
  let dur = Duration::from_millis(1000);

  let timer_ft = timer.interval(dur).from_err::<RedisError>().for_each(move |_| {
    let expired = {
      let mut expiration_ref = expirations.borrow_mut();

      let expired = expiration_ref.find_expired();
      expiration_ref.cleanup();

      expired
    };

    trace!("Cleaning up mock {} expired keys", expired.len());
    cleanup_keys(&tx, expired);

    Ok(())
  });

  Box::new(command_ft.select(timer_ft).map(|_| ()).map_err(|(e, _)| e))
}


