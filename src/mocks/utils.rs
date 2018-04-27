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
  RedisCommandKind,
  Frame,
  SplitCommand
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
  let time = chrono::Utc::now();
  time.timestamp() * 1000 + (time.timestamp_subsec_millis() as i64)
}

pub fn ok() -> Result<Frame, RedisError> {
  Ok(Frame::SimpleString("OK".into()))
}

pub fn null() -> Result<Frame, RedisError> {
  Ok(Frame::Null)
}

pub fn to_int(s: &str) -> Result<i64, RedisError> {
  s.parse::<i64>().map_err(|e| {
    RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid argument, expected number."
    )
  })
}

pub fn get_key(data: &DataSet, key: String) -> Rc<RedisKey> {
  let key: RedisKey = key.into();

  match data.keys.get(&key) {
    Some(k) => k.clone(),
    None => Rc::new(key.into())
  }
}

pub fn should_set(data: &DataSet, key: &Rc<RedisKey>, kind: SetOptions) -> bool {
  match kind {
    SetOptions::XX => data.keys.contains(key),
    SetOptions::NX => !data.keys.contains(key)
  }
}

pub fn handle_split(options: Option<SplitCommand>) -> Result<Frame, RedisError> {
  let tx = match options {
    Some(mut o) => match o.take() {
      (Some(tx), _) => tx,
      _ => return Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid split command options."
      ))
    },
    None => return Err(RedisError::new(
      RedisErrorKind::Unknown, "Invalid split command options."
    ))
  };

  let _ = tx.send(Ok(vec![RedisConfig::default()]));
  null()
}

pub fn create_command_ft(rx: UnboundedReceiver<RedisCommand>, tx: UnboundedSender<RedisCommand>) -> Box<Future<Item=(), Error=RedisError>> {
  let mut state = DataSet::default();
  let expirations = state.expirations.clone();

  let command_ft = rx.from_err::<RedisError>().fold(state, |mut state, mut command: RedisCommand| {
    let tx = command.tx.take();

    trace!("Processing mock redis command: {:?}", command.kind);

    let res = match command.kind {
      RedisCommandKind::Get         => commands::get(&mut state, command.args),
      RedisCommandKind::Set         => commands::set(&mut state, command.args),
      RedisCommandKind::Del         => commands::del(&mut state, command.args),
      RedisCommandKind::Expire      => commands::expire(&mut state, command.args),
      RedisCommandKind::HGet        => commands::hget(&mut state, command.args),
      RedisCommandKind::HSet        => commands::hset(&mut state, command.args),
      RedisCommandKind::HDel        => commands::hdel(&mut state, command.args),
      RedisCommandKind::HExists     => commands::hexists(&mut state, command.args),
      RedisCommandKind::HGetAll     => commands::hgetall(&mut state, command.args),
      RedisCommandKind::Select      => commands::select(&mut state, command.args),
      RedisCommandKind::Auth        => commands::auth(&mut state, command.args),
      RedisCommandKind::Incr        => commands::incr(&mut state, command.args),
      RedisCommandKind::IncrBy      => commands::incrby(&mut state, command.args),
      RedisCommandKind::Decr        => commands::decr(&mut state, command.args),
      RedisCommandKind::DecrBy      => commands::decrby(&mut state, command.args),
      RedisCommandKind::Ping        => commands::ping(&mut state, command.args),
      RedisCommandKind::Info        => commands::info(&mut state, command.args),
      RedisCommandKind::Publish     => commands::publish(&mut state, command.args),
      RedisCommandKind::Subscribe   => commands::subscribe(&mut state, command.args),
      RedisCommandKind::Unsubscribe => commands::unsubscribe(&mut state, command.args),

      RedisCommandKind::_Split(mut split) => handle_split(split.take()),
      RedisCommandKind::FlushAll => commands::flushall(&mut state, command.args),
      RedisCommandKind::Quit => {
        if let Some(tx) = tx {
          let _ = tx.send(null());
        }

        return Err(RedisError::new_canceled())
      },
      _ => commands::log_unimplemented()
    };

    if let Some(tx) = tx {
      let _ = tx.send(res);
    }

    Ok(state)
  })
  .then(|result| {
    match result {
      Ok(_) => Ok(()),
      Err(e) => match *e.kind() {
        RedisErrorKind::Canceled => Ok(()),
        _ => Err(e)
      }
    }
  });

  let timer = Timer::default();
  let dur = Duration::from_millis(1000);

  let timer_ft = timer.interval(dur).from_err::<RedisError>().for_each(move |_| {
    trace!("Starting to scan for expired keys.");

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

  trace!("Creating mock redis command stream...");

  Box::new(command_ft.select(timer_ft).map(|_| ()).map_err(|(e, _)| e))
}


