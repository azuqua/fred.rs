use redis_protocol::types::Frame;

use crate::multiplexer::Multiplexer;
use crate::client::RedisClientInner;
use crate::error::*;
use crate::protocol::types::*;
use crate::types::*;
use crate::utils as client_utils;
use crate::protocol::utils as protocol_utils;
use crate::multiplexer::utils as multiplexer_utils;

use super::commands;

use std::sync::Arc;
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

use tokio_timer::Timer;
use std::time::Duration;

use std::ops::{
  Deref,
  DerefMut
};

use std::collections::{
  BTreeMap
};
use crate::mocks::types::*;
use parking_lot::RwLock;

lazy_static! {

  static ref DATA_SET: Arc<RwLock<BTreeMap<u8, Arc<RwLock<DataSet>>>>> = {
    Arc::new(RwLock::new(BTreeMap::new()))
  };

}

pub fn global_data_set() -> Arc<RwLock<BTreeMap<u8, Arc<RwLock<DataSet>>>>> {
  DATA_SET.clone()
}

pub fn global_data_set_db(db: u8) -> Arc<RwLock<DataSet>> {
  let data: Arc<RwLock<BTreeMap<u8, Arc<RwLock<DataSet>>>>> = DATA_SET.clone();
  let mut data_guard = data.write();
  let data_ref = data_guard.deref_mut();

  data_ref.entry(db)
    .or_insert(Arc::new(RwLock::new(DataSet::default())))
    .clone()
}


pub fn cleanup_keys(tx: &UnboundedSender<RedisCommand>, mut expired: Vec<Arc<ExpireLog>>) {
  for expire_log in expired.into_iter() {
    let mut expire_ref = match Arc::try_unwrap(expire_log) {
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
      tx: None,
      attempted: 0
    };

    let _ = tx.unbounded_send(command);
  }
}

pub fn clear_expirations(expirations: &Arc<RwLock<Expirations>>) {
  let mut expirations_guard = expirations.write();
  let mut expirations_ref = expirations_guard.deref_mut();

  expirations_ref.expirations.clear();
  expirations_ref.dirty.clear();
  expirations_ref.sorted.clear();
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

pub fn get_key(data: &DataSet, key: String) -> Arc<RedisKey> {
  let key: RedisKey = key.into();

  match data.keys.get(&key) {
    Some(k) => k.clone(),
    None => Arc::new(key.into())
  }
}

pub fn should_set(data: &DataSet, key: &Arc<RedisKey>, kind: SetOptions) -> bool {
  match kind {
    SetOptions::XX => data.keys.contains(key),
    SetOptions::NX => !data.keys.contains(key)
  }
}

pub fn handle_command(inner: &Arc<RedisClientInner>, data_ref: &Arc<RwLock<DataSet>>, command: RedisCommand) -> Result<Frame, RedisError> {
  if RedisCommandKind::FlushAll == command.kind {
    commands::flushall(command.args)
  }else {
    let mut data_guard = data_ref.write();
    let mut data = data_guard.deref_mut();

    match command.kind {
      RedisCommandKind::Get         => commands::get(data, command.args),
      RedisCommandKind::Set         => commands::set(data, command.args),
      RedisCommandKind::Del         => commands::del(data, command.args),
      RedisCommandKind::Expire      => commands::expire(data, command.args),
      RedisCommandKind::HGet        => commands::hget(data, command.args),
      RedisCommandKind::HSet        => commands::hset( data, command.args),
      RedisCommandKind::HDel        => commands::hdel(data, command.args),
      RedisCommandKind::HExists     => commands::hexists(data, command.args),
      RedisCommandKind::HGetAll     => commands::hgetall(data, command.args),
      RedisCommandKind::Select      => commands::select(data, command.args),
      RedisCommandKind::Auth        => commands::auth(data, command.args),
      RedisCommandKind::Incr        => commands::incr(data, command.args),
      RedisCommandKind::IncrBy      => commands::incrby(data, command.args),
      RedisCommandKind::Decr        => commands::decr(data, command.args),
      RedisCommandKind::DecrBy      => commands::decrby(data, command.args),
      RedisCommandKind::Ping        => commands::ping(data, command.args),
      RedisCommandKind::Info        => commands::info(data, command.args),
      RedisCommandKind::Persist     => commands::persist(data, command.args),
      RedisCommandKind::Sadd        => commands::sadd(data, command.args),
      RedisCommandKind::Smembers    => commands::smembers(data, command.args),
      RedisCommandKind::Publish     => commands::publish(data, command.args),
      RedisCommandKind::Subscribe   => commands::subscribe(data, command.args),

      _ => commands::log_unimplemented(&command)
    }
  }
}
