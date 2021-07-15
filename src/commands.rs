use crate::types::*;
use crate::protocol::types::{RedisCommandKind, ResponseKind, RedisCommand, ValueScanInner, KeyScanInner};

use futures::{Future, Stream, IntoFuture};
use crate::error::{
  RedisError,
  RedisErrorKind
};

use crate::utils;
use crate::protocol::utils as protocol_utils;
use crate::protocol::types::ValueScanResult;
use crate::client::{RedisClientInner};
use crate::client::RedisClient;
use crate::multiplexer::utils as multiplexer_utils;

use std::sync::Arc;
use std::ops::{
  Deref,
  DerefMut
};

use std::hash::Hash;
use std::collections::{HashMap, VecDeque};
use futures::sync::mpsc::unbounded;

use futures::future;
use crate::utils::redis_string_to_f64;

const ASYNC: &'static str = "ASYNC";
const MATCH: &'static str = "MATCH";
const COUNT: &'static str = "COUNT";
const TYPE: &'static str = "TYPE";
const CHANGED: &'static str = "CH";
const INCR: &'static str = "INCR";
const WITH_SCORES: &'static str = "WITHSCORES";
const LIMIT: &'static str = "LIMIT";
const AGGREGATE: &'static str = "AGGREGATE";
const WEIGHTS: &'static str = "WEIGHTS";


pub fn quit(inner: &Arc<RedisClientInner>) -> Box<Future<Item=(), Error=RedisError>> {
  debug!("{} Closing Redis connection with Quit command.", n!(inner));

  // need to lock the closed flag so any reconnect logic running in another thread doesn't screw this up,
  // but we also don't want to hold the lock if the client is connected
  let exit_early = {
    let mut closed_guard = inner.closed.write();
    let mut closed_ref = closed_guard.deref_mut();

    debug!("{} Checking client state in quit command: {:?}", n!(inner), utils::read_client_state(&inner.state));
    if utils::read_client_state(&inner.state) != ClientState::Connected {
      if *closed_ref {
        // client is already waiting to quit
        true
      }else{
        *closed_ref = true;

        true
      }
    }else{
      false
    }
  };

  // close anything left over from previous connections or reconnection attempts
  multiplexer_utils::close_error_tx(&inner.error_tx);
  multiplexer_utils::close_reconnect_tx(&inner.reconnect_tx);
  multiplexer_utils::close_messages_tx(&inner.message_tx);
  multiplexer_utils::close_connect_tx(&inner.connect_tx);

  if exit_early {
    debug!("{} Exit early in quit command.", n!(inner));
    utils::future_ok(())
  }else{
    Box::new(utils::request_response(&inner, || {
      Ok((RedisCommandKind::Quit, vec![]))
    }).and_then(|_| {
      Ok(())
    }))
  }
}

pub fn flushall(inner: &Arc<RedisClientInner>, _async: bool) -> Box<Future<Item=String, Error=RedisError>> {
  let args = if _async {
    vec![ASYNC.into()]
  }else{
    Vec::new()
  };

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::FlushAll, args))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid FLUSHALL response."
      ))
    }
  }))
}


pub fn get<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Get, vec![key.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    Ok(if resp.kind() == RedisValueKind::Null {
      None
    } else {
      Some(resp)
    })
  }))
}

pub fn set<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>> {
  let (key, value) = (key.into(), value.into());

  Box::new(utils::request_response(inner, move || {
    let mut args = vec![key.into(), value];

    if let Some(expire) = expire {
      let (k, v) = expire.into_args();
      args.push(k.into());
      args.push(v.into());
    }
    if let Some(options) = options {
      args.push(options.to_string().into());
    }

    Ok((RedisCommandKind::Set, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    Ok(resp.kind() != RedisValueKind::Null)
  }))
}

pub fn select(inner: &Arc<RedisClientInner>, db: u8) -> Box<Future<Item=(), Error=RedisError>> {
  debug!("{} Selecting Redis database {}", n!(inner), db);

  Box::new(utils::request_response(inner, || {
    Ok((RedisCommandKind::Select, vec![RedisValue::from(db)]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame) {
      Ok(_) => Ok(()),
      Err(e) => Err(e)
    }
  }))
}

pub fn info(inner: &Arc<RedisClientInner>, section: Option<InfoKind>) -> Box<Future<Item=String, Error=RedisError>> {
  let section = section.map(|k| k.to_str());

  Box::new(utils::request_response(inner, move || {
    let vec = match section {
      Some(s) => vec![RedisValue::from(s)],
      None => vec![]
    };

    Ok((RedisCommandKind::Info, vec))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame) {
      Ok(resp) => {
        let kind = resp.kind();

        match resp.into_string() {
          Some(s) => Ok(s),
          None => Err(RedisError::new(
            RedisErrorKind::Unknown, format!("Invalid INFO response. Expected String, found {:?}", kind)
          ))
        }
      },
      Err(e) => Err(e)
    }
  }))
}

pub fn del<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, keys: K) -> Box<Future<Item=usize, Error=RedisError>> {
  let mut keys = keys.into().inner();
  let args: Vec<RedisValue> = keys.drain(..).map(|k| {
    k.into()
  }).collect();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Del, args))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DEL response."
      ))
    }
  }))
}

pub fn subscribe<T: Into<String>>(inner: &Arc<RedisClientInner>, channel: T) -> Box<Future<Item=usize, Error=RedisError>> {
  // note: if this ever changes to take in more than one channel then some additional work must be done
  // in the multiplexer to associate multiple responses with a single request
  let channel = channel.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Subscribe, vec![channel.into()]))
  }).and_then(|frame| {
    let mut results = protocol_utils::frame_to_results(frame)?;

    // last value in the array is number of channels
    let count = match results.pop() {
      Some(c) => match c.as_u64() {
        Some(i) => i,
        None => return Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid SUBSCRIBE channel count response."
        ))
      },
      None => return Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid SUBSCRIBE response."
      ))
    };

    Ok(count as usize)
  }))
}

pub fn unsubscribe<T: Into<String>>(inner: &Arc<RedisClientInner>, channel: T) -> Box<Future<Item=usize, Error=RedisError>> {
  // note: if this ever changes to take in more than one channel then some additional work must be done
  // in the multiplexer to associate multiple responses with a single request
  let channel = channel.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Unsubscribe, vec![channel.into()]))
  }).and_then(|frame| {
    let mut results = protocol_utils::frame_to_results(frame)?;

    // last value in the array is number of channels
    let count = match results.pop() {
      Some(c) => match c.as_u64() {
        Some(i) => i,
        None => return Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid UNSUBSCRIBE channel count response."
        ))
      },
      None => return Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid UNSUBSCRIBE response."
      ))
    };

    Ok(count as usize)
  }))
}

pub fn publish<T: Into<String>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, channel: T, message: V) -> Box<Future<Item=i64, Error=RedisError>> {
  let channel = channel.into();
  let message = message.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    let count = match resp.as_i64() {
      Some(c) => c,
      None => return Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid PUBLISH response."
      ))
    };

    Ok(count)
  }))
}


pub fn incr<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=i64, Error=RedisError>>  {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Incr, vec![key.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid INCR response."
      ))
    }
  }))
}

pub fn incrby<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, incr: i64) -> Box<Future<Item=i64, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::IncrBy, vec![key.into(), incr.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(i) => Ok(i as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid INCRBY response."
      ))
    }
  }))
}

pub fn incrbyfloat<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, incr: f64) -> Box<Future<Item=f64, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::IncrByFloat, vec![key.into(), incr.to_string().into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => match s.parse::<f64>() {
        Ok(f) => Ok(f),
        Err(e) => Err(e.into())
      },
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid INCRBYFLOAT response."
      ))
    }
  }))
}

pub fn decr<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Decr, vec![key.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DECR response."
      ))
    }
  }))
}

pub fn decrby<V: Into<RedisValue>, K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Box<Future<Item=i64, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let args = vec![key.into(), value.into()];

    Ok((RedisCommandKind::DecrBy, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DECRBY response."
      ))
    }
  }))
}

pub fn ping(inner: &Arc<RedisClientInner>) -> Box<Future<Item=String, Error=RedisError>> {
  let inner = inner.clone();
  debug!("{} Pinging Redis server.", n!(inner));

  Box::new(utils::request_response(&inner, move || {
    Ok((RedisCommandKind::Ping, vec![]))
  }).and_then(move |frame| {
    debug!("{} Received Redis ping response.", n!(inner));

    match protocol_utils::frame_to_single_result(frame) {
      Ok(resp) => {
        let kind = resp.kind();

        match resp.into_string() {
          Some(s) => Ok(s),
          None => Err(RedisError::new(
            RedisErrorKind::Unknown, format!("Invalid PING response. Expected String, found {:?}", kind)
          ))
        }
      },
      Err(e) => Err(e)
    }
  }))
}

pub fn auth<V: Into<String>>(inner: &Arc<RedisClientInner>, value: V) -> Box<Future<Item=String, Error=RedisError>> {
  let value = value.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Auth, vec![value.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::Auth, "AUTH denied."
      ))
    }
  }))
}

pub fn bgrewriteaof(inner: &Arc<RedisClientInner>) -> Box<Future<Item=String, Error=RedisError>> {
  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::BgreWriteAof, vec![]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid BGREWRITEAOF response."
      ))
    }
  }))
}

pub fn bgsave(inner: &Arc<RedisClientInner>) -> Box<Future<Item=String, Error=RedisError>> {
  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::BgSave, vec![]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid BGSAVE response."
      ))
    }
  }))
}

pub fn client_list(inner: &Arc<RedisClientInner>) -> Box<Future<Item=String, Error=RedisError>> {
  Box::new(utils::request_response(inner, move || {
    let args = vec![];

    Ok((RedisCommandKind::ClientList, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid CLIENTLIST response."
      ))
    }
  }))
}

pub fn client_getname(inner: &Arc<RedisClientInner>) -> Box<Future<Item=Option<String>, Error=RedisError>> {
  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClientGetName, vec![]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(Some(s)),
      None => Ok(None)
    }
  }))
}

pub fn client_setname<V: Into<String>>(inner: &Arc<RedisClientInner>, name: V) -> Box<Future<Item=Option<String>, Error=RedisError>> {
  let name = name.into();
  inner.change_client_name(name.clone());

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClientSetname, vec![name.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(Some(s)),
      None => Ok(None)
    }
  }))
}

pub fn dbsize(inner: &Arc<RedisClientInner>) -> Box<Future<Item=usize, Error=RedisError>> {
  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::DBSize, vec![]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DBSIZE response."
      ))
    }
  }))
}

pub fn dump<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=Option<String>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Dump, vec![key.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(Some(s)),
      RedisValue::Null => Ok(None),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DUMP response."
      ))
    }
  }))
}

pub fn exists<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, keys: K) -> Box<Future<Item=usize, Error=RedisError>> {
  let mut keys = keys.into().inner();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = keys.drain(..).map(|k| k.into()).collect();

    Ok((RedisCommandKind::Exists, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid EXISTS response."
      ))
    }
  }))
}

pub fn expire<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, seconds: i64) -> Box<Future<Item=bool, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Expire, vec![
      key.into(),
      seconds.into()
    ]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => match num {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid EXPIRE response value."
        ))
      },
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid EXPIRE response."
      ))
    }
  }))
}

pub fn expire_at<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, timestamp: i64) -> Box<Future<Item=bool, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let args = vec![key.into(), timestamp.into()];

    Ok((RedisCommandKind::ExpireAt, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => match num {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid EXPIREAT response value."
        ))
      },
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid EXPIREAT response."
      ))
    }
  }))
}

pub fn persist<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=bool, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move ||{
    Ok((RedisCommandKind::Persist,vec![key.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => match num {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid PERSIST response value."
        ))
      },
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid PERSIST response."
      ))
    }
  }))
}

pub fn flushdb(inner: &Arc<RedisClientInner>, _async: bool) -> Box<Future<Item=String, Error=RedisError>> {
  let args = if _async {
    vec![ASYNC.into()]
  }else{
    Vec::new()
  };

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::FlushDB, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid FLUSHALLDB response."
      ))
    }
  }))
}

pub fn getrange<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, start: usize, end: usize) -> Box<Future<Item=String, Error=RedisError>> {
  let key = key.into();
  let start = fry!(RedisValue::from_usize(start));
  let end = fry!(RedisValue::from_usize(end));

  let args = vec![
    key.into(),
    start,
    end
  ];

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::GetRange, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid GETRANGE response."
      ))
    }
  }))
}

pub fn getset<V: Into<RedisValue>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, value: V) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
  let (key, value) = (key.into(), value.into());

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), value.into()];

    Ok((RedisCommandKind::GetSet, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Null => Ok(None),
      _ => Ok(Some(resp))
    }
  }))
}

pub fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, fields: F) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let mut fields = fields.into().inner();

  Box::new(utils::request_response(inner, move || {
    let mut args: Vec<RedisValue> = Vec::with_capacity(fields.len() + 1);
    args.push(key.into());

    for field in fields.drain(..) {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HDel, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid HDEL response."
      ))
    }
  }))
}

pub fn hexists<F: Into<RedisKey>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F) -> Box<Future<Item=bool, Error=RedisError>> {
  let key = key.into();
  let field = field.into();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];

    Ok((RedisCommandKind::HExists, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => match num {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid HEXISTS response value."
        ))
      },
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid HEXISTS response."
      ))
    }
  }))
}

pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
  let key = key.into();
  let field = field.into();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];

    Ok((RedisCommandKind::HGet, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Null => Ok(None),
      _ => Ok(Some(resp))
    }
  }))
}

pub fn hgetall<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=HashMap<String, RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into()];

    Ok((RedisCommandKind::HGetAll, args))
  }).and_then(|frame| {
    let mut resp = protocol_utils::frame_to_results(frame)?;

    let mut map: HashMap<String, RedisValue> = HashMap::with_capacity(resp.len() / 2);

    for mut chunk in resp.chunks_mut(2) {
      let (key, val) = (chunk[0].take(), chunk[1].take());
      let key = match key {
        RedisValue::String(s) => s,
        _ => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid HGETALL response."
        ))
      };

      map.insert(key, val);
    }

    Ok(map)
  }))
}

pub fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F, incr: i64) -> Box<Future<Item=i64, Error=RedisError>> {
  let (key, field) = (key.into(), field.into());

  let args: Vec<RedisValue> = vec![
    key.into(),
    field.into(),
    incr.into()
  ];

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HIncrBy, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid HINCRBY response."
      ))
    }
  }))
}

pub fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F, incr: f64) -> Box<Future<Item=f64, Error=RedisError>> {
  let (key, field) = (key.into(), field.into());

  let args = vec![
    key.into(),
    field.into(),
    incr.to_string().into()
  ];

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HIncrByFloat, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => match s.parse::<f64>() {
        Ok(f) => Ok(f),
        Err(e) => Err(RedisError::new(
          RedisErrorKind::Unknown, format!("Invalid HINCRBYFLOAT response: {:?}", e)
        ))
      },
      _ => Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid HINCRBYFLOAT response."
      ))
    }
  }))
}

pub fn hkeys<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=Vec<String>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HKeys, vec![key.into()]))
  }).and_then(|frame| {
    let mut resp = protocol_utils::frame_to_results(frame)?;

    let mut out = Vec::with_capacity(resp.len());
    for val in resp.drain(..) {
      let s = match val {
        RedisValue::Null => "nil".to_owned(),
        RedisValue::String(s) => s,
        _ => return Err(RedisError::new(
          RedisErrorKind::Unknown, "Invalid HKEYS response."
        ))
      };

      out.push(s);
    }

    Ok(out)
  }))
}

pub fn hlen<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HLen, vec![key.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid HLEN response."
      ))
    }
  }))
}

pub fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, fields: F) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();
  let mut fields = fields.into().inner();

  let mut args = Vec::with_capacity(fields.len() + 1);
  args.push(key.into());

  for field in fields.drain(..) {
    args.push(field.into());
  }

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HMGet, args))
  }).and_then(|frame| {
    Ok(protocol_utils::frame_to_results(frame)?)
  }))
}

pub fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, mut values: HashMap<F, V>) -> Box<Future<Item=String, Error=RedisError>> {
  let key = key.into();

  let mut args = Vec::with_capacity(values.len() * 2 + 1);
  args.push(key.into());

  for (field, value) in values.drain() {
    let field = field.into();
    args.push(field.into());
    args.push(value.into());
  }

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HMSet, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid HMSET response."
      ))
    }
  }))
}

pub fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (inner: &Arc<RedisClientInner>, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let field = field.into();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into(), value.into()];

    Ok((RedisCommandKind::HSet, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    let res = match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid HSET response."
      ))
    };

    res
  }))
}

pub fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (inner: &Arc<RedisClientInner>, key: K, field: F, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
  let (key, field, value) = (key.into(), field.into(), value.into());

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into(), value];

    Ok((RedisCommandKind::HSetNx, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid HSETNX response."
      ))
    }
  }))
}

pub fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F) -> Box<Future<Item=usize, Error=RedisError>> {
  let (key, field) = (key.into(), field.into());

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];

    Ok((RedisCommandKind::HStrLen, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid HSTRLEN response."
      ))
    }
  }))
}

pub fn hvals<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::HVals, vec![key.into()]))
  }).and_then(|frame| {
    Ok(protocol_utils::frame_to_results(frame)?)
  }))
}

pub fn llen<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::LLen, vec![key.into()]))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid LLEN response."
      ))
    }
  }))
}

pub fn lpush<K: Into<RedisKey>, V: Into<RedisValue>> (inner: &Arc<RedisClientInner>, key: K, value: V) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let value = value.into();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), value.into()];

    Ok((RedisCommandKind::LPush, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid LPUSH response."
      ))
    }
  }))
}

pub fn lpop<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into()];

    Ok((RedisCommandKind::LPop, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    let resp = if resp.kind() == RedisValueKind::Null {
      None
    } else {
      Some(resp)
    };

    Ok(resp)
  }))
}

pub fn memoryusage<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, samples: Option<i64>) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = vec![key.into()];
    if let Some(num_samples) = samples {
      let mut samples_vec = vec!["SAMPLES".into(), num_samples.into()];
      args.append(&mut samples_vec);
    };

    Ok((RedisCommandKind::MemoryUsage, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid MEMORY USAGE response."
      ))
    }
  }))
}

pub fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let value = values.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + value.len());
    args.push(key.into());

    for value in value.inner().into_iter() {
      args.push(value);
    }

    Ok((RedisCommandKind::Sadd, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid SADD response."
      ))
    }
  }))
}

pub fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let value = values.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + value.len());
    args.push(key.into());

    for value in value.inner().into_iter() {
      args.push(value);
    }

    Ok((RedisCommandKind::Srem, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid SREM response."
      ))
    }
  }))
}

pub fn smembers<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Smembers, vec![key.into()]))
  }).and_then(|frame| {
    Ok(protocol_utils::frame_to_results(frame)?)
  }))
}

pub fn psubscribe<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, patterns: K) -> Box<Future<Item=Vec<usize>, Error=RedisError>> {
  let patterns = patterns.into().inner();

  Box::new(utils::request_response(inner, move || {
    let mut keys = Vec::with_capacity(patterns.len());

    for pattern in patterns.into_iter() {
      keys.push(pattern.into());
    }

    let kind = RedisCommandKind::Psubscribe(ResponseKind::Multiple {
      count: keys.len(),
      buffer: VecDeque::new()
    });

    Ok((kind, keys))
  }).and_then(|frame| {
    let result = protocol_utils::frame_to_results(frame)?;
    utils::pattern_pubsub_counts(result)
  }))
}

pub fn punsubscribe<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, patterns: K) -> Box<Future<Item=Vec<usize>, Error=RedisError>> {
  let patterns = patterns.into().inner();

  Box::new(utils::request_response(inner, move || {
    let mut keys = Vec::with_capacity(patterns.len());

    for pattern in patterns.into_iter() {
      keys.push(pattern.into());
    }

    let kind = RedisCommandKind::Punsubscribe(ResponseKind::Multiple {
      count: keys.len(),
      buffer: VecDeque::new()
    });

    Ok((kind, keys))
  }).and_then(|frame| {
    let result = protocol_utils::frame_to_results(frame)?;
    utils::pattern_pubsub_counts(result)
  }))
}

pub fn scan<P: Into<String>>(inner: &Arc<RedisClientInner>, pattern: Option<P>, count: Option<usize>, _type: Option<ScanType>) -> Box<Stream<Item=ScanResult, Error=RedisError>> {
  let pattern = pattern.map(|s| s.into());
  let (tx, rx) = unbounded();
  let cursor = "0".to_owned();

  let count = if let Some(count) = count {
    match RedisValue::from_usize(count) {
      Ok(d) => Some(d),
      Err(e) => return Box::new(future::err(e).into_stream())
    }
  }else{
    None
  };

  let key_slot = if let Some(ref pattern) = pattern {
    if multiplexer_utils::clustered_scan_pattern_has_hash_tag(pattern) {
      Some(redis_protocol::redis_keyslot(pattern))
    }else{
      None
    }
  }else{
    None
  };

  let mut args = Vec::with_capacity(7);
  args.push(cursor.clone().into());

  if let Some(pattern) = pattern {
    args.push(MATCH.into());
    args.push(pattern.into());
  }
  if let Some(count) = count {
    args.push(COUNT.into());
    args.push(count);
  }
  if let Some(_type) = _type {
    args.push(TYPE.into());
    args.push(_type.to_str().into());
  }
  let scan = KeyScanInner { key_slot, cursor, tx };

  let cmd = RedisCommand {
    kind: RedisCommandKind::Scan(scan),
    args: args,
    attempted: 0,
    tx: None
  };

  if let Err(e) = utils::send_command(inner, cmd) {
    return Box::new(future::err(e).into_stream());
  }

  Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()))
}

pub fn hscan<K: Into<RedisKey>, P: Into<String>>(inner: &Arc<RedisClientInner>, key: K, pattern: Option<P>, count: Option<usize>) -> Box<Stream<Item=HScanResult, Error=RedisError>> {
  let pattern = pattern.map(|s| s.into());
  let (tx, rx) = unbounded();
  let cursor = "0".to_owned();
  let key = key.into();

  let count = if let Some(count) = count {
    match RedisValue::from_usize(count) {
      Ok(d) => Some(d),
      Err(e) => return Box::new(future::err(e).into_stream())
    }
  }else{
    None
  };

  let mut args = Vec::with_capacity(6);
  args.push(key.into());
  args.push(cursor.clone().into());

  if let Some(pattern) = pattern {
    args.push(MATCH.into());
    args.push(pattern.into());
  }
  if let Some(count) = count {
    args.push(COUNT.into());
    args.push(count);
  }
  let scan = ValueScanInner { cursor, tx };

  let cmd = RedisCommand {
    kind: RedisCommandKind::Hscan(scan),
    args: args,
    attempted: 0,
    tx: None
  };

  if let Err(e) = utils::send_command(inner, cmd) {
    return Box::new(future::err(e).into_stream());
  }

  Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()).filter_map(|result| {
    match result {
      ValueScanResult::HScan(res) => Some(res),
      _ => {
        warn!("Invalid scan result for hscan");
        None
      }
    }
  }))
}

pub fn sscan<K: Into<RedisKey>, P: Into<String>>(inner: &Arc<RedisClientInner>, key: K, pattern: Option<P>, count: Option<usize>) -> Box<Stream<Item=SScanResult, Error=RedisError>> {
  let pattern = pattern.map(|s| s.into());
  let (tx, rx) = unbounded();
  let cursor = "0".to_owned();
  let key = key.into();

  let count = if let Some(count) = count {
    match RedisValue::from_usize(count) {
      Ok(d) => Some(d),
      Err(e) => return Box::new(future::err(e).into_stream())
    }
  }else{
    None
  };

  let mut args = Vec::with_capacity(6);
  args.push(key.into());
  args.push(cursor.clone().into());

  if let Some(pattern) = pattern {
    args.push(MATCH.into());
    args.push(pattern.into());
  }
  if let Some(count) = count {
    args.push(COUNT.into());
    args.push(count);
  }
  let scan = ValueScanInner { cursor, tx };

  let cmd = RedisCommand {
    kind: RedisCommandKind::Sscan(scan),
    args: args,
    attempted: 0,
    tx: None
  };

  if let Err(e) = utils::send_command(inner, cmd) {
    return Box::new(future::err(e).into_stream());
  }

  Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()).filter_map(|result| {
    match result {
      ValueScanResult::SScan(res) => Some(res),
      _ => {
        warn!("Invalid scan result for sscan");
        None
      }
    }
  }))
}

pub fn zscan<K: Into<RedisKey>, P: Into<String>>(inner: &Arc<RedisClientInner>, key: K, pattern: Option<P>, count: Option<usize>) -> Box<Stream<Item=ZScanResult, Error=RedisError>> {
  let pattern = pattern.map(|s| s.into());
  let (tx, rx) = unbounded();
  let cursor = "0".to_owned();
  let key = key.into();

  let count = if let Some(count) = count {
    match RedisValue::from_usize(count) {
      Ok(d) => Some(d),
      Err(e) => return Box::new(future::err(e).into_stream())
    }
  }else{
    None
  };

  let mut args = Vec::with_capacity(6);
  args.push(key.into());
  args.push(cursor.clone().into());

  if let Some(pattern) = pattern {
    args.push(MATCH.into());
    args.push(pattern.into());
  }
  if let Some(count) = count {
    args.push(COUNT.into());
    args.push(count);
  }
  let scan = ValueScanInner { cursor, tx };

  let cmd = RedisCommand {
    kind: RedisCommandKind::Zscan(scan),
    args: args,
    attempted: 0,
    tx: None
  };

  if let Err(e) = utils::send_command(inner, cmd) {
    return Box::new(future::err(e).into_stream());
  }

  Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()).filter_map(|result| {
    match result {
      ValueScanResult::ZScan(res) => Some(res),
      _ => {
        warn!("Invalid scan result for zscan");
        None
      }
    }
  }))
}

pub fn mget<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, keys: K) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let keys = keys.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }

    Ok((RedisCommandKind::Mget, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zadd<K: Into<RedisKey>, V: Into<MultipleZaddValues>>(inner: &Arc<RedisClientInner>, key: K, options: Option<SetOptions>, changed: bool, incr: bool, values: V) -> Box<Future<Item=RedisValue, Error=RedisError>> {
  let key = key.into();
  let values = values.into().inner();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4 + (values.len() * 2));
    args.push(key.into());

    if let Some(opts) = options {
      args.push(opts.to_string().into());
    }
    if changed {
      args.push(CHANGED.into());
    }
    if incr {
      args.push(INCR.into());
    }

    for (score, value) in values.into_iter() {
      args.push(utils::f64_to_redis_string(score)?);
      args.push(value.into());
    }

    Ok((RedisCommandKind::Zadd, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_single_result(frame)
  }))
}

pub fn zcard<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zcard, vec![key.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid negative ZCARD response."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZCARD response."))
    }
  }))
}

pub fn zcount<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let min = utils::f64_to_redis_string(min)?;
    let max = utils::f64_to_redis_string(max)?;

    Ok((RedisCommandKind::Zcount, vec![key.into(), min, max]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid negative ZCOUNT response."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZCOUNT response."))
    }
  }))
}

pub fn zlexcount<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, min: M, max: N) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let min = min.into();
  let max = max.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zlexcount, vec![key.into(), min.into(), max.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid negative ZLEXCOUNT response."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZLEXCOUNT response."))
    }
  }))
}

pub fn zincrby<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, incr: f64, value: V) -> Box<Future<Item=f64, Error=RedisError>> {
  let key = key.into();
  let value = value.into();

  Box::new(utils::request_response(inner, move || {
    let incr = utils::f64_to_redis_string(incr)?;

    Ok((RedisCommandKind::Zincrby, vec![key.into(), incr, value]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::String(s) => utils::redis_string_to_f64(&s),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZINCRBY response."))
    }
  }))
}

pub fn zrange<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64, with_scores: bool) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = vec![key.into(), start.into(), stop.into()];

    if with_scores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zrange, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, min: M, max: N, limit: Option<(usize, usize)>) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();
  let min = min.into();
  let max = max.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = vec![key.into(), min.into(), max.into()];

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(RedisValue::from_usize(offset)?);
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zrangebylex, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zrangebyscore<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key.into());

    let min = utils::f64_to_redis_string(min)?;
    let max = utils::f64_to_redis_string(max)?;
    args.push(min);
    args.push(max);

    if with_scores {
      args.push(WITH_SCORES.into());
    }

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(RedisValue::from_usize(offset)?);
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zrangebyscore, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zpopmax<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zpopmax, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zpopmin<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zpopmin, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zrank<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Box<Future<Item=RedisValue, Error=RedisError>> {
  let key = key.into();
  let value = value.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zrank, vec![key.into(), value.into()]))
  }).and_then(|frame| {
    protocol_utils::frame_to_single_result(frame)
  }))
}

pub fn zrem<K: Into<RedisKey>, V: Into<MultipleValues>>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let values = values.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + values.len());
    args.push(key.into());

    for value in values.inner().into_iter() {
      args.push(value.into());
    }

    Ok((RedisCommandKind::Zrem, args))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREM response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREM response. Expected integer."))
    }
  }))
}

pub fn zremrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, min: M, max: N) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();
  let min = min.into();
  let max = max.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zremrangebylex, vec![key.into(), min.into(), max.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYLEX response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYLEX response. Expected integer."))
    }
  }))
}

pub fn zremrangebyrank<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zremrangebyrank, vec![key.into(), start.into(), stop.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYRANK response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYRANK response. Expected integer."))
    }
  }))
}

pub fn zremrangebyscore<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64) -> Box<Future<Item=usize, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = vec![
      key.into(),
      utils::f64_to_redis_string(min)?,
      utils::f64_to_redis_string(max)?
    ];

    Ok((RedisCommandKind::Zremrangebyscore, args))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYSCORE response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYSCORE response. Expected integer."))
    }
  }))
}

pub fn zrevrange<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64, with_scores: bool) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(start.into());
    args.push(stop.into());

    if with_scores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zrevrange, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zrevrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, max: M, min: N, limit: Option<(usize, usize)>) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();
  let max = max.into();
  let min = min.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    args.push(max.into());
    args.push(min.into());

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(RedisValue::from_usize(offset)?);
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zrevrangebylex, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zrevrangebyscore<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, max: f64, min: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Box<Future<Item=Vec<RedisValue>, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key.into());

    args.push(utils::f64_to_redis_string(max)?);
    args.push(utils::f64_to_redis_string(min)?);

    if with_scores {
      args.push(WITH_SCORES.into());
    }

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(RedisValue::from_usize(offset)?);
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zrevrangebyscore, args))
  }).and_then(|frame| {
    protocol_utils::frame_to_results(frame)
  }))
}

pub fn zrevrank<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Box<Future<Item=RedisValue, Error=RedisError>> {
  let key = key.into();
  let value = value.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zrevrank, vec![key.into(), value.into()]))
  }).and_then(|frame| {
    protocol_utils::frame_to_single_result(frame)
  }))
}

pub fn zscore<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Box<Future<Item=RedisValue, Error=RedisError>> {
  let key = key.into();
  let value = value.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zscore, vec![key.into(), value.into()]))
  }).and_then(|frame| {
    protocol_utils::frame_to_single_result(frame)
  }))
}

pub fn zinterstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(inner: &Arc<RedisClientInner>,
                                                                                       destination: D,
                                                                                       keys: K,
                                                                                       weights: W,
                                                                                       aggregate: Option<AggregateOptions>)
  -> Box<Future<Item=usize, Error=RedisError>>
{
  let destination = destination.into();
  let keys = keys.into();
  let weights = weights.into();

  Box::new(utils::request_response(inner, move || {
    if keys.len() == 0 {
      return Err(RedisError::new(RedisErrorKind::InvalidArgument, "ZINTERSTORE numkeys cannot be 0."));
    }

    let mut args = Vec::with_capacity(5 + keys.len() + weights.len());
    args.push(destination.into());
    args.push(RedisValue::from_usize(keys.len())?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(WEIGHTS.into());

      for weight in weights.inner().into_iter() {
        args.push(utils::f64_to_redis_string(weight)?);
      }
    }

    if let Some(aggregate) = aggregate {
      args.push(AGGREGATE.into());
      args.push(aggregate.to_str().into());
    }

    Ok((RedisCommandKind::Zinterstore, args))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZINTERSTORE response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZINTERSTORE response. Expected integer."))
    }
  }))
}

pub fn zunionstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(inner: &Arc<RedisClientInner>,
                                                                                       destination: D,
                                                                                       keys: K,
                                                                                       weights: W,
                                                                                       aggregate: Option<AggregateOptions>)
  -> Box<Future<Item=usize, Error=RedisError>>
{
  let destination = destination.into();
  let keys = keys.into();
  let weights = weights.into();

  Box::new(utils::request_response(inner, move || {
    if keys.len() == 0 {
      return Err(RedisError::new(RedisErrorKind::InvalidArgument, "ZUNIONSTORE numkeys cannot be 0."));
    }

    let mut args = Vec::with_capacity(5 + keys.len() + weights.len());
    args.push(destination.into());
    args.push(RedisValue::from_usize(keys.len())?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(WEIGHTS.into());

      for weight in weights.inner().into_iter() {
        args.push(utils::f64_to_redis_string(weight)?);
      }
    }

    if let Some(aggregate) = aggregate {
      args.push(AGGREGATE.into());
      args.push(aggregate.to_str().into());
    }

    Ok((RedisCommandKind::Zunionstore, args))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZUNIONSTORE response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZUNIONSTORE response. Expected integer."))
    }
  }))
}

pub fn ttl<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Ttl, vec![key.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => Ok(i),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid TTL response. Expected integer."))
    }
  }))
}

pub fn pttl<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Box<Future<Item=i64, Error=RedisError>> {
  let key = key.into();

  Box::new(utils::request_response(inner, move || {
    Ok((RedisCommandKind::Pttl, vec![key.into()]))
  }).and_then(|frame| {
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => Ok(i),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid PTTL response. Expected integer."))
    }
  }))
}

