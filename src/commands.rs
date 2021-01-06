use crate::types::*;
use crate::protocol::types::{RedisCommandKind, ResponseKind, RedisCommand, ValueScanInner, KeyScanInner};

// use futures::{Future, Stream, IntoFuture};
use futures::{Future, Stream, FutureExt, TryFutureExt, StreamExt};
use futures::future::IntoFuture;
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

use std::pin::Pin;
use std::sync::Arc;
use std::ops::{
  Deref,
  DerefMut
};

use std::hash::Hash;
use std::collections::{HashMap, VecDeque};
use futures::channel::mpsc::unbounded;

use futures::future;

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


pub async fn quit(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  debug!("{} Closing Redis connection with Quit command.", n!(inner));

  // need to lock the closed flag so any reconnect logic running in another thread doesn't screw this up,
  // but we also don't want to hold the lock if the client is connected
  let exit_early = {
    let mut closed_guard = inner.closed.write();
    let closed_ref = closed_guard.deref_mut();

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
    return Ok(())
  }else{
    utils::request_response(&inner, || {
      Ok((RedisCommandKind::Quit, vec![]))
    }).await?;
    Ok(())
  }
}

pub async fn flushall(inner: &Arc<RedisClientInner>, _async: bool) -> Result<String, RedisError> {
  let args = if _async {
    vec![ASYNC.into()]
  }else{
    Vec::new()
  };

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::FlushAll, args))
  }).await?;

  match protocol_utils::frame_to_single_result(frame)? {
    RedisValue::String(s) => Ok(s),
    _ => Err(RedisError::new(
      RedisErrorKind::ProtocolError, "Invalid FLUSHALL response."
    ))
  }
}

/*
pub fn get(inner: &Arc<RedisClientInner>, key: RedisKey) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send>> {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Get, vec![key.into()]))
  }).await?;

  let resp = protocol_utils::frame_to_single_result(frame)?;

  Ok(if resp.kind() == RedisValueKind::Null {
    None
  } else {
    Some(resp)
  })
}*/

pub fn get<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Pin<Box<dyn Future<Output=Result<Option<RedisValue>, RedisError>> + Send + '_>> {
  let key = key.into();
  Box::pin(utils::request_response_ft(inner, RedisCommandKind::Get, vec![key.into()])
    .and_then(|frame| {
      futures::future::ready(protocol_utils::frame_to_single_result(frame))
    })
    .map_ok(|resp| {
      if resp.kind() == RedisValueKind::Null { None } else { Some(resp) }
    })
  )
}

pub async fn set<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) ->Result<bool, RedisError> {
  let (key, value) = (key.into(), value.into());

  let frame = utils::request_response(inner, move || {
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
  }).await?;

  let resp = protocol_utils::frame_to_single_result(frame)?;

  Ok(resp.kind() != RedisValueKind::Null)
}

pub async fn select(inner: &Arc<RedisClientInner>, db: u8) -> Result<(), RedisError> {
  debug!("{} Selecting Redis database {}", n!(inner), db);

  let frame = utils::request_response(inner, || {
    Ok((RedisCommandKind::Select, vec![RedisValue::from(db)]))
  }).await?;

  match protocol_utils::frame_to_single_result(frame) {
    Ok(_) => Ok(()),
    Err(e) => Err(e)
  }
}

pub async fn info(inner: &Arc<RedisClientInner>, section: Option<InfoKind>) -> Result<String, RedisError> {
  let section = section.map(|k| k.to_str());

  let frame = utils::request_response(inner, move || {
    let vec = match section {
      Some(s) => vec![RedisValue::from(s)],
      None => vec![]
    };

    Ok((RedisCommandKind::Info, vec))
  }).await?;

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
}

pub async fn del<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, keys: K) -> Result<usize, RedisError>  {
  let mut keys = keys.into().inner();
  let args: Vec<RedisValue> = keys.drain(..).map(|k| {
    k.into()
  }).collect();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Del, args))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DEL response."
      ))
    }
}

pub async fn subscribe<T: Into<String>>(inner: &Arc<RedisClientInner>, channel: T) -> Result<usize, RedisError>  {
  // note: if this ever changes to take in more than one channel then some additional work must be done
  // in the multiplexer to associate multiple responses with a single request
  let channel = channel.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Subscribe, vec![channel.into()]))
  }).await?;
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
}

pub async fn unsubscribe<T: Into<String>>(inner: &Arc<RedisClientInner>, channel: T) -> Result<usize, RedisError>  {
  // note: if this ever changes to take in more than one channel then some additional work must be done
  // in the multiplexer to associate multiple responses with a single request
  let channel = channel.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Unsubscribe, vec![channel.into()]))
  }).await?;
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
}

pub async fn publish<T: Into<String>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, channel: T, message: V) -> Result<i64, RedisError>  {
  let channel = channel.into();
  let message = message.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    let count = match resp.as_i64() {
      Some(c) => c,
      None => return Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid PUBLISH response."
      ))
    };

    Ok(count)
}


pub async fn incr<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<i64, RedisError>   {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Incr, vec![key.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid INCR response."
      ))
    }
}

pub async fn incrby<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, incr: i64) -> Result<i64, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::IncrBy, vec![key.into(), incr.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(i) => Ok(i as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid INCRBY response."
      ))
    }
}

pub async fn incrbyfloat<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, incr: f64) -> Result<f64, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::IncrByFloat, vec![key.into(), incr.to_string().into()]))
  }).await?;
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
}

pub async fn decr<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<i64, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Decr, vec![key.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DECR response."
      ))
    }
}

pub async fn decrby<V: Into<RedisValue>, K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Result<i64, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let args = vec![key.into(), value.into()];

    Ok((RedisCommandKind::DecrBy, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DECRBY response."
      ))
    }
}

pub async fn ping(inner: &Arc<RedisClientInner>) -> Result<String, RedisError>  {
  let inner = inner.clone();
  debug!("{} Pinging Redis server.", n!(inner));

  let frame = utils::request_response(&inner, move || {
    Ok((RedisCommandKind::Ping, vec![]))
  }).await?;
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
}

pub async fn auth<V: Into<String>>(inner: &Arc<RedisClientInner>, value: V) -> Result<String, RedisError>  {
  let value = value.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Auth, vec![value.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::Auth, "AUTH denied."
      ))
    }
}

pub async fn bgrewriteaof(inner: &Arc<RedisClientInner>) -> Result<String, RedisError>  {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::BgreWriteAof, vec![]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid BGREWRITEAOF response."
      ))
    }
}

pub async fn bgsave(inner: &Arc<RedisClientInner>) -> Result<String, RedisError>  {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::BgSave, vec![]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid BGSAVE response."
      ))
    }
}

pub async fn client_list(inner: &Arc<RedisClientInner>) -> Result<String, RedisError>  {
  let frame = utils::request_response(inner, move || {
    let args = vec![];

    Ok((RedisCommandKind::ClientList, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(s),
      None => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid CLIENTLIST response."
      ))
    }
}

pub async fn client_getname(inner: &Arc<RedisClientInner>) -> Result<Option<String>, RedisError>  {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClientGetName, vec![]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(Some(s)),
      None => Ok(None)
    }
}

pub async fn client_setname<V: Into<String>>(inner: &Arc<RedisClientInner>, name: V) -> Result<Option<String>, RedisError>  {
  let name = name.into();
  inner.change_client_name(name.clone());

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClientSetname, vec![name.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp.into_string() {
      Some(s) => Ok(Some(s)),
      None => Ok(None)
    }
}

pub async fn dbsize(inner: &Arc<RedisClientInner>) -> Result<usize, RedisError>  {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::DBSize, vec![]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DBSIZE response."
      ))
    }
}

pub async fn dump<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<Option<String>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Dump, vec![key.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(Some(s)),
      RedisValue::Null => Ok(None),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid DUMP response."
      ))
    }
}

pub async fn exists<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, keys: K) -> Result<usize, RedisError>  {
  let mut keys = keys.into().inner();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = keys.drain(..).map(|k| k.into()).collect();

    Ok((RedisCommandKind::Exists, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid EXISTS response."
      ))
    }
}

pub async fn expire<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, seconds: i64) -> Result<bool, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Expire, vec![
      key.into(),
      seconds.into()
    ]))
  }).await?;
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
}

pub async fn expire_at<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, timestamp: i64) -> Result<bool, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let args = vec![key.into(), timestamp.into()];

    Ok((RedisCommandKind::ExpireAt, args))
  }).await?;
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
}

pub async fn persist<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<bool, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move ||{
    Ok((RedisCommandKind::Persist,vec![key.into()]))
  }).await?;
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
}

pub async fn flushdb(inner: &Arc<RedisClientInner>, _async: bool) -> Result<String, RedisError>  {
  let args = if _async {
    vec![ASYNC.into()]
  }else{
    Vec::new()
  };

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::FlushDB, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid FLUSHALLDB response."
      ))
    }
}

pub async fn getrange<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, start: usize, end: usize) -> Result<String, RedisError>  {
  let key = key.into();
  let start = RedisValue::from_usize(start)?;
  let end = RedisValue::from_usize(end)?;

  let args = vec![
    key.into(),
    start,
    end
  ];

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::GetRange, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid GETRANGE response."
      ))
    }
}

pub async fn getset<V: Into<RedisValue>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, value: V) -> Result<Option<RedisValue>, RedisError>  {
  let (key, value) = (key.into(), value.into());

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), value.into()];

    Ok((RedisCommandKind::GetSet, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Null => Ok(None),
      _ => Ok(Some(resp))
    }
}

pub async fn hdel<F: Into<MultipleKeys>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, fields: F) -> Result<usize, RedisError>  {
  let key = key.into();
  let mut fields = fields.into().inner();

  let frame = utils::request_response(inner, move || {
    let mut args: Vec<RedisValue> = Vec::with_capacity(fields.len() + 1);
    args.push(key.into());

    for field in fields.drain(..) {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HDel, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid HDEL response."
      ))
    }
}

pub async fn hexists<F: Into<RedisKey>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F) -> Result<bool, RedisError>  {
  let key = key.into();
  let field = field.into();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];

    Ok((RedisCommandKind::HExists, args))
  }).await?;
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
}

pub async fn hget<F: Into<RedisKey>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F) -> Result<Option<RedisValue>, RedisError>  {
  let key = key.into();
  let field = field.into();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];

    Ok((RedisCommandKind::HGet, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Null => Ok(None),
      _ => Ok(Some(resp))
    }
}

pub async fn hgetall<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<HashMap<String, RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into()];

    Ok((RedisCommandKind::HGetAll, args))
  }).await?;
    let mut resp = protocol_utils::frame_to_results(frame)?;

    let mut map: HashMap<String, RedisValue> = HashMap::with_capacity(resp.len() / 2);

    for chunk in resp.chunks_mut(2) {
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
}

pub async fn hincrby<F: Into<RedisKey>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F, incr: i64) -> Result<i64, RedisError>  {
  let (key, field) = (key.into(), field.into());

  let args: Vec<RedisValue> = vec![
    key.into(),
    field.into(),
    incr.into()
  ];

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HIncrBy, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as i64),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid HINCRBY response."
      ))
    }
}

pub async fn hincrbyfloat<K: Into<RedisKey>, F: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F, incr: f64) -> Result<f64, RedisError>  {
  let (key, field) = (key.into(), field.into());

  let args = vec![
    key.into(),
    field.into(),
    incr.to_string().into()
  ];

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HIncrByFloat, args))
  }).await?;
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
}

pub async fn hkeys<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<Vec<String>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HKeys, vec![key.into()]))
  }).await?;
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
}

pub async fn hlen<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<usize, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HLen, vec![key.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid HLEN response."
      ))
    }
}

pub async fn hmget<F: Into<MultipleKeys>, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, fields: F) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();
  let mut fields = fields.into().inner();

  let mut args = Vec::with_capacity(fields.len() + 1);
  args.push(key.into());

  for field in fields.drain(..) {
    args.push(field.into());
  }

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HMGet, args))
  }).await?;
    Ok(protocol_utils::frame_to_results(frame)?)
}

pub async fn hmset<V: Into<RedisValue>, F: Into<RedisKey> + Hash + Eq, K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, mut values: HashMap<F, V>) -> Result<String, RedisError>  {
  let key = key.into();

  let mut args = Vec::with_capacity(values.len() * 2 + 1);
  args.push(key.into());

  for (field, value) in values.drain() {
    let field = field.into();
    args.push(field.into());
    args.push(value.into());
  }

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HMSet, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::String(s) => Ok(s),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid HMSET response."
      ))
    }
}

pub async fn hset<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (inner: &Arc<RedisClientInner>, key: K, field: F, value: V) -> Result<usize, RedisError>  {
  let key = key.into();
  let field = field.into();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into(), value.into()];

    Ok((RedisCommandKind::HSet, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    let res = match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid HSET response."
      ))
    };

    res
}

pub async fn hsetnx<K: Into<RedisKey>, F: Into<RedisKey>, V: Into<RedisValue>> (inner: &Arc<RedisClientInner>, key: K, field: F, value: V) -> Result<usize, RedisError>  {
  let (key, field, value) = (key.into(), field.into(), value.into());

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into(), value];

    Ok((RedisCommandKind::HSetNx, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid HSETNX response."
      ))
    }
}

pub async fn hstrlen<K: Into<RedisKey>, F: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K, field: F) -> Result<usize, RedisError>  {
  let (key, field) = (key.into(), field.into());

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];

    Ok((RedisCommandKind::HStrLen, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid HSTRLEN response."
      ))
    }
}

pub async fn hvals<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HVals, vec![key.into()]))
  }).await?;
    Ok(protocol_utils::frame_to_results(frame)?)
}

pub async fn llen<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<usize, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::LLen, vec![key.into()]))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid LLEN response."
      ))
    }
}

pub async fn lpush<K: Into<RedisKey>, V: Into<RedisValue>> (inner: &Arc<RedisClientInner>, key: K, value: V) -> Result<usize, RedisError>  {
  let key = key.into();
  let value = value.into();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into(), value.into()];

    Ok((RedisCommandKind::LPush, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid LPUSH response."
      ))
    }
}

pub async fn lpop<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<Option<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let args: Vec<RedisValue> = vec![key.into()];

    Ok((RedisCommandKind::LPop, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    let resp = if resp.kind() == RedisValueKind::Null {
      None
    } else {
      Some(resp)
    };

    Ok(resp)
}

pub async fn sadd<K: Into<RedisKey>, V: Into<MultipleValues>>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Result<usize, RedisError>  {
  let key = key.into();
  let value = values.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + value.len());
    args.push(key.into());

    for value in value.inner().into_iter() {
      args.push(value);
    }

    Ok((RedisCommandKind::Sadd, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid SADD response."
      ))
    }
}

pub async fn srem<K: Into<RedisKey>, V: Into<MultipleValues>>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Result<usize, RedisError>  {
  let key = key.into();
  let value = values.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + value.len());
    args.push(key.into());

    for value in value.inner().into_iter() {
      args.push(value);
    }

    Ok((RedisCommandKind::Srem, args))
  }).await?;
    let resp = protocol_utils::frame_to_single_result(frame)?;

    match resp {
      RedisValue::Integer(num) => Ok(num as usize),
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown , "Invalid SREM response."
      ))
    }
}

pub async fn smembers<K: Into<RedisKey>> (inner: &Arc<RedisClientInner>, key: K) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Smembers, vec![key.into()]))
  }).await?;
    Ok(protocol_utils::frame_to_results(frame)?)
}

pub async fn psubscribe<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, patterns: K) -> Result<Vec<usize>, RedisError>  {
  let patterns = patterns.into().inner();

  let frame = utils::request_response(inner, move || {
    let mut keys = Vec::with_capacity(patterns.len());

    for pattern in patterns.into_iter() {
      keys.push(pattern.into());
    }

    let kind = RedisCommandKind::Psubscribe(ResponseKind::Multiple {
      count: keys.len(),
      buffer: VecDeque::new()
    });

    Ok((kind, keys))
  }).await?;
    let result = protocol_utils::frame_to_results(frame)?;
    utils::pattern_pubsub_counts(result)
}

pub async fn punsubscribe<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, patterns: K) -> Result<Vec<usize>, RedisError>  {
  let patterns = patterns.into().inner();

  let frame = utils::request_response(inner, move || {
    let mut keys = Vec::with_capacity(patterns.len());

    for pattern in patterns.into_iter() {
      keys.push(pattern.into());
    }

    let kind = RedisCommandKind::Punsubscribe(ResponseKind::Multiple {
      count: keys.len(),
      buffer: VecDeque::new()
    });

    Ok((kind, keys))
  }).await?;
    let result = protocol_utils::frame_to_results(frame)?;
    utils::pattern_pubsub_counts(result)
}

pub fn scan<P: Into<String>>(inner: &Arc<RedisClientInner>, pattern: Option<P>, count: Option<usize>, _type: Option<ScanType>) -> Box<dyn Stream<Item=Result<ScanResult, RedisError>>> {
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
  let scan = KeyScanInner { cursor, tx };

  let cmd = RedisCommand {
    kind: RedisCommandKind::Scan(scan),
    args: args,
    attempted: 0,
    tx: None
  };

  if let Err(e) = utils::send_command(inner, cmd) {
    return Box::new(future::err(e).into_stream());
  }

  //Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()))
  Box::new(rx)
}

pub fn hscan<K: Into<RedisKey>, P: Into<String>>(inner: &Arc<RedisClientInner>, key: K, pattern: Option<P>, count: Option<usize>) -> Box<dyn Stream<Item=Result<HScanResult, RedisError>>> {
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

  //Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()).filter_map(|result| {
  Box::new(rx.filter_map(|result| {
    let result = match result {
      //ValueScanResult::HScan(res) => Some(res),
      Ok(ValueScanResult::HScan(res)) => Some(Ok(res)), // FIXME: check semantics of error handling here, need to bail on first error?
      Ok(_) => {
        warn!("Invalid scan result for hscan");
        None
      },
      Err(e) => Some(Err(e)),
    };
    futures::future::ready(result)
  }))
}

pub fn sscan<K: Into<RedisKey>, P: Into<String>>(inner: &Arc<RedisClientInner>, key: K, pattern: Option<P>, count: Option<usize>) -> Box<dyn Stream<Item=Result<SScanResult, RedisError>>> {
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

  //Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()).filter_map(|result| {
  Box::new(rx.filter_map(|result| {
    let result = match result {
      Ok(ValueScanResult::SScan(res)) => Some(Ok(res)),
      Ok(_) => {
        warn!("Invalid scan result for sscan");
        None
      },
      Err(e) => Some(Err(e)),
    };
    futures::future::ready(result)
  }))
}

pub fn zscan<K: Into<RedisKey>, P: Into<String>>(inner: &Arc<RedisClientInner>, key: K, pattern: Option<P>, count: Option<usize>) -> Box<dyn Stream<Item=Result<ZScanResult, RedisError>>> {
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

  //Box::new(rx.from_err::<RedisError>().and_then(|res| res.into_future()).filter_map(|result| {
  Box::new(rx.filter_map(|result| {
    let result = match result {
      Ok(ValueScanResult::ZScan(res)) => Some(Ok(res)),
      Ok(_) => {
        warn!("Invalid scan result for zscan");
        None
      },
      Err(e) => Some(Err(e))
    };
    futures::future::ready(result)
  }))
}

pub async fn mget<K: Into<MultipleKeys>>(inner: &Arc<RedisClientInner>, keys: K) -> Result<Vec<RedisValue>, RedisError>  {
  let keys = keys.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }

    Ok((RedisCommandKind::Mget, args))
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zadd<K: Into<RedisKey>, V: Into<MultipleZaddValues>>(inner: &Arc<RedisClientInner>, key: K, options: Option<SetOptions>, changed: bool, incr: bool, values: V) -> Result<RedisValue, RedisError>  {
  let key = key.into();
  let values = values.into().inner();

  let frame = utils::request_response(inner, move || {
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
  }).await?;
    protocol_utils::frame_to_single_result(frame)
}

pub async fn zcard<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<usize, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zcard, vec![key.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid negative ZCARD response."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZCARD response."))
    }
}

pub async fn zcount<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64) -> Result<usize, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let min = utils::f64_to_redis_string(min)?;
    let max = utils::f64_to_redis_string(max)?;

    Ok((RedisCommandKind::Zcount, vec![key.into(), min, max]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid negative ZCOUNT response."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZCOUNT response."))
    }
}

pub async fn zlexcount<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, min: M, max: N) -> Result<usize, RedisError>  {
  let key = key.into();
  let min = min.into();
  let max = max.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zlexcount, vec![key.into(), min.into(), max.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid negative ZLEXCOUNT response."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZLEXCOUNT response."))
    }
}

pub async fn zincrby<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, incr: f64, value: V) -> Result<f64, RedisError>  {
  let key = key.into();
  let value = value.into();

  let frame = utils::request_response(inner, move || {
    let incr = utils::f64_to_redis_string(incr)?;

    Ok((RedisCommandKind::Zincrby, vec![key.into(), incr, value]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::String(s) => utils::redis_string_to_f64(&s),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZINCRBY response."))
    }
}

pub async fn zrange<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64, with_scores: bool) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = vec![key.into(), start.into(), stop.into()];

    if with_scores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zrange, args))
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, min: M, max: N, limit: Option<(usize, usize)>) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();
  let min = min.into();
  let max = max.into();

  let frame = utils::request_response(inner, move || {
    let mut args = vec![key.into(), min.into(), max.into()];

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(RedisValue::from_usize(offset)?);
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zrangebylex, args))
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zrangebyscore<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
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
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zpopmax<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zpopmax, args))
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zpopmin<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(RedisValue::from_usize(count)?);
    }

    Ok((RedisCommandKind::Zpopmin, args))
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zrank<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Result<RedisValue, RedisError>  {
  let key = key.into();
  let value = value.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zrank, vec![key.into(), value.into()]))
  }).await?;
    protocol_utils::frame_to_single_result(frame)
}

pub async fn zrem<K: Into<RedisKey>, V: Into<MultipleValues>>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Result<usize, RedisError>  {
  let key = key.into();
  let values = values.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + values.len());
    args.push(key.into());

    for value in values.inner().into_iter() {
      args.push(value.into());
    }

    Ok((RedisCommandKind::Zrem, args))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREM response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREM response. Expected integer."))
    }
}

pub async fn zremrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, min: M, max: N) -> Result<usize, RedisError>  {
  let key = key.into();
  let min = min.into();
  let max = max.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zremrangebylex, vec![key.into(), min.into(), max.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYLEX response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYLEX response. Expected integer."))
    }
}

pub async fn zremrangebyrank<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64) -> Result<usize, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zremrangebyrank, vec![key.into(), start.into(), stop.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYRANK response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYRANK response. Expected integer."))
    }
}

pub async fn zremrangebyscore<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64) -> Result<usize, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let args = vec![
      key.into(),
      utils::f64_to_redis_string(min)?,
      utils::f64_to_redis_string(max)?
    ];

    Ok((RedisCommandKind::Zremrangebyscore, args))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYSCORE response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZREMRANGEBYSCORE response. Expected integer."))
    }
}

pub async fn zrevrange<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64, with_scores: bool) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(start.into());
    args.push(stop.into());

    if with_scores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zrevrange, args))
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zrevrangebylex<K: Into<RedisKey>, M: Into<String>, N: Into<String>>(inner: &Arc<RedisClientInner>, key: K, max: M, min: N, limit: Option<(usize, usize)>) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();
  let max = max.into();
  let min = min.into();

  let frame = utils::request_response(inner, move || {
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
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zrevrangebyscore<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K, max: f64, min: f64, with_scores: bool, limit: Option<(usize, usize)>) -> Result<Vec<RedisValue>, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
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
  }).await?;
    protocol_utils::frame_to_results(frame)
}

pub async fn zrevrank<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Result<RedisValue, RedisError>  {
  let key = key.into();
  let value = value.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zrevrank, vec![key.into(), value.into()]))
  }).await?;
    protocol_utils::frame_to_single_result(frame)
}

pub async fn zscore<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V) -> Result<RedisValue, RedisError>  {
  let key = key.into();
  let value = value.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Zscore, vec![key.into(), value.into()]))
  }).await?;
    protocol_utils::frame_to_single_result(frame)
}

pub async fn zinterstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(inner: &Arc<RedisClientInner>,
                                                                                       destination: D,
                                                                                       keys: K,
                                                                                       weights: W,
                                                                                       aggregate: Option<AggregateOptions>)
  -> Result<usize, RedisError>
{
  let destination = destination.into();
  let keys = keys.into();
  let weights = weights.into();

  let frame = utils::request_response(inner, move || {
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
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZINTERSTORE response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZINTERSTORE response. Expected integer."))
    }
}

pub async fn zunionstore<D: Into<RedisKey>, K: Into<MultipleKeys>, W: Into<MultipleWeights>>(inner: &Arc<RedisClientInner>,
                                                                                       destination: D,
                                                                                       keys: K,
                                                                                       weights: W,
                                                                                       aggregate: Option<AggregateOptions>)
  -> Result<usize, RedisError>
{
  let destination = destination.into();
  let keys = keys.into();
  let weights = weights.into();

  let frame = utils::request_response(inner, move || {
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
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZUNIONSTORE response. Expected non-negative integer."))
      }else{
        Ok(i as usize)
      },
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid ZUNIONSTORE response. Expected integer."))
    }
}

pub async fn ttl<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<i64, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Ttl, vec![key.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => Ok(i),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid TTL response. Expected integer."))
    }
}

pub async fn pttl<K: Into<RedisKey>>(inner: &Arc<RedisClientInner>, key: K) -> Result<i64, RedisError>  {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Pttl, vec![key.into()]))
  }).await?;
    match protocol_utils::frame_to_single_result(frame)? {
      RedisValue::Integer(i) => Ok(i),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid PTTL response. Expected integer."))
    }
}
