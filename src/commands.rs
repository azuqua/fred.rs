use crate::types::*;
use crate::protocol::types::RedisCommandKind;

use futures::{
  Future,
  Stream
};
use crate::error::{
  RedisError,
  RedisErrorKind
};

use crate::utils;
use crate::protocol::utils as protocol_utils;
use crate::client::{RedisClientInner};
use crate::client::RedisClient;
use crate::multiplexer::utils as multiplexer_utils;

use std::sync::Arc;
use std::ops::{
  Deref,
  DerefMut
};

const ASYNC: &'static str = "ASYNC";

pub fn quit(inner: &Arc<RedisClientInner>) -> Box<Future<Item=(), Error=RedisError>> {
  debug!("Closing Redis connection with Quit command.");

  // need to lock the closed flag so any reconnect logic running in another thread doesn't screw this up,
  // but we also don't want to hold the lock if the client is connected
  let exit_early = {
    let mut closed_guard = inner.closed.write();
    let mut closed_ref = closed_guard.deref_mut();

    debug!("Checking client state in quit command: {:?}", utils::read_client_state(&inner.state));
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
    debug!("Exit early in quit command.");
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
  debug!("Selecting Redis database {}", db);

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
  // in the multiplexer to associate mutliple responses with a single request
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

