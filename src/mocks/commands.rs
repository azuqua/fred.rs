#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use super::utils;
use ::utils as client_utils;

use super::types::*;

use ::error::*;
use ::types::*;

use ::protocol::types::{
  RedisCommand,
  RedisCommandKind,
  Frame
};

pub fn log_unimplemented() -> Result<Frame, RedisError> {
  warn!("Mock redis function not implemented.");
  Ok(Frame::Null)
}

pub fn auth(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn select(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn set(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn get(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Ok(Frame::Null)
  };

  let val = match data.data.get(&key) {
    Some(RedisValue::String(s)) => Frame::BulkString(s),
    Some(RedisValue::Integer(i)) => Frame::Integer(i),
    Some(RedisValue::Null) | None => Frame::Null,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  Ok(val)
}

pub fn del(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn expire(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn hget(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn hset(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn hdel(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn flushall(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  data.data.clear();
  data.maps.clear();
  data.sets.clear();
  data.key_types.clear();
  data.keys.clear();
  utils::clear_expirations(&data.expirations);

  Ok(Frame::SimpleString("OK".into()))
}

