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

pub fn set(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn get(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn del(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn expire(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn hget(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn hset(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

pub fn hdel(data: &mut DataSet, args: Vec<RedisValue>) -> Result<Frame, RedisError> {


  unimplemented!()
}

