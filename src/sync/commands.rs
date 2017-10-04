//! Commands for the borrowed interface.
//!
//! Implementation details moved here to keep the borrowed.rs file from becoming too unwieldy.

use futures::Future;
use futures::sync::oneshot::{
  Sender as OneshotSender
};

use boxfnonce::SendBoxFnOnce;

use ::error::*;
use ::types::*;
use ::RedisClient;

use super::utils;

pub type CommandFnResp = Box<Future<Item=Option<RedisClient>, Error=RedisError>>;
// used instead of Box<FnOnce> due to https://github.com/rust-lang/rust/issues/28796
pub type CommandFn = SendBoxFnOnce<(RedisClient,), CommandFnResp>;

pub type ConnectSender = OneshotSender<Result<(), RedisError>>;

// Make sure everything that is owned by these functions is moved into the client's function, or is
// explicitly dropped before the function finishes. See the issue above for why this is the case.

pub fn subscribe(client: RedisClient, tx: OneshotSender<Result<usize, RedisError>>, channel: String) -> CommandFnResp {
  Box::new(client.subscribe(channel).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn unsubscribe(client: RedisClient, tx: OneshotSender<Result<usize, RedisError>>, channel: String) -> CommandFnResp {
  Box::new(client.unsubscribe(channel).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn publish(client: RedisClient, tx: OneshotSender<Result<i64, RedisError>>, channel: String, message: RedisValue) -> CommandFnResp {
  Box::new(client.publish(channel, message).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn get(client: RedisClient, tx: OneshotSender<Result<Option<RedisValue>, RedisError>>, key: RedisKey) -> CommandFnResp {
  Box::new(client.get(key).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn set(client: RedisClient, tx: OneshotSender<Result<bool, RedisError>>, key: RedisKey, value: RedisValue, expire: Option<Expiration>, options: Option<SetOptions>) -> CommandFnResp {
  Box::new(client.set(key, value, expire, options).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn del(client: RedisClient, tx: OneshotSender<Result<usize, RedisError>>, keys: Vec<RedisKey>) -> CommandFnResp {
  Box::new(client.del(keys).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn decr(client: RedisClient, tx: OneshotSender<Result<i64, RedisError>>, key: RedisKey) -> CommandFnResp {
  Box::new(client.decr(key).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn incr(client: RedisClient, tx: OneshotSender<Result<i64, RedisError>>, key: RedisKey) -> CommandFnResp {
  Box::new(client.incr(key).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn hget(client: RedisClient, tx: OneshotSender<Result<Option<RedisValue>, RedisError>>, key: RedisKey, field: RedisKey) -> CommandFnResp {
  Box::new(client.hget(key, field).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn hset(client: RedisClient, tx: OneshotSender<Result<usize, RedisError>>, key: RedisKey, field: RedisKey, value: RedisValue) -> CommandFnResp {
  Box::new(client.hset(key, field, value).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

pub fn hdel(client: RedisClient, tx: OneshotSender<Result<usize, RedisError>>, key: RedisKey, fields: Vec<RedisKey>) -> CommandFnResp {
  Box::new(client.hdel(key, fields).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}

