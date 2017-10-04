
use futures::future;
use futures::{
  IntoFuture,
  Future,
  Stream
};

use redis_client::error::{
  RedisErrorKind,
  RedisError
};
use redis_client::types::*;
use redis_client::RedisClient;

use super::utils;

pub fn should_set_and_get_simple_key(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.hset("foo", "bar", "baz").and_then(|(client, _)| {
    client.hget("foo", "bar")
  })
  .and_then(|(client, val)| {
    let val = match val {
      Some(v) => v,
      None => panic!("Expected value for foo not found.")
    };

    assert_eq!(val.into_string().unwrap(), "baz");
    client.hdel("foo", "bar")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 1);
    client.hget("foo", "bar")
  })
  .and_then(|(client, val)| {
    assert!(val.is_none());
    Ok(())
  }))
}
