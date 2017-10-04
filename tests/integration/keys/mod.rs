
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

static FOOBAR: &'static str = "foobar";

pub fn should_set_and_get_simple_key(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.set("foo", "bar", None, None).and_then(|(client, set)| {
    assert!(set);
    client.get("foo")
  })
  .and_then(|(client, val)| {
    let val = match val {
      Some(v) => v,
      None => panic!("Expected value for foo not found.")
    };

    assert_eq!(val.into_string().unwrap(), "bar");
    client.del("foo")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 1);
    client.get("foo")
  })
  .and_then(|(client, val)| {
    assert!(val.is_none());
    Ok(())
  }))
}

pub fn should_set_and_get_large_key(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let value = utils::random_string(1000000);

  Box::new(client.set(FOOBAR, value.clone(), None, None).and_then(|(client, set)| {
    assert!(set);
    client.get(FOOBAR)
  })
  .and_then(move |(client, val)| {
    let val = match val {
      Some(v) => v,
      None => panic!("Expected value for foo not found.")
    };

    assert_eq!(val.into_string().unwrap(), value);
    client.del(FOOBAR)
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 1);
    client.get(FOOBAR)
  })
  .and_then(|(client, val)| {
    assert!(val.is_none());
    Ok(())
  }))
}