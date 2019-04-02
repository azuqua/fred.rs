
use futures::future;
use futures::{
  IntoFuture,
  Future,
  Stream
};
use futures::stream;

use std::rc::Rc;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;
use fred::owned::RedisClientOwned;

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

pub fn should_set_and_get_all_simple_key(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.hset("foo", "bar", "baz").and_then(|(client, _)| {
    client.hgetall("foo")
  })
  .and_then(|(client, mut val)| {
    assert_eq!(val.len(), 1);
    assert!(val.contains_key("bar".into()));
    assert_eq!(val.remove("bar".into()).unwrap().into_string().unwrap(), "baz");
    client.hdel("foo", "bar")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 1);
    client.hgetall("foo")
  })
  .and_then(|(client, val)| {
    assert_eq!(val.len(), 0);
    Ok(())
  }))
}

pub fn should_check_hexists(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.hset("foo", "bar", "baz").and_then(|(client, count)| {
    client.hexists("foo", "bar")
  })
  .and_then(|(client, exists)| {
    assert!(exists);
    client.hdel("foo", "bar")
  })
  .and_then(|(client, deleted)| {
    assert_eq!(deleted, 1);
    client.hexists("foo", "bar")
  })
  .and_then(|(client, exists)| {
    assert!(!exists);
    Ok(())
  }))
}

pub fn should_read_large_hash(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  // set 1000 4k strings in a hashmap, then read in hkeys and hvals
  let mut keys = Vec::with_capacity(1000);
  let mut vals = Vec::with_capacity(1000);
  for _ in 0..1000 {
    keys.push(utils::random_string(32));
    vals.push(utils::random_string(4000));
  }
  let keys = Rc::new(keys);
  let vals = Rc::new(vals);

  let set_keys = keys.clone();
  let set_vals = vals.clone();
  let get_keys = keys.clone();
  let get_vals = vals.clone();

  Box::new(stream::iter_ok(0..1000).fold(client, move |client, idx| {
    client.hset("foo", &set_keys[idx], &set_vals[idx]).and_then(|(client, _)| {
      Ok(client)
    })
  })
  .and_then(|client| {
    client.hgetall("foo")
  })
  .and_then(move |(client, mut values)| {
    for (idx, key) in get_keys.iter().enumerate() {
      let expected: RedisValue = (&get_vals[idx]).into();
      assert_eq!(values.get(key).unwrap(), &expected);
      let _ = values.remove(key);
    }
    assert_eq!(values.len(), 0);

    client.del("foo")
  })
  .and_then(|(client, _)| {
    client.hgetall("foo")
  })
  .and_then(|(client, values)| {
    assert_eq!(values.len(), 0);
    Ok(())
  }))
}
