
use futures::future;
use futures::{
  IntoFuture,
  Future,
  Stream
};

use futures::stream;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;
use fred::owned::RedisClientOwned;

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

pub fn should_set_and_get_random_keys(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  // set and get 1000 random keys

  Box::new(stream::iter_ok(0..1000).fold(client, |client, _| {
    let key = utils::random_string(32);
    let val = utils::random_string(1000);

    let get_key = key.clone();
    let del_key = key.clone();
    let get_val = val.clone();
    client.set(&key, val, None, None).and_then(move |(client, set)| {
      assert!(set);
      client.get(get_key)
    })
    .and_then(move |(client, result)| {
      let result = result.unwrap().into_string().unwrap();
      assert_eq!(result, get_val);
      client.del(del_key)
    })
    .and_then(move |(client, deleted)| {
      assert_eq!(deleted, 1);
      client.get(key)
    })
    .and_then(|(client, result)| {
      assert_eq!(result, None);
      Ok(client)
    })
  })
  .map(|_| ()))
}

pub fn should_expire_and_persist(client: RedisClient)-> Box<Future<Item=(), Error=RedisError>>{
  Box::new(client.set("foo", "bar", None, None).and_then(|(client, _)| {
    client.get("foo")
  })
  .and_then(|(client, val)| {
    let val = match val {
      Some(v) => v,
      None => panic!("Expected value for foo not found.")
    };

    assert_eq!(val.into_string().unwrap(), "bar");
    client.expire("foo",10)
  })
  .and_then(|(client, success)| {
    assert!(success);
    client.persist("foo")
  })
  .and_then(|(client, success)| {
    assert!(success);
    Ok(())
  }))
}