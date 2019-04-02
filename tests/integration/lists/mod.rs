use futures::future;
use futures::{IntoFuture, Future, Stream};
use futures::stream;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;
use fred::owned::RedisClientOwned;

use super::utils;

pub fn should_llen_on_empty_list(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.llen("foo").and_then(|(client, len)| {
    assert_eq!(len, 0);

    Ok(())
  }))
}

pub fn should_llen_on_list_with_elements(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.lpush("foo", 1).and_then(|(client, len)| {
    assert_eq!(len, 1);

    client.lpush("foo", 2)
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 2);

    client.lpush("foo", 3)
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 3);

    client.llen("foo")
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 3);

    Ok(())
  }))
}

pub fn should_lpush_and_lpop_to_list(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(stream::iter_ok(0..50).fold(client, move |client, num| {
    client.lpush("foo", num.to_string()).and_then(move |(client, len)| {
      assert_eq!(len, num + 1);

      Ok(client)
    })
  })
  .and_then(|client| {
    stream::iter_ok(0..50).fold(client, move |client, num| {
      client.lpop("foo").and_then(move |(client, value)| {
        let value = match value {
          Some(v) => v,
          None => panic!("Expected value for list foo not found.")
        };

        assert_eq!(value.into_string().unwrap(), (49 - num).to_string());

        Ok(client)
      })
    })
  })
  .and_then(|_| Ok(())))
}
