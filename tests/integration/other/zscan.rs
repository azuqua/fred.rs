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

use super::super::utils;

pub fn should_zscan_simple_database(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let commands_ft = client.zadd("foo", None, false, false, (1.0, "a")).and_then(|(client, _)| {
    client.zadd("foo", None, false, false, (2.0, "b"))
  })
  .and_then(|(client, _)| {
    client.zadd("foo", None, false, false, (3.0, "c"))
  })
  .and_then(|(client, _)| {
    // count that all three were recv
    client.zscan("foo", Some("*"), Some(1)).fold(3, |mut count, mut state| {
      for (key, score) in state.take_results().unwrap() {
        if key.as_str() == "a" || key.as_str() == "b" || key.as_str() == "c" {
          count -= 1;
        }
      }
      state.next();

      Ok::<_, RedisError>(count)
    })
    .and_then(|remaining| {
      assert_eq!(remaining, 0);
      Ok(())
    })
  });

  Box::new(commands_ft.map(|_| ()))
}
