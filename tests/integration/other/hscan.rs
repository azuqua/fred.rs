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

pub fn should_hscan_simple_database(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let commands_ft = client.hset("foo", "a", "b").and_then(|(client, _)| {
    client.hset("foo", "b", "c")
  })
  .and_then(|(client, _)| {
    client.hset("foo", "c", "d")
  })
  .and_then(|(client, _)| {
    // count that all three were recv
    client.hscan("foo", Some("*"), Some(1)).fold(3, |mut count, mut state| {
      for key in state.take_results().unwrap().keys() {
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




