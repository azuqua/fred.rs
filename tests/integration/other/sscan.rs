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

pub fn should_sscan_simple_database(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let commands_ft = client.sadd("foo", "a").and_then(|(client, _)| {
    client.sadd("foo", "b")
  })
  .and_then(|(client, _)| {
    client.sadd("foo", "c")
  })
  .and_then(|(client, _)| {
    // count that all three were recv
    client.sscan("foo", Some("*"), Some(1)).fold(3, |mut count, mut state| {
      for key in state.take_results().unwrap() {
        if key.as_str().unwrap() == "a" || key.as_str().unwrap() == "b" || key.as_str().unwrap() == "c" {
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
