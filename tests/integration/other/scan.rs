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

pub fn should_scan_simple_database(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let commands_ft = client.set("foo", "a", None, None).and_then(|(client, _)| {
    client.set("bar", "b", None, None)
  })
  .and_then(|(client, _)| {
    client.set("baz", "c", None, None)
  })
  .and_then(|(client, _)| {
    // count that all three were recv
    client.scan(Some("*"), Some(1), None).fold(3, |mut count, mut state| {
      for key in state.take_results().unwrap() {
        if key.as_str() == "foo" || key.as_str() == "bar" || key.as_str() == "baz" {
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


