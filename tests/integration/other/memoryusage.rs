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
use hyper::header::UserAgent;

use super::super::utils;

// This is the example from the docs
pub fn should_memory_usage_simple(client: RedisClient) -> Box<dyn Future<Item=(), Error=RedisError>> {
  let commands_ft = client.set("", "", None, None)
  .and_then(|(client, _)| {
    // verify that this is a reasonable number
    client.memoryusage("", None).then(|mut res| {
      // when I test in centralized mode, it is 60 but it's possible that different redis deployments would give different values.
      match res {
        Ok(usage) => assert!(usage < 60),
        Err(e) => panic!("Failed MEMORY USAGE")
      }
      Ok::<_, RedisError>(res)
    })
  });

  Box::new(commands_ft.map(|_| ()))
}

pub fn should_memory_usage_hset(client: RedisClient) -> Box<dyn Future<Item=(), Error=RedisError>> {
  let commands_ft = client.hset("foo", "xxx", "b").and_then(|(client, _)| {
    client.hset("foo", "ba", "c")
  })
  .and_then(|(client, _)| {
    client.hset("foo", "c", "baaaaaaaaaaa")
  })
  .and_then(|(client, _)| {
    // verify that this is a reasonable number
    client.memoryusage("foo", Some(3)).then(|mut res| {
      // when I test in centralized mode, it is 91 but it's possible that different redis deployments would give different values.
      match res {
        Ok(usage) => {
          assert!(usage < 101);
        },
        Err(e) => panic!("Failed MEMORY USAGE")
      }
      Ok::<_, RedisError>(res)
    })
  });

  Box::new(commands_ft.map(|_| ()))
}




