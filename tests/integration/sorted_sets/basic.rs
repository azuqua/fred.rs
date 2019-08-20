
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

use fred::client::{
  f64_to_redis_string,
  redis_string_to_f64
};

pub fn should_add_and_remove_elements(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let scores = vec![(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")];

  Box::new(client.zadd("foo", None, false, false, scores).and_then(|(client, count)| {
    assert_eq!(count.as_i64().unwrap(), 5);
    client.zcard("foo")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 5);
    client.zscore("foo", "c")
  })
  .and_then(|(client, score)| {
    assert_eq!(redis_string_to_f64(&score.as_str()).unwrap(), 3.0);
    client.zcount("foo", 0.0, 10.0)
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 5);
    client.zrem("foo", vec!["a", "b"])
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 2);
    client.zcard("foo")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 3);
    client.zrank("foo", "c")
  })
  .and_then(|(client, rank)| {
    assert_eq!(rank.as_i64().unwrap(), 0);
    client.zrange("foo", 0, 5, true)
  })
  .and_then(|(client, values)| {
    let values: Vec<(String, f64)> = values.chunks_exact(2).map(|chunk| {
      (chunk[0].as_str().to_string(), redis_string_to_f64(&chunk[1].as_str()).unwrap())
    })
    .collect();

    assert_eq!(values, vec![("c".into(), 3.0), ("d".into(), 4.0), ("e".into(), 5.0)]);
    Ok(())
  }))
}

pub fn should_push_and_pop_min_max(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let values = vec![
    (1.0, "a"),
    (2.0, "b"),
    (3.0, "c"),
    (4.0, "d"),
    (5.0, "e"),
    (6.0, "f")
  ];

  Box::new(client.zadd("foo", None, false, false, values).and_then(|(client, count)| {
    assert_eq!(count.as_i64().unwrap(), 6);
    client.zpopmax("foo", None)
  })
  .and_then(|(client, values)| {
    let values: Vec<(f64, String)> = values.chunks_exact(2).map(|chunk| {
      (redis_string_to_f64(&chunk[1].as_str()).unwrap(), chunk[0].as_str().to_string())
    })
    .collect();
    assert_eq!(values, vec![(6.0, "f".into())]);

    client.zpopmax("foo", Some(2))
  })
  .and_then(|(client, values)| {
    let values: Vec<(f64, String)> = values.chunks_exact(2).map(|chunk| {
      (redis_string_to_f64(&chunk[1].as_str()).unwrap(), chunk[0].as_str().to_string())
    })
    .collect();
    assert_eq!(values, vec![(5.0, "e".into()), (4.0, "d".into())]);

    client.zpopmin("foo", None)
  })
  .and_then(|(client, values)| {
    let values: Vec<(f64, String)> = values.chunks_exact(2).map(|chunk| {
      (redis_string_to_f64(&chunk[1].as_str()).unwrap(), chunk[0].as_str().to_string())
    })
    .collect();
    assert_eq!(values, vec![(1.0, "a".into())]);

    client.zpopmin("foo", Some(2))
  })
  .and_then(|(client, values)| {
    let values: Vec<(f64, String)> = values.chunks_exact(2).map(|chunk| {
      (redis_string_to_f64(&chunk[1].as_str()).unwrap(), chunk[0].as_str().to_string())
    })
    .collect();
    assert_eq!(values, vec![(2.0, "b".into()), (3.0, "c".into())]);

    Ok(())
  }))
}
