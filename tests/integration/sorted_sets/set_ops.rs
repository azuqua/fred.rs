
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

pub fn should_perform_set_operations(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let a_values = vec![
    (1.0, "a"),
    (6.0, "f"),
    (4.0, "d"),
    (3.0, "c"),
    (5.0, "e"),
    (2.0, "b")
  ];
  let b_values = vec![
    (1.0, "c"),
    (6.0, "h"),
    (4.0, "f"),
    (3.0, "e"),
    (5.0, "g"),
    (2.0, "d")
  ];

  Box::new(client.zadd("foo", None, false, false, a_values).and_then(|(client, count)| {
    assert_eq!(count.as_i64().unwrap(), 6);
    client.zadd("bar", None, false, false, b_values)
  })
  .and_then(|(client, count)| {
    assert_eq!(count.as_i64().unwrap(), 6);
    client.zunionstore("baz", vec!["foo", "bar"], vec![], Some(AggregateOptions::Max))
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 8);
    client.zrange("baz", 0, -1, true)
  })
  .and_then(|(client, values)| {
    let values: Vec<(String, f64)> = values.chunks_exact(2).map(|chunk| {
      (chunk[0].as_str().unwrap().to_string(), chunk[1].as_f64().unwrap())
    })
    .collect();

    assert_eq!(values, vec![
      ("a".to_string(), 1.0),
      ("b".to_string(), 2.0),
      ("c".to_string(), 3.0),
      ("d".to_string(), 4.0),
      ("e".to_string(), 5.0),
      ("g".to_string(), 5.0),
      ("f".to_string(), 6.0),
      ("h".to_string(), 6.0)
    ]);

    client.zinterstore("wibble", vec!["foo", "bar"], vec![], Some(AggregateOptions::Sum))
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 4);
    client.zrange("wibble", 0, -1, true)
  })
  .and_then(|(client, values)| {
    let values: Vec<(String, f64)> = values.chunks_exact(2).map(|chunk| {
      (chunk[0].as_str().unwrap().to_string(), chunk[1].as_f64().unwrap())
    })
    .collect();

    assert_eq!(values, vec![
      ("c".to_string(), 4.0),
      ("d".to_string(), 6.0),
      ("e".to_string(), 8.0),
      ("f".to_string(), 10.0),
    ]);

    Ok(())
  }))
}