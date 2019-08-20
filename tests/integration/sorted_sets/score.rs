
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

pub fn should_read_sorted_score_entries(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let values = vec![
    (1.0, "a"),
    (6.0, "f"),
    (4.0, "d"),
    (3.0, "c"),
    (5.0, "e"),
    (2.0, "b")
  ];

  Box::new(client.zadd("foo", None, false, false, values).and_then(|(client, count)| {
    assert_eq!(count.as_i64().unwrap(), 6);
    client.zscore("foo", "d")
  })
  .and_then(|(client, count)| {
    assert_eq!(count.as_f64().unwrap(), 4.0);

    client.zrangebyscore("foo", 0.0, 7.0, false, None)
  })
  .and_then(|(client, values)| {
    let values: Vec<String> = values.iter()
      .map(|val| val.as_str().to_string()).collect();

    assert_eq!(values, vec![
      "a".to_owned(),
      "b".to_owned(),
      "c".to_owned(),
      "d".to_owned(),
      "e".to_owned(),
      "f".to_owned()
    ]);

    client.zrangebyscore("foo", 0.0, 10.0, true, Some((0, 10)))
  })
  .and_then(|(client, values)| {
    let values: Vec<(String, f64)> = values.chunks_exact(2)
      .map(|chunk| (chunk[0].as_str().to_string(), chunk[1].as_f64().unwrap()))
      .collect();

    assert_eq!(values, vec![
      ("a".to_owned(), 1.0),
      ("b".to_owned(), 2.0),
      ("c".to_owned(), 3.0),
      ("d".to_owned(), 4.0),
      ("e".to_owned(), 5.0),
      ("f".to_owned(), 6.0)
    ]);

    client.zrevrangebyscore("foo", 10.0, 0.0, false, None)
  })
  .and_then(|(client, values)| {
    let values: Vec<String> = values.iter()
      .map(|val| val.as_str().to_string()).collect();

    assert_eq!(values, vec![
      "f".to_owned(),
      "e".to_owned(),
      "d".to_owned(),
      "c".to_owned(),
      "b".to_owned(),
      "a".to_owned()
    ]);

    client.zrevrangebyscore("foo", 10.0, 0.0, true, Some((0, 10)))
  })
  .and_then(|(client, values)| {
    let values: Vec<(String, f64)> = values.chunks_exact(2)
      .map(|chunk| (chunk[0].as_str().to_string(), chunk[1].as_f64().unwrap()))
      .collect();

    assert_eq!(values, vec![
      ("f".to_owned(), 6.0),
      ("e".to_owned(), 5.0),
      ("d".to_owned(), 4.0),
      ("c".to_owned(), 3.0),
      ("b".to_owned(), 2.0),
      ("a".to_owned(), 1.0)
    ]);

    Ok(())
  }))
}