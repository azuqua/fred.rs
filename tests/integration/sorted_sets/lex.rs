
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

pub fn should_read_sorted_lex_entries(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let values = vec![
    (1.0, "a"),
    (1.0, "f"),
    (1.0, "d"),
    (1.0, "c"),
    (1.0, "e"),
    (1.0, "b")
  ];

  Box::new(client.zadd("foo", None, false, false, values).and_then(|(client, count)| {
    assert_eq!(count.as_i64().unwrap(), 6);
    client.zlexcount("foo", "-", "+")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 6);

    client.zrangebylex("foo", "-", "+", None)
  })
  .and_then(|(client, values)| {
    let values: Vec<String> = values.iter()
      .map(|val| val.as_str().unwrap().to_string()).collect();

    assert_eq!(values, vec![
      "a".to_owned(),
      "b".to_owned(),
      "c".to_owned(),
      "d".to_owned(),
      "e".to_owned(),
      "f".to_owned()
    ]);

    client.zrevrangebylex("foo", "+", "-", None)
  })
  .and_then(|(client, values)| {
    let values: Vec<String> = values.iter()
      .map(|val| val.as_str().unwrap().to_string()).collect();

    assert_eq!(values, vec![
      "f".to_owned(),
      "e".to_owned(),
      "d".to_owned(),
      "c".to_owned(),
      "b".to_owned(),
      "a".to_owned()
    ]);

    Ok(())
  }))
}