use futures::future;
use futures::{IntoFuture, Future, Stream};
use futures::stream;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;

use super::utils;

pub fn should_sadd_on_new_set(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  // Test adding values to the set "foo" as a vec  
  Box::new(client.sadd("foo", vec![1, 2, 3]).and_then(|(client, len)| {
    assert_eq!(len, 3);
    // Test adding value to the set "foo" as a single int that already exists 
    client.sadd("foo", 1)
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 0);

    // Test adding value to the set "foo" as a single int that doesn't exist
    client.sadd("foo", 4)
  })
  .and_then(|(client, len)| { 
    assert_eq!(len, 1);

    // Test adding value to the set "foo" as a vec that doesn't exist
    client.sadd("foo", vec![5])
  })
  .and_then(|(client, len)| { 
    assert_eq!(len, 1);

    // Test adding values to the set "foo" as a vec that containts two pre-existing values and two new values
    client.sadd("foo", vec![1, 2, 6, 7])
  })
 .and_then(|(client, len)| {
    assert_eq!(len, 2);

    Ok(())
  }))
}

pub fn should_srem_members_of_set(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  // Test adding values to the set "foo" as a vec for removal 
  Box::new(client.sadd("foo", vec![1, 2, 3]).and_then(|(client, len)| {
    assert_eq!(len, 3);
    // Test removing value from the set "foo" as a single int 
    client.srem("foo", 1)
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 1);

    client.sadd("foo", vec![1, 4])
  })
  .and_then(|(client, len)| { 
    assert_eq!(len, 2);

    // Test removing value from the set "foo" as a vec with two existing values and one non-existing value
    client.srem("foo", vec![1, 2, 5])
  })
  .and_then(|(client, len)| { 
    assert_eq!(len, 2);

    // Test removing value from the set "foo" that does not exist
    client.srem("foo", vec![7])
  })
 .and_then(|(client, len)| {
    assert_eq!(len, 0);

    Ok(())
  }))
}