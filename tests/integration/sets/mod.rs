use futures::future;
use futures::{IntoFuture, Future, Stream};
use futures::stream;

use std::borrow::Borrow;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;
use fred::owned::RedisClientOwned;

use super::utils;

pub fn should_sadd_members_to_set(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
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

    // Test adding values to the set "foo" as a vec that contains two pre-existing values and two new values
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

pub fn should_smembers_of_set(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  // Test adding values to the set "foo" as a vec for retrieval
  Box::new(client.sadd("foo", vec![1, 2, 3]).and_then(|(client, len)| {
    assert_eq!(len, 3);
    // Test getting all members from set
    client.smembers("foo")
  })
  .and_then(|(client, result)| {
    assert_eq!(result.len(), 3);

    assert!(result.contains(&RedisValue::String("1".borrow().to_string())));
    assert!(result.contains(&RedisValue::String("2".borrow().to_string())));
    assert!(result.contains(&RedisValue::String("3".borrow().to_string())));

    client.sadd("foo", vec![4])
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 1);

    // Test getting all members to ensure new addition successfully returned
    client.smembers("foo")
  })
  .and_then(|(client, result)| {
    assert_eq!(result.len(), 4);
    assert!(result.contains(&RedisValue::String("4".borrow().to_string())));

    Ok(())
  }))
}