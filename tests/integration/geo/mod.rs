
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

mod utils;

pub fn should_add_basic_single_pos(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.geoadd("Sicily", (13.361389, 38.115556, "Palermo")).and_then(|(client, count)| {
    assert_eq!(count, 1);

    Ok::<_, RedisError>(())
  }))
}

pub fn should_add_and_check_multiple(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.geoadd("Sicily", vec![(13.361389, 38.115556, "Palermo"), (15.087269, 37.502669, "Catania")]).and_then(|(client, count)| {
    assert_eq!(count, 2);

    client.geodist("Sicily", "Palermo", "Catania", None)
  })
  .and_then(|(client, dist)| {
    assert_eq!(dist, Some(166274.1516));

    client.geohash("Sicily", vec!["Palermo", "Catania"])
  })
  .and_then(|(client, hashes)| {
    assert_eq!(hashes, vec!["sqc8b49rny0".to_string(), "sqdtr74hyu0".to_string()]);

    client.geopos("Sicily", vec!["Palermo", "Catania", "NonExisting"])
  })
  .and_then(|(client, coordinates)| {
    assert_eq!(coordinates, vec![
      Some((13.36138933897018433, 38.11555639549629859)),
      Some((15.08726745843887329, 37.50266842333162032)),
      None
    ]);

    Ok::<_, RedisError>(())
  }))
}

pub fn should_correctly_run_georadius(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.geoadd("Sicily", vec![(13.361389, 38.115556, "Palermo")]).and_then(|(client, count)| {
    assert_eq!(count, 1);

    client.georadius("Sicily", 15.0, 37.0, 200.0, GeoUnit::Kilometers, false, false, false, None, None, None, None)
  })
  .and_then(|(client, values)| {
    assert_eq!(values, vec![
      RedisValue::from("Palermo")
    ]);

    client.georadius("Sicily", 15.0, 37.0, 200.0, GeoUnit::Kilometers, true, true, false, None, None, None, None)
  })
  .and_then(|(client, values)| {
    assert_eq!(values, vec![
      RedisValue::from("Palermo"),
      RedisValue::from("190.4424"),
      RedisValue::Array(vec![
        RedisValue::from("13.36138933897018433"),
        RedisValue::from("38.11555639549629859")
      ])
    ]);

    Ok::<_, RedisError>(())
  }))
}