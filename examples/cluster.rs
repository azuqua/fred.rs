#![allow(unused_variables)]
#![allow(unused_imports)]

extern crate fred;
extern crate tokio_core;
extern crate futures;

use fred::RedisClient;
use fred::owned::RedisClientOwned;
use fred::types::*;
use fred::error::*;

use tokio_core::reactor::Core;
use futures::Future;

use std::time::Duration;

fn main() {
  let config = RedisConfig::default_clustered();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Connecting to {:?}...", config);

  let client = RedisClient::new(config, None);
  let connection = client.connect(&handle);

  let commands = client.on_connect().and_then(|client| {
    println!("Clustered client connected.");

    client.info(Some(InfoKind::Cluster))
  })
  .and_then(|(client, info)| {
    println!("Cluster info: {:?}", info);

    client.quit()
  });

  let (reason, client) = match core.run(connection.join(commands)) {
    Ok((r, c)) => (r, c),
    Err(e) => panic!("Connection closed abruptly: {}", e)
  };

  println!("Connection closed gracefully with error: {:?}", reason);
}