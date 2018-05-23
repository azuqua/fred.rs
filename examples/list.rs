#![allow(unused_variables)]
#![allow(unused_imports)]

extern crate fred;
extern crate tokio_core;
extern crate futures;

use fred::RedisClient;
use fred::types::*;

use tokio_core::reactor::Core;
use futures::Future;

fn main() {
  let config = RedisConfig::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Connecting to {:?}...", config);

  let client = RedisClient::new(config);
  let connection = client.connect(&handle);

  let commands = client.on_connect().and_then(|client| {
    println!("Client connected.");

    client.select(0)
  })
  .and_then(|client| {
    println!("Selected database.");

    client.info(None)
  })
  .and_then(|(client, _)| {
    println!("LPush first value to 'foo'");

    client.lpush("foo", 1)
  })
  .and_then(|(client, result)| {
    println!("LPush second value to 'foo'");

    client.lpush("foo", "two")
  })
  .and_then(|(client, result)| {
    println!("LPush third value to 'foo'");

    client.lpush("foo", 3.0)
  })
  .and_then(|(client, result)| {
    client.lpop("foo")
  })
  .and_then(|(client, result)| {
    println!("LPop got {:?}", result);

    client.lpop("foo")
  })
  .and_then(|(client, result)| {
    println!("LPop got {:?}", result);

    client.lpop("foo")
  })
  .and_then(|(client, result)| {
    println!("LPop got {:?}", result);

    client.quit()
  });

  let (reason, client) = match core.run(connection.join(commands)) {
    Ok((r, c)) => (r, c),
    Err(e) => panic!("Connection closed abruptly: {}", e)
  };

  println!("Connection closed gracefully with error: {:?}", reason);
}

