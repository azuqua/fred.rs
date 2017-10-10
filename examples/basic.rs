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
  .and_then(|(client, info)| {
    println!("Redis server info: {}", info);

    client.get("foo")
  })
  .and_then(|(client, result)| {
    println!("Got foo: {:?}", result);

    client.set("foo", "bar", Some(Expiration::PX(1000)), Some(SetOptions::NX))
  })
  .and_then(|(client, result)| {
    println!("Set 'bar' at 'foo'? {}. Subscribing to 'baz'...", result);

    client.subscribe("baz")
  })
  .and_then(|(client, result)| {
    println!("Subscribed to 'baz'. Now subscribed to {} channels. Now exiting...", result);

    client.quit()
  });

  let (reason, client) = match core.run(connection.join(commands)) {
    Ok((r, c)) => (r, c),
    Err(e) => panic!("Connection closed abruptly: {}", e)
  };

  println!("Connection closed gracefully with error: {:?}", reason);
}