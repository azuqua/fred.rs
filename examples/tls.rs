#![allow(unused_variables)]
#![allow(unused_imports)]

/// Note: Fred must be compiled with the `enable-tls` feature for this to work.

extern crate fred;
extern crate tokio_core;
extern crate futures;

use fred::RedisClient;
use fred::owned::RedisClientOwned;
use fred::types::*;

use tokio_core::reactor::Core;
use futures::Future;

fn main() {
  let config = RedisConfig::Centralized {
    // Note: this must match the hostname tied to the cert
    host: "foo.bar.com".into(),
    port: 6379,
    key: Some("key".into()),
    // if compiled without `enable-tls` setting this to `true` does nothing, which is done to avoid requiring TLS dependencies unless necessary
    tls: true
  };

  // otherwise usage is the same as the non-tls client...

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Connecting to {:?}...", config);

  let client = RedisClient::new(config, None);
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