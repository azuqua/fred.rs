#![allow(unused_variables)]
#![allow(unused_imports)]

extern crate redis_client;
extern crate tokio_core;
extern crate tokio_timer;
extern crate futures;

use redis_client::RedisClient;
use redis_client::types::*;
use redis_client::error::*;

use tokio_core::reactor::Core;
use tokio_timer::Timer;
use futures::Future;

use std::time::Duration;

fn main() {
  let config = RedisConfig::default();
  let policy = ReconnectPolicy::Constant {
    delay: 5000,
    attempts: 0,
    max_attempts: 10
  };
  let timer = Timer::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Connecting to {:?} with policy {:?}", config, policy);

  let client = RedisClient::new(config);
  let connection = client.connect_with_policy(&handle, policy);

  let commands = client.on_connect().and_then(|client| {
    println!("Client connected.");

    client.select(1)
  })
  .and_then(|client| {
    // sleep for 10 seconds, maybe restart the redis server in another process...

    println!("Sleeping for 10 seconds...");
    timer.sleep(Duration::from_millis(10 * 1000))
      .from_err::<RedisError>()
      .map(move |_| client)
  })
  .and_then(|client| {
    client.quit()
  });

  let (reason, client) = match core.run(connection.join(commands)) {
    Ok((r, c)) => (r, c),
    Err(e) => panic!("Connection closed abruptly: {}", e)
  };

  println!("Connection closed gracefully with error: {:?}", reason);
}