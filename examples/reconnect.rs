#![allow(unused_variables)]
#![allow(unused_imports)]

extern crate fred;
extern crate tokio_core;
extern crate tokio_timer_patched as tokio_timer;
extern crate futures;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use fred::RedisClient;
use fred::owned::RedisClientOwned;

use fred::types::*;
use fred::error::*;

use tokio_core::reactor::Core;
use tokio_timer::Timer;
use futures::Future;
use futures::stream::Stream;

use std::time::Duration;

fn main() {
  pretty_env_logger::init();

  let config = RedisConfig::default();
  let policy = ReconnectPolicy::Constant {
    delay: 1000,
    attempts: 0,
    max_attempts: 0
  };
  let timer = Timer::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Connecting to {:?} with policy {:?}", config, policy);

  let client = RedisClient::new(config, Some(timer.clone()));
  let connection = client.connect_with_policy(&handle, policy);

  let errors = client.on_error().for_each(|err| {
    println!("Client error: {:?}", err);
    Ok(())
  });

  let reconnects = client.on_reconnect().for_each(|client| {
    println!("Client reconnected.");
    Ok(())
  });

  let commands = client.on_connect().and_then(|client| {
    println!("Client connected.");

    client.select(1)
  })
  .and_then(|client| {
    // sleep for 30 seconds, maybe restart the redis server in another process...

    println!("Sleeping for 30 seconds...");
    timer.sleep(Duration::from_millis(30 * 1000))
      .from_err::<RedisError>()
      .map(move |_| client)
  })
  .and_then(|client| {
    client.quit()
  });

  let composed = connection.join(commands)
    .join(errors)
    .join(reconnects);

  let _ = core.run(composed).unwrap();
}