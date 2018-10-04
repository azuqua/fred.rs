#![allow(unused_variables)]
#![allow(unused_imports)]

extern crate fred;
extern crate tokio_core;
extern crate tokio_timer_patched as tokio_timer;
extern crate futures;

use fred::RedisClient;
use fred::types::*;
use fred::error::*;

use tokio_core::reactor::Core;
use futures::Future;
use futures::stream::{
  self,
  Stream
};
use futures::future::{
  self,
  Either
};

use tokio_timer::Timer;
use std::time::Duration;

fn main() {
  let foo_config = RedisConfig::default();
  let bar_config = RedisConfig::default();
  let baz_config = RedisConfig::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let foo_client = RedisClient::new(foo_config);
  let bar_client = RedisClient::new(bar_config);
  let baz_client = RedisClient::new(baz_config);

  let foo_connection = foo_client.connect(&handle);
  let bar_connection = bar_client.connect(&handle);
  let baz_connection = baz_client.connect(&handle);

  // join all the connection futures together
  let composed_connections = foo_connection
    .join(bar_connection)
    .join(baz_connection);

  // create error handlers for all the clients
  let foo_errors = foo_client.on_error().for_each(|err| {
    println!("Foo client error: {:?}", err);
    Ok(())
  });
  let bar_errors = bar_client.on_error().for_each(|err| {
    println!("Bar client error: {:?}", err);
    Ok(())
  });
  let baz_errors = baz_client.on_error().for_each(|err| {
    println!("Baz client error: {:?}", err);
    Ok(())
  });

  // join the error stream futures together into one future
  let composed_errors = foo_errors
    .join(bar_errors)
    .join(baz_errors);

  // next create futures representing the commands that each client should run...

  // read `foo`, wait 10 seconds, quit
  let foo_commands = foo_client.on_connect().and_then(|foo_client| {
    println!("Foo client connected.");

    foo_client.get("foo")
  })
  .and_then(|(foo_client, value)| {
    println!("Foo client got `foo`: {:?}. Sleeping for 10 seconds...", value);

    let timer = Timer::default();
    timer.sleep(Duration::from_millis(10 * 1000)).from_err::<RedisError>()
      .map(move |_| foo_client)
  })
  .and_then(|foo_client| {
    println!("Closing foo client...");

    foo_client.quit()
  });

  // increment `bar`, quit
  let bar_commands = bar_client.on_connect().and_then(|bar_client| {
    println!("Bar client connected.");

    bar_client.incr("bar")
  })
  .and_then(|(bar_client, value)| {
    println!("Bar client incremented `bar`: {:?}. Closing bar client...", value);

    bar_client.quit()
  });

  // set `foo` to `1`, decrement `bar`, quit
  let baz_commands = baz_client.on_connect().and_then(|baz_client| {
    println!("Baz client connected.");

    baz_client.set("foo", 1, None, None)
  })
  .and_then(|(baz_client, _)| {
    baz_client.decr("bar")
  })
  .and_then(|(baz_client, value)| {
    println!("Baz client decremented `bar` to {:?}. Closing baz client...", value);

    baz_client.quit()
  });

  // compose the command futures from the clients together
  let composed_commands = foo_commands
    .join(bar_commands)
    .join(baz_commands);

  // compose the connection futures, error handler futures, and command futures into one future, and run that
  let composed = composed_connections
    .join(composed_errors)
    .join(composed_commands);

  let _ = core.run(composed).unwrap();
}