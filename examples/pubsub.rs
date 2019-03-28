#![allow(unused_variables)]
#![allow(unused_imports)]

//! Create two clients, one for publishing messages and one for subscribing to messages. The publisher publishes `MESSAGE_COUNT`
//! messages and then quits, and the subscriber listens until it receives `MESSAGE_COUNT` messages, and then quits.

extern crate fred;
extern crate tokio_timer_patched as tokio_timer;
extern crate tokio_core;
extern crate futures;

use fred::RedisClient;
use fred::owned::RedisClientOwned;

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

const MESSAGE_COUNT: u32 = 5;

fn main() {
  let timer = Timer::default();

  let publisher_config = RedisConfig::default();
  let subscriber_config = RedisConfig::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Publisher connecting to {:?}...", publisher_config);
  println!("Subscriber connecting to {:?}...", subscriber_config);

  let publisher = RedisClient::new(publisher_config, Some(timer.clone()));
  let subscriber = RedisClient::new(subscriber_config, Some(timer.clone()));

  let publisher_connection = publisher.connect(&handle);
  let subscriber_connection = subscriber.connect(&handle);

  let publisher_messages = publisher.on_connect().and_then(|publisher| {
    // sleep for a second before publishing messages so the subscriber has time to connect and subscribe

    timer.sleep(Duration::from_millis(1000))
      .from_err::<RedisError>()
      .map(move |_| publisher)
  })
  .and_then(|publisher| {
    stream::iter_ok(0..MESSAGE_COUNT).fold(publisher, |publisher, count| {
      println!("Publishing message #{}...", count + 1);

      publisher.publish("foo", "bar").map(|(client, _)| client)
    })
  })
  .and_then(|publisher| {
    println!("Closing publisher.");

    publisher.quit()
  });

  let subscriber_commands = subscriber.on_connect().and_then(|subscriber| {
    println!("Subscribing to `foo` channel.");

    subscriber.subscribe("foo")
  });

  let subscriber_messages = subscriber.on_message().fold((subscriber, 0), |(subscriber, count), (channel, message)| {
    println!("Received message {:?} on channel {:?}", message, channel);

    // quit after MESSAGE_COUNT messages have been received
    if count < MESSAGE_COUNT - 1 {
      Either::A(future::ok((subscriber, count + 1)))
    }else{
      println!("Closing subscriber.");

      // `quit` will close the stream behind `on_message`, so there's no need to return an error here
      Either::B(subscriber.quit()
        .and_then(move |subscriber| Ok((subscriber, count))))
    }
  });

  let composed = publisher_connection
    .join(subscriber_connection)
    .join(subscriber_messages)
    .join(publisher_messages)
    .join(subscriber_commands);

  let _ = core.run(composed).unwrap();
}