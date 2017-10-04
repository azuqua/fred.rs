#![allow(unused_variables)]
#![allow(unused_imports)]

//! Create two clients, one for publishing messages and one for subscribing to messages. The publisher publishes `MESSAGE_COUNT`
//! messages and then quits, and the subscriber listens until it receives `MESSAGE_COUNT` messages, and then quits.

extern crate redis_client;
extern crate tokio_core;
extern crate tokio_timer;
extern crate futures;

use redis_client::RedisClient;
use redis_client::types::*;
use redis_client::error::*;

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

use std::time::Duration;

const MESSAGE_COUNT: u32 = 5;

fn main() {
  let publisher_config = RedisConfig::default();
  let subscriber_config = RedisConfig::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Publisher connecting to {:?}...", publisher_config);
  println!("Subscriber connecting to {:?}...", subscriber_config);

  let publisher = RedisClient::new(publisher_config);
  let subscriber = RedisClient::new(subscriber_config);

  let publisher_connection = publisher.connect(&handle);
  let subscriber_connection = subscriber.connect(&handle);

  let subscriber_messages = subscriber.on_message().fold((subscriber, 0), |(subscriber, count), (channel, message)| {
    println!("Received message {:?} on channel {:?}", message, channel);

    // quit after MESSAGE_COUNT messages have been received
    if count < MESSAGE_COUNT {
      Either::A(future::ok((subscriber, count + 1)))
    }else{
      // `quit` will close the stream behind `on_message`, so there's no need to return an error here
      Either::B(subscriber.quit()
        .and_then(move |subscriber| Ok((subscriber, count))))
    }
  });

  let publisher_messages = stream::iter_ok(0..MESSAGE_COUNT).fold(publisher, |publisher, count| {
    println!("Publishing message #{}...", count);

    publisher.publish("foo", "bar").map(|(client, _)| client)
  })
  .and_then(|publisher| {
    publisher.quit()
  });

  let composed = publisher_connection
    .join(subscriber_connection)
    .join(subscriber_messages)
    .join(publisher_messages);

  let _ = core.run(composed).unwrap();
}