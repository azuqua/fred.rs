use futures::future;
use futures::future::Either;
use futures::{IntoFuture, Future, Stream};

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;
use fred::owned::RedisClientOwned;

use super::utils;

use tokio_timer::Timer;
use std::time::Duration;

use std::rc::Rc;
use std::cell::RefCell;

const CHANNEL1: &'static str = "foo";
const CHANNEL2: &'static str = "bar";
const CHANNEL3: &'static str = "baz";
const FAKE_MESSAGE: &'static str = "wibble";

fn value_to_str(value: &RedisValue) -> String {
  value.as_string().expect("value wasnt a string")
}

pub fn should_psubscribe_on_multiple_channels(publisher: RedisClient, subscriber: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  pretty_env_logger::init();

  let timer = Timer::default();

  let timer_ft = timer.sleep(Duration::from_secs(10)).from_err::<RedisError>().and_then(|_| {
    Err::<(), RedisError>(RedisError::new(RedisErrorKind::Timeout, "Didn't receive message."))
  });

  let on_message_ft = subscriber.on_message().fold(0, |count, (channel, message)| {
    // check each channel is correct in the right order, using count to check (should go foo, bar, baz)
    // on third return a canceled error
    println!("got message {} {}", channel, message.as_string().unwrap());

    if count == 0 && channel == CHANNEL1 && &value_to_str(&message) == FAKE_MESSAGE {
      Ok(count + 1)
    }else if count == 1 && channel == CHANNEL2 && &value_to_str(&message) == FAKE_MESSAGE {
      Ok(count + 1)
    }else if count == 2 && channel == CHANNEL3 && &value_to_str(&message) == FAKE_MESSAGE {
      Err(RedisError::new_canceled())
    }else{
      Err(RedisError::new(RedisErrorKind::Unknown, "Invalid message."))
    }
  })
  .then(|result| {
    if let Err(e) = result {
      if e.is_canceled() {
        Ok(())
      }else{
        Err(e)
      }
    }else{
      Err(RedisError::new_timeout())
    }
  });
  let message_ft = on_message_ft.select2(timer_ft).map_err(|error| match error {
    Either::A((e, _)) => e,
    Either::B((e, _)) => e
  });

  let subscribe_ft = subscriber.clone().psubscribe(vec![CHANNEL1, "ba*"]);

  let publish_ft = timer.sleep(Duration::from_secs(2)).from_err::<RedisError>().and_then(move |_| {
    // emit on 3 channels in a row
    publisher.publish(CHANNEL1, FAKE_MESSAGE).and_then(|(publisher, _)| {
      publisher.publish(CHANNEL2, FAKE_MESSAGE)
    })
    .and_then(|(publisher, _)| {
      publisher.publish(CHANNEL3, FAKE_MESSAGE)
    })
  });
  let all_futures = subscribe_ft.join(message_ft).join(publish_ft);

  Box::new(all_futures.map(|_| ()))
}

pub fn should_punsubscribe_on_multiple_channels(publisher: RedisClient, subscriber: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {

  Box::new(future::ok(()))
}

