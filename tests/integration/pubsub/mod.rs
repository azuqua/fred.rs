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
use fred::borrowed::RedisClientBorrowed;

use std::ops::{
  Deref,
  DerefMut
};

const CHANNEL1: &'static str = "foo";
const CHANNEL2: &'static str = "bar";
const CHANNEL3: &'static str = "baz";
const FAKE_MESSAGE: &'static str = "wibble";

fn value_to_str(value: &RedisValue) -> &str {
  match *value {
    RedisValue::String(ref s) => s,
    _ => panic!("value wasnt a string")
  }

}

pub fn should_psubscribe_on_multiple_channels(publisher: RedisClient, subscriber: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let timer = Timer::default();

  let timer_ft = timer.sleep(Duration::from_secs(10)).from_err::<RedisError>().and_then(|_| {
    Err::<(), RedisError>(RedisError::new(RedisErrorKind::Timeout, "Didn't receive message."))
  });

  let on_message_ft = subscriber.on_message().fold(0, |count, (channel, message)| {
    // check each channel is correct in the right order, using count to check (should go foo, bar, baz)
    // on third return a canceled error

    if count == 0 && channel == CHANNEL1 && value_to_str(&message) == FAKE_MESSAGE {
      Ok(count + 1)
    }else if count == 1 && channel == CHANNEL2 && value_to_str(&message) == FAKE_MESSAGE {
      Ok(count + 1)
    }else if count == 2 && channel == CHANNEL3 && value_to_str(&message) == FAKE_MESSAGE {
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


fn incr_shared_count(count: &Rc<RefCell<usize>>) {
  let mut guard = count.borrow_mut();
  let mut guard_ref = guard.deref_mut();
  *guard_ref += 1;
}

fn read_shared_count(count: &Rc<RefCell<usize>>) -> usize {
  count.borrow().deref().clone()
}

pub fn should_punsubscribe_on_multiple_channels(publisher: RedisClient, subscriber: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  // subscribe to multiple channels, but only publish on one of them every second
  // after the first message is received unsubscribe from that channel
  // wait a few seconds and make sure no other messages are received, then exit

  let timer = Timer::default();
  let subscriber2 = subscriber.clone();

  let timer_ft = timer.sleep(Duration::from_secs(20)).from_err::<RedisError>().and_then(|_| {
    Err::<(), RedisError>(RedisError::new(RedisErrorKind::Timeout, "Didn't receive any message."))
  });

  let shared_count = Rc::new(RefCell::new(0));
  let message_count = shared_count.clone();

  let on_message_ft = subscriber2.on_message().fold(subscriber2, move |subscriber, (channel, message)| {
    if read_shared_count(&shared_count) == 0 && channel == CHANNEL2 && value_to_str(&message) == FAKE_MESSAGE {
      let count = shared_count.clone();
      Box::new(subscriber.punsubscribe("ba*").map(move |(client, _)| {
        incr_shared_count(&count);
        client
      }))
    }else{
      utils::future_error(RedisError::new(RedisErrorKind::Unknown, "Invalid message."))
    }
  });

  let success_count_ft = timer.sleep(Duration::from_secs(5)).from_err::<RedisError>().and_then(move |_| {
    if read_shared_count(&message_count) == 1 {
      Ok(())
    }else{
      Err(RedisError::new(RedisErrorKind::Unknown, "Invalid message count."))
    }
  });
  let on_message_ft = on_message_ft.select2(success_count_ft).map_err(|error| match error {
    Either::A((e, _)) => e,
    Either::B((e, _)) => e
  });

  let message_ft = on_message_ft.select2(timer_ft).map_err(|error| match error {
    Either::A((e, _)) => e,
    Either::B((e, _)) => e
  });

  let subscribe_ft = subscriber.clone().psubscribe(vec!["ba*", CHANNEL1, "f*"]);

  let publisher_ft = timer.interval(Duration::from_secs(1)).from_err::<RedisError>().fold((publisher, 0), move |(publisher, count), _| {
    if count < 6 {
      Box::new(publisher.publish(CHANNEL2, FAKE_MESSAGE).map(move |(client, _)| (client, count + 1)))
    }else{
      utils::future_error(RedisError::new_canceled())
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
  let all_futures = message_ft.join(subscribe_ft).join(publisher_ft);

  Box::new(all_futures.map(|_| ()))
}

