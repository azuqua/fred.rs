
use fred;

use tokio_core::reactor::{
  Handle,
  Core,
  Remote
};

use futures::future;
use futures::{
  IntoFuture,
  Future,
  BoxFuture,
  Stream,
  stream
};
use futures::channel::oneshot::{
  Sender as OneshotSender,
  Receiver as OneshotReceiver,
  channel as oneshot_channel
};
use futures::stream::ForEach;
use futures::channel::mpsc::{
  Sender,
  Receiver,
  channel
};
use futures::stream::BoxStream;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::client::{RedisClient, RedisClientInner};
use fred::owned::RedisClientOwned;

use tokio_timer::Timer;

use std::thread;
use std::sync::Arc;

use rand;
use rand::Rng;

pub type TestFuture = Box<Future<Item=(), Error=RedisError>>;

macro_rules! sleep_ms(
  ($($arg:tt)*) => { {
    ::std::thread::sleep(::std::time::Duration::from_millis($($arg)*))
  } }
);

pub fn create_core() -> Core {
  Core::new().unwrap()
}


pub fn future_error<T: 'static>(err: RedisError) -> Box<Future<Item = T, Error = RedisError>> {
  Box::new(future::err(err))
}

pub fn future_ok<T: 'static>(d: T) -> Box<Future<Item = T, Error = RedisError>> {
  Box::new(future::ok(d))
}

pub fn setup_test_client<F: FnOnce(RedisClient) -> TestFuture>(config: RedisConfig, timer: Timer, func: F) {
  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client = RedisClient::new(config, Some(timer));
  let connection = client.connect(&handle);

  let clone = client.clone();
  let commands = client.on_connect().and_then(move |client| {
    if client.is_clustered() {
      Box::new(client.split_cluster(&handle).and_then(move |clients| {
        stream::iter_ok(clients.into_iter()).map(|(_client, _) | {
          _client.clone().flushall(false).then(move |_| _client.quit())
        })
        .buffer_unordered(6)
        .fold((), |_, _| Ok::<_, RedisError>(()))
        .map(move |_| (client, "OK".to_owned()))
      }))
    }else{
      client.flushall(false)
    }
  })
  .and_then(|(client, _)| {
    func(client)
  })
  .and_then(|_| {
    clone.quit()
  });

  let _ = core.run(connection.join(commands)).unwrap();
}


fn flush_two_centralized(client_1: RedisClient, client_2: RedisClient) -> Box<Future<Item=(RedisClient, RedisClient), Error=RedisError>> {
  Box::new(client_1.flushall(false).map(|(c, _)| c)
    .join(client_2.flushall(false).map(|(c, _)| c)))
}

pub fn setup_two_test_clients<F: FnOnce(RedisClient, RedisClient) -> TestFuture>(config: RedisConfig, timer: Timer, func: F) {
  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client_1 = RedisClient::new(config.clone(), Some(timer.clone()));
  let connection_1 = client_1.connect(&handle);

  let client_2 = RedisClient::new(config, Some(timer));
  let connection_2 = client_2.connect(&handle);

  let clone_1 = client_1.clone();
  let clone_2 = client_2.clone();

  let connected_ft = client_1.on_connect().join(client_2.on_connect());

  let commands = connected_ft.and_then(move |client| {
    // flush the databases before running the test

    if client_1.is_clustered() {
      let flushall_1 = client_1.split_cluster(&handle).and_then(move |clients| {
        stream::iter_ok(clients.into_iter()).map(|(_client, _) | {
          _client.clone().flushall(false).then(move |_| _client.quit())
        })
        .buffer_unordered(6)
        .fold((), |_, _| Ok::<_, RedisError>(()))
        .map(move |_| client_1)
      });

      let flushall_2 = client_2.split_cluster(&handle).and_then(move |clients| {
        stream::iter_ok(clients.into_iter()).map(|(_client, _) | {
          _client.clone().flushall(false).then(move |_| _client.quit())
        })
        .buffer_unordered(6)
        .fold((), |_, _| Ok::<_, RedisError>(()))
        .map(move |_| client_2)
      });

      Box::new(flushall_1.join(flushall_2))
    }else{
      flush_two_centralized(client_1, client_2)
    }
  })
  .and_then(|(client_1, client_2)| {
    // run the test fn
    func(client_1, client_2)
  })
  .and_then(|_| {
    clone_1.quit().join(clone_2.quit())
  });

  let _ = core.run(connection_1.join(connection_2).join(commands)).unwrap();
}

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .gen_ascii_chars()
    .take(len)
    .collect()
}
