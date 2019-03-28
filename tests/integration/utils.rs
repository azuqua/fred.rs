
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
use futures::sync::oneshot::{
  Sender as OneshotSender,
  Receiver as OneshotReceiver,
  channel as oneshot_channel
};
use futures::stream::ForEach;
use futures::sync::mpsc::{
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
use fred::client::RedisClient;
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

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .gen_ascii_chars()
    .take(len)
    .collect()
}
