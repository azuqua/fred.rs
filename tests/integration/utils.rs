
use redis_client;

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
  Stream
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

use redis_client::error::{
  RedisErrorKind,
  RedisError
};
use redis_client::types::*;
use redis_client::RedisClient;

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

pub fn setup_test_client<F: FnOnce(RedisClient) -> TestFuture>(config: RedisConfig, func: F) {
  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client = RedisClient::new(config);
  let connection = client.connect(&handle);

  let clone = client.clone();
  let commands = client.on_connect().and_then(|client| {
    func(client)
  })
  .and_then(|_| {
    clone.quit()
  });

  let res = core.run(connection.join(commands));

  assert!(res.is_ok());
}

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .gen_ascii_chars()
    .take(len)
    .collect()
}

