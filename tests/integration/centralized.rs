use fred;
use super::utils;

use tokio_core::reactor::{
  Core,
  Handle
};

use tokio_timer::*;

use futures::future;
use futures::{
  IntoFuture,
  Future,
  Stream
};
use futures::sync::oneshot::{
  Sender as OneshotSender,
  Receiver as OneshotReceiver,
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  Sender,
  Receiver,
  channel
};

use std::time::Duration;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::client::RedisClient;
use fred::owned::RedisClientOwned;

use std::thread;
use std::sync::Arc;

use pretty_env_logger;
use tokio_timer::Timer;

use super::keys as keys_tests;

lazy_static! {

  pub static ref TIMER: Timer = Timer::default();

}

#[test]
fn it_should_connect_and_disconnect() {
  let mut config = RedisConfig::default();
  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client = RedisClient::new(config, Some(TIMER.clone()));
  let connection_ft = client.connect(&handle);

  let select_ft = client.on_connect().and_then(|client| {
    client.select(0)
  })
  .and_then(|client| {
    client.quit()
  });

  let (err, client) = core.run(connection_ft.join(select_ft)).unwrap();
  assert!(err.is_none());
}

#[test]
fn it_should_connect_and_disconnect_with_policy() {
  let mut config = RedisConfig::default();
  let policy = ReconnectPolicy::Constant {
    delay: 2000,
    attempts: 0,
    max_attempts: 10
  };

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client = RedisClient::new(config, Some(TIMER.clone()));
  let connection_ft = client.connect_with_policy(&handle, policy);

  let select_ft = client.on_connect().and_then(|client| {
    client.select(0)
  })
  .and_then(|client| {
    client.quit()
  });

  let (_, client) = core.run(connection_ft.join(select_ft)).unwrap();
}


pub mod keys {
  use super::*;

  #[test]
  fn it_should_set_and_get_simple_key() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      keys_tests::should_set_and_get_simple_key(client)
    });
  }

  #[test]
  fn it_should_set_and_get_large_key() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      keys_tests::should_set_and_get_large_key(client)
    })
  }

  #[test]
  fn it_should_set_and_get_random_keys() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      keys_tests::should_set_and_get_random_keys(client)
    })
  }

  #[test]
  fn it_should_set_random_keys_with_fqdn_addresses() {
    let config = RedisConfig::new_centralized("localhost", 6379, None);

    utils::setup_test_client(config, TIMER.clone(),|client| {
      keys_tests::should_set_and_get_random_keys(client)
    })
  }

  #[cfg(all(feature="metrics", not(feature="mock")))]
  #[test]
  fn it_should_track_latency_and_size_metrics() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      keys_tests::should_track_latency_and_size(client)
    })
  }

}

pub mod hashes {
  use super::*;


}

pub mod lists {
  use super::*;


}

pub mod sets {
  use super::*;


}
