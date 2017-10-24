

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
use fred::RedisClient;

use std::thread;
use std::sync::Arc;

use pretty_env_logger;

use super::keys as keys_tests;
use super::hashes as hashes_tests;

#[test]
fn it_should_connect_and_disconnect() {
  let mut config = RedisConfig::default_clustered();
  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client = RedisClient::new(config);
  let connection_ft = client.connect(&handle);

  let select_ft = client.on_connect().and_then(|client| {
    client.info(None)
  })
  .and_then(|(client, _)| {
    client.quit()
  });

  let (err, client) = core.run(connection_ft.join(select_ft)).unwrap();
  assert!(err.is_none());
}


#[test]
fn it_should_connect_and_disconnect_with_policy() {
  let mut config = RedisConfig::default_clustered();
  let policy = ReconnectPolicy::Constant {
    delay: 2000,
    attempts: 0,
    max_attempts: 10
  };

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  let client = RedisClient::new(config);
  let connection_ft = client.connect_with_policy(&handle, policy);

  let select_ft = client.on_connect().and_then(|client| {
    client.info(None)
  })
  .and_then(|(client, _)| {
    client.quit()
  });

  let (_, client) = core.run(connection_ft.join(select_ft)).unwrap();
}

pub mod keys {
  use super::*;

  #[test]
  fn it_should_set_and_get_simple_key() {
    let config = RedisConfig::default_clustered();
    utils::setup_test_client(config, |client| {
      keys_tests::should_set_and_get_simple_key(client)
    });
  }

  #[test]
  fn it_should_set_and_get_large_key() {
    let config = RedisConfig::default_clustered();
    utils::setup_test_client(config, |client| {
      keys_tests::should_set_and_get_large_key(client)
    })
  }

  #[test]
  fn it_should_set_and_get_random_keys() {
    let config = RedisConfig::default_clustered();
    utils::setup_test_client(config, |client| {
      keys_tests::should_set_and_get_random_keys(client)
    })
  }

}


pub mod hashes {
  use super::*;

  #[test]
  fn it_should_set_and_get_simple_key() {
    let config = RedisConfig::default_clustered();
    utils::setup_test_client(config, |client| {
      hashes_tests::should_set_and_get_simple_key(client)
    });
  }

  #[test]
  fn it_should_set_and_get_all_simple_key() {
    let config = RedisConfig::default_clustered();
    utils::setup_test_client(config, |client| {
      hashes_tests::should_set_and_get_all_simple_key(client)
    });
  }

  #[test]
  fn it_should_check_hexists() {
    let config = RedisConfig::default_clustered();
    utils::setup_test_client(config, |client| {
      hashes_tests::should_check_hexists(client)
    });
  }

  #[test]
  fn it_should_read_large_hash() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, |client| {
      hashes_tests::should_read_large_hash(client)
    });
  }

}
