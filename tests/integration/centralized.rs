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
use futures::channel::oneshot::{
  Sender as OneshotSender,
  Receiver as OneshotReceiver,
  channel as oneshot_channel
};
use futures::channel::mpsc::{
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
use super::sets as sets_tests;
use super::hashes as hashes_tests;
use super::lists as lists_tests;
use super::pubsub as pubsub_tests;
use super::other as other_tests;
use super::sorted_sets as sorted_sets_tests;

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

  #[test]
  fn it_should_expire_and_persist(){
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      keys_tests::should_expire_and_persist(client)
    });
  }

}


pub mod hashes {
  use super::*;

  #[test]
  fn it_should_set_and_get_simple_key() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      hashes_tests::should_set_and_get_simple_key(client)
    });
  }

  #[test]
  fn it_should_set_and_get_all_simple_key() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      hashes_tests::should_set_and_get_all_simple_key(client)
    });
  }

  #[test]
  fn it_should_check_hexists() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      hashes_tests::should_check_hexists(client)
    });
  }

  #[test]
  fn it_should_read_large_hash() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      hashes_tests::should_read_large_hash(client)
    });
  }

}

pub mod lists {
  use super::*;

  #[test]
  fn it_should_llen_on_empty_list() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      lists_tests::should_llen_on_empty_list(client)
    });
  }

  #[test]
  fn it_should_llen_on_list_with_elements() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      lists_tests::should_llen_on_list_with_elements(client)
    });
  }

  #[test]
  fn it_should_lpush_and_lpop_to_list() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      lists_tests::should_lpush_and_lpop_to_list(client)
    });
  }
}

pub mod sets {
  use super::*;

  #[test]
  fn it_should_sadd_members_to_set() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      sets_tests::should_sadd_members_to_set(client)
    });
  }

  #[test]
  fn it_should_srem_members_of_set() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      sets_tests::should_srem_members_of_set(client)
    });
  }

  #[test]
  fn it_should_smembers_of_set() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(),|client| {
      sets_tests::should_smembers_of_set(client)
    });
  }

}

pub mod pubsub {
  use super::*;

  #[test]
  fn it_should_psubscribe_to_multiple_channels() {
    let config = RedisConfig::default();
    utils::setup_two_test_clients(config, TIMER.clone(),|client_1, client_2| {
      pubsub_tests::should_psubscribe_on_multiple_channels(client_1, client_2)
    });
  }

  #[test]
  fn it_should_punsubscribe_to_multiple_channels() {
    let config = RedisConfig::default();
    utils::setup_two_test_clients(config, TIMER.clone(),|client_1, client_2| {
      pubsub_tests::should_punsubscribe_on_multiple_channels(client_1, client_2)
    });
  }

}

pub mod other {
  use super::*;

  #[test]
  fn it_should_scan_simple_database() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      other_tests::scan::should_scan_simple_database(client)
    });
  }

  #[test]
  fn it_should_hscan_simple_database() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      other_tests::hscan::should_hscan_simple_database(client)
    });
  }

  #[test]
  fn it_should_sscan_simple_database() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      other_tests::sscan::should_sscan_simple_database(client)
    })
  }

  #[test]
  fn it_should_zscan_simple_database() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      other_tests::zscan::should_zscan_simple_database(client)
    })
  }

}

pub mod sorted_sets {
  use super::*;

  #[test]
  fn it_should_add_and_remove_elements() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      sorted_sets_tests::basic::should_add_and_remove_elements(client)
    })
  }

  #[test]
  fn it_should_push_and_pop_min_max() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      sorted_sets_tests::basic::should_push_and_pop_min_max(client)
    })
  }

  #[test]
  fn it_should_read_sorted_lex_entries() {
    let config = RedisConfig::default_centralized();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      sorted_sets_tests::lex::should_read_sorted_lex_entries(client)
    })
  }

  #[test]
  fn it_should_read_sorted_score_entries() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      sorted_sets_tests::score::should_read_sorted_score_entries(client)
    })
  }

  #[test]
  fn it_should_perform_set_operations() {
    let config = RedisConfig::default();
    utils::setup_test_client(config, TIMER.clone(), |client| {
      sorted_sets_tests::set_ops::should_perform_set_operations(client)
    })
  }


}