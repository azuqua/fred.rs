//! A module to pool multiple Redis clients via one interface that will round-robin requests among clients in the pool.

use crate::error::*;
use crate::RedisClient;
use crate::types::*;
use crate::utils;
use crate::client::ConnectionFuture;
use azuqua_core_async::*;

use futures::{
  Future,
  Stream,
  future
};
use tokio_timer::Timer;

use std::sync::Arc;
use std::ops::{
  DerefMut,
  Deref
};
use std::borrow::{
  Borrow,
  BorrowMut
};
use std::convert::AsRef;
use std::sync::atomic::{
  AtomicUsize,
  Ordering
};
use std::slice::Iter;

use std::fmt;

fn decr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_sub(1, Ordering::AcqRel).saturating_sub(1)
}

fn incr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
}

fn read_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.load(Ordering::Acquire)
}

fn set_atomic(size: &Arc<AtomicUsize>, val: usize) -> usize {
  size.swap(val, Ordering::AcqRel)
}

/// A module to pool multiple Redis clients together into one interface that will round-robin requests among clients. `RedisClient` functions can be called on the pool as if it were an individual client.
#[derive(Clone)]
pub struct RedisPool {
  clients: Arc<Vec<RedisClient>>,
  last: Arc<AtomicUsize>,
}

impl fmt::Display for RedisPool {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Redis Pool]")
  }
}

/// The result of creating a new `RedisPool` from a `RedisConfig`.
pub struct NewRedisPool {
  /// The `RedisPool` created from `RedisPool::new` or `RedisPool::new_with_policy`.
  pub pool: RedisPool,
  /// A future running the connections for each client in the pool.
  pub connections: ConnectionFuture
}

impl RedisPool {

  /// Create a new pool with `size` clients from a `RedisConfig`.
  ///
  /// Note: The clients will not initiate a connection until the `connections` future from the returned `NewRedisPool` has started running on an event loop.
  pub fn new(spawner: &Spawner, config: RedisConfig, size: usize, timer: Option<Timer>) -> Result<NewRedisPool, RedisError> {
    if size < 1 {
      return Err(RedisError::new(
        RedisErrorKind::Unknown, "Client pool size must be > 0."
      ));
    }
    let timer = timer.unwrap_or(Timer::default());

    let clients = Vec::with_capacity(size);
    let memo = (spawner.clone(), config, timer, clients, utils::future_ok(None));

    let (_, _, _, clients, connections) = (0..size).fold(memo, |(spawner, config, timer, mut clients, connections), _| {
      let client = RedisClient::new(config.clone(), Some(timer.clone()));
      let connections = Box::new(client.connect(&spawner).join(connections).map(|error| {
        // errors are given priority in the same order that clients are initialized
        // since all clients connect to the same server it's likely that they'd all hit the same error anyways
        match error {
          (Some(e1), Some(e2)) => Some(e1),
          (Some(e1), None)     => Some(e1),
          (None, Some(e2))     => Some(e2),
          (None, None)         => None
        }
      }));

      clients.push(client);
      (spawner, config, timer, clients, connections)
    });

    Ok(NewRedisPool {
      connections,
      pool: RedisPool {
        clients: Arc::new(clients),
        last: Arc::new(AtomicUsize::new(0))
      }
    })
  }

  /// Create a new pool with `size` clients from a `RedisConfig`, applying the reconnect policy from `policy` to each client.
  ///
  /// Note: The clients will not initiate a connection until the `connections` future from the returned `NewRedisPool` has started running on an event loop.
  pub fn new_with_policy(spawner: &Spawner, config: RedisConfig, policy: ReconnectPolicy, size: usize, timer: Option<Timer>) -> Result<NewRedisPool, RedisError> {
    if size < 1 {
      return Err(RedisError::new(
        RedisErrorKind::Unknown, "Client pool size must be > 0."
      ));
    }
    let timer = timer.unwrap_or(Timer::default());

    let clients = Vec::with_capacity(size);
    let memo = (spawner.clone(), config, policy, timer, clients, utils::future_ok(None));

    let (_, _, _, _, clients, connections) = (0..size).fold(memo, |(spawner, config, policy, timer, mut clients, connections), _| {
      let client = RedisClient::new(config.clone(), Some(timer.clone()));
      let connections = Box::new(client.connect_with_policy(&spawner, policy.clone()).join(connections).map(|error| {
        // errors are given priority in the same order that clients are initialized
        // since all clients connect to the same server it's likely that they'd all hit the same error anyways
        match error {
          (Some(e1), Some(e2)) => Some(e1),
          (Some(e1), None)     => Some(e1),
          (None, Some(e2))     => Some(e2),
          (None, None)         => None
        }
      }));

      clients.push(client);
      (spawner, config, policy, timer, clients, connections)
    });

    Ok(NewRedisPool {
      connections,
      pool: RedisPool {
        clients: Arc::new(clients),
        last: Arc::new(AtomicUsize::new(0))
      }
    })
  }

  /// Create a pool from existing clients.
  ///
  /// This is especially useful if the clients were created separately on different event loop threads.
  pub fn from_clients(clients: Vec<RedisClient>) -> Result<RedisPool, RedisError> {
    if clients.len() < 1 {
      return Err(RedisError::new(
        RedisErrorKind::Unknown, "Client pool size must be > 0."
      ));
    }

    Ok(RedisPool {
      clients: Arc::new(clients),
      last: Arc::new(AtomicUsize::new(0))
    })
  }

  /// Read the size of the pool.
  pub fn len(&self) -> usize {
    self.clients.len()
  }

  /// A reference to the client that should run the next command.
  pub fn next(&self) -> &RedisClient {
    &self.clients[incr_atomic(&self.last) % self.clients.len()]
  }

  /// A reference to the client that ran the last command.
  pub fn last(&self) -> &RedisClient {
    &self.clients[read_atomic(&self.last) % self.clients.len()]
  }

  /// Attempt to unwrap the inner client array, assuming the checks from `Arc::try_unwrap` all pass.
  pub fn take(self) -> Result<Vec<RedisClient>, Arc<Vec<RedisClient>>> {
    Arc::try_unwrap(self.clients)
  }

  /// An iterator over the inner client array.
  pub fn iter(&self) -> Iter<RedisClient> {
    self.clients.iter()
  }

  /// Create a pool from a shared reference to a client array.
  ///
  /// This is primarily for when `take` fails to unwrap the inner client array.
  pub fn from_shared(clients: Arc<Vec<RedisClient>>) -> RedisPool {
    RedisPool {
      clients,
      last: Arc::new(AtomicUsize::new(0))
    }
  }

}

impl Deref for RedisPool {
  type Target = RedisClient;

  fn deref(&self) -> &RedisClient {
    self.next()
  }
}

impl Borrow<RedisClient> for RedisPool {
  fn borrow(&self) -> &RedisClient {
    self.next()
  }
}

impl<'a> From<&'a RedisPool> for &'a RedisClient {
  fn from(p: &'a RedisPool) -> &'a RedisClient {
    p.next()
  }
}

impl<'a> From<&'a RedisPool> for RedisClient {
  fn from(p: &'a RedisPool) -> RedisClient {
    p.next().clone()
  }
}

