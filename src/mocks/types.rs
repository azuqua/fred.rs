#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::error::*;
use crate::types::*;

use std::fmt;

use crate::utils as client_utils;
use super::utils;

use crate::protocol::types::{
  RedisCommand,
  RedisCommandKind
};

use std::collections::{
  VecDeque,
  BinaryHeap,
  HashMap,
  BTreeMap,
  BTreeSet
};

use std::cmp::{
  Ord,
  Ordering,
  PartialOrd
};

use std::rc::Rc;
use std::cell::RefCell;

use std::borrow::{
  Borrow,
  BorrowMut
};

use tokio_core::reactor::Handle;

use futures::{
  Future,
  Stream
};
use std::time::Instant;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpireLog {
  /// Timestamp of when to clean up, in ms.
  pub after: Instant,
  /// Timestamp of set operation, reference to the key. This is set by the library.
  pub internal: Option<(Instant, Arc<RedisKey>)>
}

impl ExpireLog {

  pub fn set_internal(&mut self, set: Instant, key: &Arc<RedisKey>) {
    self.internal = Some((set, key.clone()));
  }

  pub fn has_internal(&self) -> bool {
    self.internal.is_some()
  }

  pub fn get_set_time(&self) -> Option<&Instant> {
    match self.internal {
      Some((ref set, _)) => Some(set),
      None => None
    }
  }

  pub fn get_key(&self) -> Option<&Arc<RedisKey>> {
    match self.internal {
      Some((_, ref key)) => Some(key),
      None => None
    }
  }

}

impl Default for ExpireLog {

  fn default() -> ExpireLog {
    ExpireLog {
      after: Instant::now(),
      internal: None
    }
  }

}

impl Ord for ExpireLog {
  fn cmp(&self, other: &ExpireLog) -> Ordering {
    self.after.cmp(&other.after)
  }
}

impl PartialOrd for ExpireLog {

  fn partial_cmp(&self, other: &ExpireLog) -> Option<Ordering> {
    Some(self.cmp(other))
  }

}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyType {
  Data,
  Map,
  List,
  Set
}

impl fmt::Display for KeyType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

/// Uses a map of "dirty" logs to batch up slower operations on the heap.
#[derive(Debug, Clone)]
pub struct Expirations {
  pub expirations: BTreeMap<Arc<RedisKey>, Arc<ExpireLog>>,
  pub sorted: BinaryHeap<Arc<ExpireLog>>,
  pub dirty: BTreeMap<Arc<RedisKey>, Arc<ExpireLog>>
}

impl Expirations {

  pub fn new() -> Expirations {
    Expirations {
      expirations: BTreeMap::new(),
      sorted: BinaryHeap::new(),
      dirty: BTreeMap::new()
    }
  }

  /// Add or update an expire log in the data set.
  pub fn add(&mut self, key: &Arc<RedisKey>, mut expiration: ExpireLog) -> Result<(), RedisError> {
    if !expiration.has_internal() {
      expiration.set_internal(Instant::now(), key);
    }

    let expiration = Arc::new(expiration);

    if let Some(old) = self.expirations.insert(key.clone(), expiration.clone()) {
      // move old value to deleted set for lazy deletion later
      self.dirty.insert(key.clone(), old);
    };

    let added = match self.expirations.get(key) {
      Some(added) => added,
      None => return Err(RedisError::new(
        RedisErrorKind::Unknown, "Error adding expiration log."
      ))
    };
    self.sorted.push(added.clone());

    Ok(())
  }

  pub fn del(&mut self, key: &Arc<RedisKey>) -> Result<usize, RedisError> {
    let old = match self.expirations.remove(key) {
      Some(old) => old,
      None => return Ok(0)
    };
    self.dirty.insert(key.clone(), old);

    Ok(1)
  }

  pub fn dirty_logs(&self) -> usize {
    self.dirty.len()
  }

  pub fn find_expired(&mut self) -> Vec<Arc<ExpireLog>> {
    let now = Instant::now();
    let mut out: Vec<Arc<ExpireLog>> = Vec::new();

    while self.sorted.len() > 0 {
      let youngest = match self.sorted.pop() {
        Some(y) => y,
        None => break
      };

      if !youngest.has_internal() {
        continue;
      }

      if youngest.after < now {
        // pop it off the heap either way
        let youngest_set_time = match youngest.get_set_time() {
          Some(s) => s,
          None => continue // skip it, shouldn't be possible
        };
        let youngest_key = match youngest.get_key() {
          Some(k) => k,
          None => continue
        };

        if let Some(saved_expire) = self.expirations.remove(youngest_key) {
          let saved_expire_set_time = match saved_expire.get_set_time() {
            Some(s) => s,
            None => continue
          };

          if saved_expire_set_time == youngest_set_time {
            out.push(saved_expire.clone());
          }else{
            // put it back, it's a later expiration on the same key
            self.expirations.insert(youngest_key.clone(), saved_expire);
          }
          // ignore if the key was updated with a later expiration
        }

        self.dirty.remove(youngest_key);
      }else{
        // put it back
        self.sorted.push(youngest);
        break;
      }
    }

    out
  }

  // do a full pass over the binary heap to remove things from the `dirty` map
  pub fn cleanup(&mut self) {
    let mut new_sorted: BinaryHeap<Arc<ExpireLog>> = BinaryHeap::new();

    for expire in self.sorted.drain() {
      let expire_key = match expire.get_key() {
        Some(k) => k,
        None => continue
      };
      let expire_set_time = match expire.get_set_time() {
        Some(s) => s,
        None => continue
      };

      if let Some(dirty) = self.dirty.remove(expire_key) {
        let dirty_set_time = match dirty.get_set_time() {
          Some(s) => s,
          None => continue
        };

        if dirty_set_time != expire_set_time {
          new_sorted.push(expire.clone());
        }
        // don't put back in sorted queue
      }else{
        new_sorted.push(expire.clone());
      }
    }

    self.sorted = new_sorted;
  }

}

pub struct DataSet {
  pub keys: BTreeSet<Arc<RedisKey>>,
  pub key_types: BTreeMap<Arc<RedisKey>, KeyType>,
  pub data: BTreeMap<Arc<RedisKey>, RedisValue>,
  pub maps: BTreeMap<Arc<RedisKey>, BTreeMap<Arc<RedisKey>, RedisValue>>,
  pub sets: BTreeMap<Arc<RedisKey>, BTreeSet<RedisKey>>,
  pub lists: BTreeMap<Arc<RedisKey>, VecDeque<RedisValue>>,
  pub expirations: Arc<RwLock<Expirations>>,
}

impl Default for DataSet {

  fn default() -> Self {
    DataSet {
      keys: BTreeSet::new(),
      key_types: BTreeMap::new(),
      data: BTreeMap::new(),
      maps: BTreeMap::new(),
      sets: BTreeMap::new(),
      lists: BTreeMap::new(),
      expirations: Arc::new(RwLock::new(Expirations::new()))
    }
  }

}


