#![allow(unused_variables)]
#![allow(unused_imports)]

//! Same idea as `sync_owned.rs`, but shows how the removal of ownership requirements on commands can make using this much easier.

extern crate redis_client;
extern crate tokio_core;
extern crate tokio_timer;
extern crate futures;

use redis_client::RedisClient;
use redis_client::sync::borrowed::RedisClientRemote;
use redis_client::types::*;
use redis_client::error::*;

use tokio_core::reactor::Core;
use futures::Future;
use futures::stream::{
  self,
  Stream
};
use futures::future::{
  self,
  Either
};

use tokio_timer::Timer;
use std::time::Duration;
use std::thread;

const KEY: &'static str = "bar";

fn main() {
  let sync_client = RedisClientRemote::new();

  let t_sync_client = sync_client.clone();
  // create an event loop in a separate thread and run the client there
  let event_loop_jh = thread::spawn(move || {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let config = RedisConfig::default();
    let client = RedisClient::new(config);

    // future that runs the underlying connection
    let connection = client.connect(&handle);

    // future that runs the remote interface
    let remote = t_sync_client.init(client);

    let composed = connection.join(remote);
    let _ = core.run(composed).unwrap();
  });

  // block this thread until the underlying client is connected
  let _ = sync_client.on_connect().wait();

  // create two threads, one to increment a value every second, and a second to read the same value every second
  let reader_client = sync_client.clone();
  let reader_jh = thread::spawn(move || {

    // since ownership requirements are removed on commands the client can be more easily used inside loops, FnMut, etc
    loop {
      let value = match reader_client.get(KEY).wait() {
        Ok(v) => v,
        Err(e) => {
          println!("Error reading key: {:?}", e);
          break;
        }
      };

      if let Some(value) = value {
        println!("Reader read key {:?} with value {:?}", KEY, value);
      }

      thread::sleep(Duration::from_millis(1000));
    }
  });

  let writer_client = sync_client.clone();
  let writer_jh = thread::spawn(move || {
    loop {
      let value = match writer_client.incr(KEY).wait() {
        Ok(v) => v,
        Err(e) => {
          println!("Error incrementing key: {:?}", e);
          break;
        }
      };
      println!("Writer incremented key {:?} to {:?}", KEY, value);

      thread::sleep(Duration::from_millis(1000));
    }
  });

  // block this thread while the three child threads run
  let _ = event_loop_jh.join();
  let _ = reader_jh.join();
  let _ = writer_jh.join();
}