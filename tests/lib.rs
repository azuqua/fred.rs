#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(deprecated)]
#![allow(unused_macros)]

extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate redis_client;
extern crate rand;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

// this is a poor way of dealing with global mutable state
#[test]
fn init_test_logger() {
  pretty_env_logger::init().unwrap();
}

pub mod integration;
