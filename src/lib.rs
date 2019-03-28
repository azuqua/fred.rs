//!
//!
//!

#![allow(unused_imports)]


extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate bytes;
extern crate parking_lot;
extern crate url;
extern crate redis_protocol;
extern crate float_cmp;
extern crate tokio_timer_patched as tokio_timer;
extern crate tokio_io;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

#[cfg(feature="enable-tls")]
extern crate native_tls;
#[cfg(feature="enable-tls")]
extern crate tokio_tls;

#[macro_use]
mod utils;
mod protocol;
mod multiplexer;

/// Error handling types.
pub mod error;
/// Configuration options, return value types, etc.
pub mod types;
/// The `RedisClient` implementation.
pub mod client;
/// Size and latency metrics types.
pub mod metrics;

mod commands;
pub mod borrowed;
pub mod owned;

pub use client::RedisClient;

/// A helper module to re-export several common dependencies.
pub mod prelude {
  pub use crate::error::*;
  pub use crate::types::*;
  pub use crate::client::RedisClient;
}



















