//!
//!
//!

#![allow(unused_imports)]


extern crate futures_03 as futures;
// extern crate tokio_core;
//extern crate tokio_compat;
//extern crate tokio_proto;
extern crate bytes;
extern crate parking_lot;
extern crate url;
extern crate redis_protocol;
extern crate float_cmp;
extern crate tokio_timer_patched as tokio_timer;
// extern crate tokio_io;
extern crate rand;

#[macro_use]
extern crate lazy_static;

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

#[cfg(feature="mocks")]
mod mocks;

/// Error handling types.
pub mod error;
/// Configuration options, return value types, etc.
pub mod types;
/// The `RedisClient` implementation. See the `borrowed` and `owned` modules for most of the Redis command implementations.
pub mod client;
/// Size and latency metrics types.
pub mod metrics;
pub mod pool;

mod commands;
pub mod borrowed;
pub mod owned;

mod async_ng;

pub use client::RedisClient;

/// A helper module to re-export several common dependencies.
pub mod prelude {
  pub use crate::error::*;
  pub use crate::types::*;
  pub use crate::client::RedisClient;
}
