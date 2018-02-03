Fred
====

[![Build Status](https://travis-ci.org/azuqua/fred.rs.svg?branch=master)](https://travis-ci.org/azuqua/fred.rs)
[![Crates.io](https://img.shields.io/crates/v/fred.svg)](https://crates.io/crates/fred)

[Documentation](https://docs.rs/fred/*/fred/)

A Redis client for Rust based on [Futures](https://github.com/alexcrichton/futures-rs) and [Tokio](https://tokio.rs/) that supports PubSub commands, clustered Redis deployments, and more.

## Install

With [cargo edit](https://github.com/killercup/cargo-edit).

```
cargo add fred
```

## Features

* Supports clustered Redis deployments.
* Optional built-in reconnection logic with multiple backoff policies.
* Publish-Subscribe interface.
* Optional `Send` and `Sync` wrappers for the client.
* Flexible interface for different use cases.
* 100% safe rust.

## Example

```rust
extern crate fred;
extern crate tokio_core;
extern crate futures;

use fred::RedisClient;
use fred::types::*;

use tokio_core::reactor::Core;
use futures::{
  Future,
  Stream
};

fn main() {
  let config = RedisConfig::default();

  let mut core = Core::new().unwrap();
  let handle = core.handle();

  println!("Connecting to {:?}...", config);
  
  let client = RedisClient::new(config);
  let connection = client.connect(&handle);
  
  let commands = client.on_connect().and_then(|client| {
    println!("Client connected.");
    
    client.select(0)
  })
  .and_then(|client| {
    println!("Selected database.");
    
    client.info(None)
  })
  .and_then(|(client, info)| {
    println!("Redis server info: {}", info);
    
    client.get("foo")
  })
  .and_then(|(client, result)| {
    println!("Got foo: {:?}", result);
    
    client.set("foo", "bar", Some(Expiration::PX(1000)), Some(SetOptions::NX))
  })
  .and_then(|(client, result)| {
    println!("Set 'bar' at 'foo'? {}.", result);
    
    client.quit()
  });

  let (reason, client) = match core.run(connection.join(commands)) {
    Ok((r, c)) => (r, c),
    Err(e) => panic!("Connection closed abruptly: {}", e) 
  };

  println!("Connection closed gracefully with error: {:?}", reason);
}
```

See [examples](https://github.com/azuqua/fred.rs/tree/master/examples) for more.

## Redis Cluster

Clustered Redis deployments are supported by this module by specifying a `RedisConfig::Clustered` variant when using `connect` or `connect_with_policy`. When creating a clustered configuration only one valid host from the cluster is needed, regardless of how many nodes exist in the cluster. When the client first connects to a node it will use the `CLUSTER NODES` command to inspect the state of the cluster.

In order to simplify error handling and usage patterns this module caches the state of the cluster in memory and maintains connections to each master node in the cluster. When a command is received the client hashes the key or [key hash tag](https://redis.io/topics/cluster-spec#keys-hash-tags) to find the node that should receive the request and then dispatches the request to that node. In the event that a node returns a `MOVED` or `ASK` error the client will pause to rebuild the in-memory cluster state. When the local cluster state and new connections have been fully rebuilt the client will begin processing commands again. 

## Logging

This module uses [pretty_env_logger](https://github.com/seanmonstar/pretty-env-logger) for logging. To enable logs use the environment
variable `RUST_LOG` with a value of `trace`, `debug`, `error`, or `info`. See the documentation for [env_logger](http://rust-lang-nursery.github.io/log/env_logger/) for more information. 

## Features

|  Name   | Default | Description                                                     |
|---------|---------|-----------------------------------------------------------------|
| sync    |    x    | Enable the `Send` and `Sync` wrappers.                          |
| fuzz    |         | Expose the protocol utils as public functions for fuzz testing. |
| metrics |    x    | Enable latency and payload size metrics.                        |

## Tests

To run the unit and integration tests:

```
cargo test -- --test-threads=1
```

Note a local Redis server must be running on port 6379 and a clustered deployment must be running on ports 30000 - 30005 for the integration tests to pass.

**Beware: the tests will periodically run `flushall`.**

## TODO

* More commands.
* Blocking commands.
* Distribute reads among slaves.
* Pipelined requests.
* Lua.
* Change the protocol parsing to use [nom](https://github.com/Geal/nom).