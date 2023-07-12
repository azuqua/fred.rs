# Notice

This repository has moved to a [new location](https://github.com/aembke/fred.rs). The crates.io location will not change, so nothing needs to change in your toml if you're using that.

The [newest version](https://crates.io/crates/fred/3.0.0) is a complete rewrite on async/await, new tokio, etc. It also includes a number of new features and breaking changes to the API. See the new repository for more information. 

Changes to the 2.x major version will be made here, if necessary, but all other development will happen in the new repository. When the 2.x major version is fully deprecated this repository will be archived. 

Fred
====

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/fred.svg)](https://crates.io/crates/fred)
[![API docs](https://docs.rs/fred/badge.svg)](https://docs.rs/fred)

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
* Supports ElastiCache, including TLS support.
* Gracefully handle live cluster rebalancing operations.
* Flexible interfaces for different use cases.
* Supports various scanning functions.
* Automatically retry requests under bad network conditions.
* Built-in tracking for network latency and payload size metrics.
* Built-in mocking layer for running tests without a Redis server.
* A client pooling interface to round-robin requests among a pool of connections.

## Example

```rust
extern crate fred;
extern crate tokio_core;
extern crate futures;

use fred::RedisClient;
use fred::owned::RedisClientOwned;
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
  
  let client = RedisClient::new(config, None);
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

See [examples](https://github.com/atko-flow/fred.rs/tree/master/examples) for more.

## Redis Cluster

Clustered Redis deployments are supported by this module by specifying a `RedisConfig::Clustered` variant when using `connect` or `connect_with_policy`. When creating a clustered configuration only one valid host from the cluster is needed, regardless of how many nodes exist in the cluster. When the client first connects to a node it will use the `CLUSTER NODES` command to inspect the state of the cluster.

In order to simplify error handling and usage patterns this module caches the state of the cluster in memory and maintains connections to each node in the cluster. In the event that a node returns a `MOVED` or `ASK` error the client will pause to rebuild the in-memory cluster state. When the local cluster state and new connections have been fully rebuilt the client will begin processing commands again. Any requests sent while the in-memory cache is being rebuilt will be queued up and replayed when the connection is available again. 

Additionally, this module will not acknowledge requests as having finished until a response arrives, so in the event that a connection dies while a request is in flight it will be retried multiple times (configurable via features below) when the connection comes back up.

## Logging

This module uses [pretty_env_logger](https://github.com/seanmonstar/pretty-env-logger) for logging. To enable logs use the environment
variable `RUST_LOG` with a value of `trace`, `debug`, `warn`, `error`, or `info`. See the documentation for [env_logger](http://rust-lang-nursery.github.io/log/env_logger/) for more information. 

When a client is initialized it will generate a unique client name with a prefix of `fred-`. This name will appear in nearly all logging statements on the client in order to associate client and server operations if logging is enabled on both.

## Elasticache and occasional NOAUTH errors

When using Amazon Elasticache and Redis auth, NOAUTH errors can occasionally occur even though the client has previously authenticated during the session.  This prevents further commands from being processed successfully until the connection is rebuilt.  While the exact cause of the NOAUTH response from Elasticache is not fully known, it can be worked around by using the `reconnect-on-auth-error` compiler feature.  When enabled, NOAUTH errors are treated similarly to other general connection errors and if a reconnection policy is configured then the client will automatically rebuild the connection, re-auth and continue on with the previous command.  By default this behaviour is not enabled.

## Features

|    Name                     | Default | Description                                                                                                                                                          |
|---------------------------- |---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enable-tls                  |         | Enable TLS support. This requires OpenSSL (or equivalent) dependencies.                                                                                              |
| ignore-auth-error           |    x    | Ignore auth errors that occur when a password is supplied but not required.                                                                                          |
| reconnect-on-auth-error     |         | A NOAUTH error is treated the same as a general connection failure and the client will reconnect based on the reconnection policy.                                                 |
| mocks                       |         | Enable the mocking layer, which will use local memory instead of an actual redis server.                                                                             |
| super-duper-bad-networking  |         | Increase the number of times a request will be automatically retried from 3 to 20. A request is retried when the connection closes while waiting on a response.      |

## Environment Variables

|   Name                            | Default | Description                                                                              |
|-----------------------------------|---------|------------------------------------------------------------------------------------------|
| FRED_DISABLE_CERT_VERIFICATION    | `false` | Disable certificate verification when using TLS features.                                |


## Tests

To run the unit and integration tests:

```
cargo test -- --test-threads=1
```

Note a local Redis server must be running on port 6379 and a clustered deployment must be running on ports 30001 - 30006 for the integration tests to pass.

**Beware: the tests will periodically run `flushall`.**

## TODO

* Expand the mocking layer to support all commands.
* More commands.
* Blocking commands.
* Distribute reads among slaves.
* Transactions.
* Lua.

## Contributing

See the [contributing](CONTRIBUTING.md) documentation for info on adding new commands. For anything more complicated feel free to file an issue and we'd be happy to point you in the right direction. 
