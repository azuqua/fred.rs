# Examples

To run an example: 

```
cargo run --example basic
```

1. [Basic](basic.rs) - Basic usage including setting up an event loop, creating a client, connecting, and disconnecting.
2. [Reconnect](reconnect.rs) - How to use the built-in reconnection logic.
3. [Pubsub](pubsub.rs) - How to use the [publish-subscribe](https://redis.io/topics/pubsub) interface.
4. [Cluster](cluster.rs) - How to use with clustered Redis deployments.
5. [Sync Owned](sync_owned.rs) - Examples showing how to wrap a `RedisClient` with a `RedisClientRemote` in order to issue commands from another thread.
6. [Sync Borrowed](sync_borrowed.rs) - Examples showing how to create a `RedisClientRemote` that borrows `self` on each command, allowing for greater control over ownership.
7. [Multiple](multiple.rs) - An example that creates multiple clients on an event loop and composes their connections and commands together.
8. [Http](http.rs) - An example using a Redis client with a [hyper http server](https://github.com/hyperium/hyper).