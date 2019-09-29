# Examples

To run an example: 

```
cargo run --example basic
```

1. [Basic](basic.rs) - Basic usage including setting up an event loop, creating a client, connecting, and disconnecting.
2. [Reconnect](reconnect.rs) - How to use the built-in reconnection logic.
3. [Pubsub](pubsub.rs) - How to use the [publish-subscribe](https://redis.io/topics/pubsub) interface.
4. [Cluster](cluster.rs) - How to use with clustered Redis deployments.
5. [Multiple](multiple.rs) - An example that creates multiple clients on an event loop and composes their connections and commands together.
6. [TLS](tls.rs) - An example showing how to use the TLS features.