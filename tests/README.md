Testing
=======

To run the tests:

```bash
cargo test
```

A local Redis server must be running on 6379 and a clustered deployment must be running on ports 30000-30005 for the integration tests to pass.

To see detailed debug logs:

```bash
RUST_LOG=redis_client=trace cargo test -- --nocapture
```