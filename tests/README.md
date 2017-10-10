Testing
=======

To run the tests:

```bash
cargo test -- --test-threads=1
```

A local Redis server must be running on 6379 and a clustered deployment must be running on ports 30000-30005 for the integration tests to pass.

**Beware: the tests will periodically run `flushall`.**

To see detailed debug logs:

```bash
RUST_LOG=fred=trace cargo test -- --test-threads=1
```

See the [contributing](../CONTRIBUTING.md) docs for more info on the tests.