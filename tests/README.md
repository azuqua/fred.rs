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

## Travis CI

The travis configuration for this module does the following:

1. Download redis source with the version present in REDIS_VERSION, currently 3.2.9.
2. Build it, run it in centralized mode.
3. Build it again, run it in clustered mode according the defaults in the `src/utils/create-cluster/create-cluster` script.
4. Compile the module and tests in debug mode, then run them.
5. Compile the module and tests in release mode, then run them.
6. Turn off redis.
7. Optionally update the docs on the gh-pages branch.

See the [.travis.yml](../.travis.yml) file for more info.