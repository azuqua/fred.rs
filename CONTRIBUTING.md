# General
* Use 2 spaces instead of tabs.
* Use `utils::future_ok` and `utils::future_error` to return boxed futures around non-future types.

# Adding Commands

In order to illustrate how to add new commands to the client we'll use the `set` command as an example.

First, implement the command in the `src/commands.rs` file.

```rust
pub fn set<K: Into<RedisKey>, V: Into<RedisValue>>(inner: &Arc<RedisClientInner>, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>> {
  let (key, value) = (key.into(), value.into());

  Box::new(utils::request_response(inner, move || {
    let mut args = vec![key.into(), value];

    if let Some(expire) = expire {
      let (k, v) = expire.into_args();
      args.push(k.into());
      args.push(v.into());
    }
    if let Some(options) = options {
      args.push(options.to_string().into());
    }

    Ok((RedisCommandKind::Set, args))
  }).and_then(|frame| {
    let resp = protocol_utils::frame_to_single_result(frame)?;

    Ok(resp.kind() != RedisValueKind::Null)
  }))
}
```

A few things to note:
* The first argument is always a `&Arc<RedisClientInner>`. This struct contains all the state necessary to implement any functionality on the client.
* The error type is always `RedisError`.
* Argument types are usually `Into<T>`, allowing the caller to input any type that can be converted into the required type.
* Contrary to previous versions of this module, the returned future does not contain `Self`.
* The `request_response` utility function is used, which handles all the message passing between the client and socket. The closure provided runs before the command is executed and must return a `RedisCommandKind` and a list of arguments corresponding to the ordered set of arguments to be passed to the server.
* Any `Into<T>` types are converted to `T` before moving them into the `request_response` closure. This is necessary because `Into` is a trait and therefore is not `Sized`.
* The `key` argument is converted twice, once from a semi-arbitrary source type to a `RedisKey`, and a second time from a `RedisKey` into a `RedisValue`. Any arguments that are specified as `Into<RedisKey>` must use this pattern as the list of arguments passed to the server must all be `RedisValue`'s.
* The future returned by `request_response` returns a raw `Frame` from the [redis_protocol](https://github.com/aembke/redis-protocol.rs) library. This must be converted to a single result or list of results before being coerced and handed back to the caller. Use the `protocol_utils::frame_to_single_result` or `protocol_utils::frame_to_results` to do this. For commands that return a single result use the former, otherwise use the latter.
* In this case `set` does not return a generic `RedisValue`, instead it's designed to tell the caller whether or not the key was actually set or not. For this reason the function here returns a `bool` so that the caller does not have to coerce anything. Depending on the command being implemented you may want to return a more helpful type than a generic `RedisValue` in order to make life easy for the caller.

Next, implement a wrapper function in the `src/borrowed.rs` file.

First implement the function scaffolding in the `RedisClientBorrowed` trait definition.

```rust
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>>;
```

Next implement a small wrapper function in the actual implementation block for the `RedisClient`.

```rust
  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// <https://redis.io/commands/set>
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(&self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=bool, Error=RedisError>> {
    commands::set(&self.inner, key, value, expire, options)
  }
```

A things to note:
* Documentation is provided in the actual implementation block, not in the trait definition.
* Documentation contains a link to the Redis documentation for the associated command.
* The `RedisClient` implementation just calls the function you implemented above, with the same arguments in the same order.

Finally, implement another wrapper function in the `src/owned.rs` file, but with a few modifications called out in the notes below.

First implement the function scaffolding in the `RedisClientOwned` trait definition again.

```rust
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=(Self, bool), Error=RedisError>>;
```

Then implement the actual function in the `RedisClient` block below.

```rust
  /// Set a value at `key` with optional NX|XX and EX|PX arguments.
  /// The `bool` returned by this function describes whether or not the key was set due to any NX|XX options.
  ///
  /// <https://redis.io/commands/set>
  fn set<K: Into<RedisKey>, V: Into<RedisValue>>(self, key: K, value: V, expire: Option<Expiration>, options: Option<SetOptions>) -> Box<Future<Item=(Self, bool), Error=RedisError>> {
    run_borrowed(self, |inner| commands::set(inner, key, value, expire, options))
  }
```

A few more things to note:
* The same documentation is present as found in the `RedisClientBorrowed` implementation.
* This function takes ownership over `self` and returns `Self` as the first value in the tuple returned when the future resolves.
* The `run_borrowed` utility function is used to remove some boilerplate moving `self` into the callback function. This utility function, and its relative `run_borrowed_empty`, make it easy to move `self` into the returned future while only requiring that the underlying command function be specified. For commands that return a single result use `run_borrowed`, and for comamnds that do not return a result, or return an empty tuple, use `run_borrowed_empty`.

That's it.

# Integration Tests

The tests can be found inside `tests/integration`, and are separated between centralized and clustered tests, and separated further by the category of the command (hashes, lists, sets, pubsub, etc). The separation is designed to make it as easy as possible to perform the same test on both centralized and clustered deployments, and as a result you will need to implement the test once, and then wrap it twice to be called from the centralized and clustered wrappers.

Using `hget` as an example:

1 - Add a test in the `tests/integration/hashes/mod.rs` file for this. Put the test in whichever directory handles that category of commands.

```rust
pub fn should_set_and_get_simple_key(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.hset("foo", "bar", "baz").and_then(|(client, _)| {
    client.hget("foo", "bar")
  })
  .and_then(|(client, val)| {
    let val = match val {
      Some(v) => v,
      None => panic!("Expected value for foo not found.")
    };

    assert_eq!(val.into_string().unwrap(), "baz");
    client.hdel("foo", "bar")
  })
  .and_then(|(client, count)| {
    assert_eq!(count, 1);
    client.hget("foo", "bar")
  })
  .and_then(|(client, val)| {
    assert!(val.is_none());
    Ok(())
  }))
}
```

A few things to note: 

* The test function follows a naming convention of `should_do_something`.
* This function does *not* have a `#[test]` declaration above it.
* All tests return a `Box<Future<Item=(), Error=RedisError>>`.
* All tests are given their `RedisClient` instance as an argument, they don't create it.
* Tests should panic when fatal errors are encountered.

2 - Add a wrapper for the test in `tests/integration/centralized.rs`.

```rust
#[test]
fn it_should_set_and_get_simple_key() {
  let config = RedisConfig::default();
  utils::setup_test_client(config, TIMER.clone(), |client| {
    hashes_tests::should_set_and_get_simple_key(client)
  });
}
```

Note:

* You may need to create an inlined wrapping module for the command category if one doesn't already exist.
* This function does have a `#[test]` declaration above it.
* This function uses the shared static `TIMER` variable declared at the top of the file.
* A centralized config is used.
* The `utils::setup_test_client` function is used to create the test client and manage the result of the test function created above.
* The inner function just runs the test created above in step 1.

3 - Add another wrapper for the test in `tests/integration/cluster.rs`.

```rust
#[test]
fn it_should_set_and_get_simple_key() {
  let config = RedisConfig::default_clustered();
  utils::setup_test_client(config, TIMER.clone(), |client| {
    hashes_tests::should_set_and_get_simple_key(client)
  });
}
```

The clustered test is identical to the centralized test, but uses a clustered config instead of a centralized one.

That's it. Make sure a centralized redis instance and a clustered deployment are running on the default ports, then `cargo test -- --test-threads=1` or `RUST_LOG=fred=debug cargo test -- --test-threads=1 --nocapture` to see the results.

If you're having trouble getting redis installed check out the `tests/scripts/install_redis_centralized.sh` and `tests/scripts/install_redis_clustered.sh` to see how the CI tool installs them.

## Final Note

Unless you're extremely careful with the keys you use in your tests you are likely to see failing tests due to key collisions. By default cargo runs tests using several threads and in a non-deterministic order, and when reading and writing to shared state, such as a Redis server, you're likely to see race conditions. As a result you probably want to run your tests with the `--test-threads=1` argv.

```
RUST_LOG=fred=debug cargo test -- --test-threads=1
```