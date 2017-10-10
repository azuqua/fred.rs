# Contributing

* [Use 2 spaces for tabs.](https://www.youtube.com/watch?v=kQFKtI6gn9Y)
* Don't use [boxed](https://github.com/alexcrichton/futures-rs/issues/228).
* Use `?` over `try!()` unless the inner expression would need to be wrapped in `()` to be legible, then just use `try!()`.
* Wrap any acquired locks in their own scope to reduce the size of critical sections.
* Don't ever hold a lock (move a guard) into a future callback.
* Command functions should not need to acquire a write lock or mutex.

## Adding Commands

Most commands, with the exception of blocking commands, can be implemented relatively quickly. Utility functions exist to handle all the message passing semantics between the client and multiplexer, and for the `sync` wrappers utilities exist to handle all the inter-thread communication.

The following steps will use the `hget` command as an example.

1 - Implement the function on the `RedisClient` struct in `src/lib.rs`.

```rust
/// Returns the value associated with field in the hash stored at key.
///
/// https://redis.io/commands/hget
pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>> (self, key: K, field: F) -> Box<Future<Item=(RedisClient, Option<RedisValue>), Error=RedisError>> {
  let key = key.into();
  let field = field.into();
  
  Box::new(utils::request_response(&self.command_tx, &self.state, move || {
    let args: Vec<RedisValue> = vec![key.into(), field.into()];
  
    Ok((RedisCommandKind::HGet, args))
  }).and_then(|frame| {
    let resp = frame.into_single_result()?;
  
    match resp {
      RedisValue::Null => Ok((self, None)),
      _ => Ok((self, Some(resp)))
    }
  }))
}
```

A few things to note:

* Documentation is taken from the [Redis docs](https://redis.io/commands).
* A link to the actual docs is included.
* At the moment command futures are wrapped in a `Box`. When [impl Trait](https://github.com/rust-lang/rust/issues/34511) lands this module will need many, many changes, but until then just use boxes.
* Arguments with generic types use the `Into` trait. Anything that is used as a key uses `Into<RedisKey>`, and anything that is used as a value uses `Into<RedisValue>`. If the underlying command can only use one type then it's best to use the equivalent Rust type directly, instead of doing a runtime check on the given variant of `RedisValue`. See the `incr` command for an example of this.
* Calls to `into()` on the outer arguments occur before the `request_response` call.
* The command enum used to communicate with the multiplexer uses a `Vec<RedisValue>` to describe arguments, making it necessary to convert `Into<RedisKey>` arguments twice: once to convert them into a `RedisKey` and a second time to convert the `RedisKey` into a `RedisValue`.
* For commands that can take 1 or more keys (`del`, `hmget`, etc) `Into<MultipleKeys>` is used.
* All commands take ownership over `self`, and `move` it into the final callback. This makes it easy to chain commands together. The client instance should always be the first value in the returned tuple.
* The final callback from `request_response` is used to coerce a `Frame` into a `RedisValue`, or whatever is returned by the outer future. `frame.into_single_result()` can be used to convert a frame into a single `RedisValue`, and `frame.into_results()` can be used to convert a frame into a `Vec<RedisValue>`. The `into_single_result` function will return an error if called on a frame that contains multiple `RedisValues`.

2 - Implement the same function for the borrowed `Send` and `Sync` wrapper.

First add a small wrapper in `src/sync/commands.rs`.

```rust
pub fn hget(client: RedisClient, tx: OneshotSender<Result<Option<RedisValue>, RedisError>>, key: RedisKey, field: RedisKey) -> CommandFnResp {
  Box::new(client.hget(key, field).then(move |result| {
    utils::send_normal_result(tx, result)
  }))
}
```

A few things to note:

* The first two arguments, `client` and `tx`, are used on all command wrappers.
* The value inside the `Result` inside the `OneshotSender` is what the corresponding function returns on the `RedisClient`, without the `RedisClient` instance. In this case the `RedisClient::hget()` function returns a future with `Item=(RedisClient, Option<RedisValue>)`, so this function takes a oneshot sender where the value of `T` inside the result is an `Option<RedisValue>`.
* The arguments after `tx` correspond to the arguments for the corresponding command, in the same order.

Then add the corresponding function to the `RedisClientRemote` struct in `src/sync/borrowed.rs`.

```rust
/// Returns the value associated with field in the hash stored at key.
///
/// https://redis.io/commands/hget
pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(&self, key: K, field: F) -> Box<Future<Item=Option<RedisValue>, Error=RedisError>> {
  let (tx, rx) = oneshot_channel();
  let (key, field) = (key.into(), field.into());

  let func: CommandFn = SendBoxFnOnce::new(move |client: RedisClient| {
    commands::hget(client, tx, key, field)
  });

  match utils::send_command(&self.command_tx, func) {
    Ok(_) => Box::new(rx.from_err::<RedisError>().flatten()),
    Err(e) => client_utils::future_error(e)
  }
}
```

A few things to note:

* These functions take `&self`, not `self`.
* The same documentation is copied from the implementation on the `RedisClient`.
* The function signature of the command is identical to the corresponding function on the `RedisClient`, with the exception of `&self`.
* The item within the returned future is no longer a tuple of the form `(RedisClient, T)`, now it's just `T`.
* `client_utils::future_error` and `client_utils::future_ok` are used to break out of branches cleanly. Often when it comes to futures the cost of keeping the compiler happy is one more `Box`, at least until [impl Trait](https://github.com/rust-lang/rust/issues/34511) is stable.
* A `futures::sync::oneshot::channel` is used to communicate between threads. All commands create their own oneshot channel for being notified when the request is finished.
* The wrapper functions work by sending a `Box<FnOnce(RedisClient) -> Box<Future<...>>>` between threads. The receiver stays on the same thread as the `RedisClient` and injects the client as an argument when it calls the boxed function on the event loop thread. For this reason all arguments must already be coerced from `Into<T>` to `T` before being moved into the boxed function, otherwise they're not `Sized`, since `Into` is just a trait.
* You can copy-paste your way through most of these.

3 - Implement yet another wrapper for the function on the owned `Send` and `Sync` wrapper.

This is the easiest one. In `src/sync/owned.rs`:

```rust
/// Returns the value associated with field in the hash stored at key.
///
/// https://redis.io/commands/hget
pub fn hget<F: Into<RedisKey>, K: Into<RedisKey>>(self, key: K, field: F) -> Box<Future<Item=(Self, Option<RedisValue>), Error=RedisError>> {
  utils::run_borrowed(self, move |_self, borrowed| {
    Box::new(borrowed.hget(key, field).and_then(move |resp| {
      Ok((_self, resp))
    }))
  })
}
```

Note:

* The same documentation rules apply.
* These functions take `self`, not `&self`.
* The function signature is the same as the corresponding function on the borrowed interface and the `RedisClient`.
* These functions return `(RedisClientRemote, T)`, not just `T`, unlike the borrowed interface above. The `Item` within the returned future exactly matches the corresponding `Item` from the `RedisClient`, but with the first value being a `RedisClientRemote` instead of a `RedisClient`.
* Under the hood the owned wrapper just uses the borrowed wrapper, and `utils::run_borrowed` does all the work unwrapping the underlying borrowed instance.
* `_self` in the `run_borrowed` callback is the same `self` given as the first argument to `run_borrowed`.

That's it.

## Testing Commands

The tests can be found inside `tests/integration`, and are separated between centralized and clustered tests, and separated further by the category of the command (hashes, lists, sets, pubsub, etc). The separation is designed to make it as easy as possible to perform the same test on both centralized and clustered deployments, and as a result you will need to implement the test once, and then wrap it twice to be called from the centralized and clustered wrappers.

Using `hget` as an example again:

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
  utils::setup_test_client(config, |client| {
    hashes_tests::should_set_and_get_simple_key(client)
  });
}
```

Note:

* You may need to create an inlined wrapping module for the command category if one doesn't already exist.
* This function does have a `#[test]` declaration above it.
* A centralized config is used.
* The `utils::setup_test_client` function is used to create the test client and manage the result of the test function created above.
* The inner function just runs the test created above in step 1.

3 - Add another wrapper for the test in `tests/integration/cluster.rs`.

```rust
#[test]
fn it_should_set_and_get_simple_key() {
  let config = RedisConfig::default_clustered();
  utils::setup_test_client(config, |client| {
    hashes_tests::should_set_and_get_simple_key(client)
  });
}
```

The clustered test is identical to the centralized test, but uses a clustered config instead of a centralized one.

That's it. Make sure a centralized redis instance and a clustered deployment are running on the default ports, then `cargo test` or `RUST_LOG=fred=debug cargo test` to see the results.

If you're having trouble getting redis installed check out the `tests/scripts/install_redis_centralized.sh` and `tests/scripts/install_redis_clustered.sh` to see how the CI tool installs them.

## Final Note

Unless you're extremely careful with the keys you use in your tests you are likely to see failing tests due to key collisions. By default cargo runs tests using several threads and in a non-deterministic order, and when reading and writing to shared state, such as a Redis server, you're likely to see race conditions. As a result you probably want to run your tests like so:

```
RUST_LOG=fred=debug cargo test -- --test-threads=1
```