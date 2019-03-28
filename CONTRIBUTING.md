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

# Tests

