use futures::future;
use futures::{IntoFuture, Future, Stream};
use futures::stream;

use fred::error::{
  RedisErrorKind,
  RedisError
};
use fred::types::*;
use fred::RedisClient;
use fred::owned::RedisClientOwned;

use super::utils;

pub fn should_llen_on_empty_list(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.llen("foo").and_then(|(client, len)| {
    assert_eq!(len, 0);

    Ok(())
  }))
}

pub fn should_llen_on_list_with_elements(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(client.lpush("foo", 1).and_then(|(client, len)| {
    assert_eq!(len, 1);

    client.lpush("foo", 2)
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 2);

    client.lpush("foo", 3)
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 3);

    client.llen("foo")
  })
  .and_then(|(client, len)| {
    assert_eq!(len, 3);

    Ok(())
  }))
}

pub fn should_lpush_and_lpop_to_list(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(stream::iter_ok(0..50).fold(client, move |client, num| {
    client.lpush("foo", num.to_string()).and_then(move |(client, len)| {
      assert_eq!(len, num + 1);

      Ok(client)
    })
  })
  .and_then(|client| {
    stream::iter_ok(0..50).fold(client, move |client, num| {
      client.lpop("foo").and_then(move |(client, value)| {
        let value = match value {
          Some(v) => v,
          None => panic!("Expected value for list foo not found.")
        };

        assert_eq!(value.into_string().unwrap(), (49 - num).to_string());

        Ok(client)
      })
    })
  })
  .and_then(|_| Ok(())))
}

pub fn should_ltrim_list(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(
    client.del("foo")
    .and_then(|(client, _)| {
      stream::iter_ok(0..5).fold(client, move |client, num| 
        client.lpush("foo", num.to_string()).and_then(move |(client, _)| Ok(client))
      )
    })
    .and_then(|client: RedisClient| {
      client.ltrim("foo", 1, -2).and_then(|(client, resp)| {
        assert_eq!("OK", resp);
        client.llen("foo")
      })
    })
    .and_then(|(client, resp)| {
      assert_eq!(3_usize, resp);
      let expected_foo = ["3", "2", "1"];
      stream::iter_ok(0_usize..resp).fold(client, move |client, i| {
        client.lpop("foo").and_then(move |(client, val)| {
          match val {
            Some(RedisValue::String(s)) => assert_eq!(expected_foo[i], s),
            _ => panic!("foo contains incorrect values after LTRIM.")
          };
          Ok(client)
        })
      })
    })
    .and_then(|_| Ok(()))
  )
}

pub fn should_lmove_source_to_dest(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let foo = "foo";
  let bar = "bar";
  Box::new(
    client.del(vec![foo, bar])
    .and_then(move |(client, _)| 
    stream::iter_ok(0..10).fold(client, move |client, num| {
      // Note it's 'l'push.
      client.lpush(foo, num.to_string()).and_then(move |(client, len)| {
        Ok(client)
      })
    }))
    .and_then(move |client: RedisClient| {
      client.lmove(foo, bar, LmoveWhere::Right, LmoveWhere::Left)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("0".to_string()));
      client.lmove(foo, bar, LmoveWhere::Right, LmoveWhere::Left)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("1".to_string()));
      client.lmove(foo, bar, LmoveWhere::Right, LmoveWhere::Right)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("2".to_string()));
      client.lmove(foo, bar, LmoveWhere::Left, LmoveWhere::Left)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("9".to_string()));
      client.lmove(foo, bar, LmoveWhere::Left, LmoveWhere::Right)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("8".to_string()));
      client.llen(foo)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, 5); 
      let expected_foo = ["7", "6", "5", "4", "3"];
      stream::iter_ok(0..resp).fold(client, move |client, i| {
        client.lpop(foo).and_then(move |(client, val)| {
          match val {
            Some(RedisValue::String(val)) => assert_eq!(val.as_str(), expected_foo[i]),
            _ => panic!("Found unexpected value in source after LMOVE.")
          }
          Ok(client)
        })
      })
    })
    .and_then(move |client| {
      client.llen(bar)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, 5); 
      let expected_bar = ["9", "1", "0", "2", "8"];
      stream::iter_ok(0..resp).fold(client, move |client, i| {
        client.lpop(bar).and_then(move |(client, val)| {
          match val {
            Some(RedisValue::String(val)) => assert_eq!(val.as_str(), expected_bar[i]),
            _ => panic!("Found unexpected value in destination after LMOVE.")
          }
          Ok(client)
        })
      })
    })
    .then(|_| future::ok(())))
}

pub fn should_lmove_and_get_nil(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(
    // Deleting foo, in case it already exists
    client.del("foo")
    .and_then(move |(client, _)| {
      client.lmove("foo", "bar", LmoveWhere::Right, LmoveWhere::Left)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, None);
      future::ok(())
    }))
}

pub fn should_rpoplpush_source_to_dest(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  let foo = "foo";
  let bar = "bar";
  Box::new(
    client.del(vec![foo, bar])
    .and_then(move |(client, _)| 
    stream::iter_ok(0..5).fold(client, move |client, num| {
      // Note it's 'l'push.
      client.lpush(foo, num.to_string()).and_then(move |(client, len)| {
        Ok(client)
      })
    }))
    .and_then(move |client: RedisClient| {
      client.rpoplpush(foo, bar)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("0".to_string()));
      client.rpoplpush(foo, bar)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, Some("1".to_string()));
      client.llen(foo)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, 3); 
      let expected_foo = ["4", "3", "2"];
      stream::iter_ok(0..resp).fold(client, move |client, i| {
        client.lpop(foo).and_then(move |(client, val)| {
          match val {
            Some(RedisValue::String(val)) => assert_eq!(val.as_str(), expected_foo[i]),
            _ => panic!("Found unexpected value in source after RPOPLPUSH.")
          }
          Ok(client)
        })
      })
    })
    .and_then(move |client| {
      client.llen(bar)
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, 2); 
      let expected_bar = ["1", "0"];
      stream::iter_ok(0..resp).fold(client, move |client, i| {
        client.lpop(bar).and_then(move |(client, val)| {
          match val {
            Some(RedisValue::String(val)) => assert_eq!(val.as_str(), expected_bar[i]),
            _ => panic!("Found unexpected value in destination after RPOPLPUSH.")
          }
          Ok(client)
        })
      })
    })
    .then(|_| future::ok(())))
}

pub fn should_rpoplpush_to_get_nil(client: RedisClient) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(
    // Deleting foo, in case it already exists
    client.del("foo")
    .and_then(move |(client, _)| {
      client.rpoplpush("foo", "bar")
    })
    .and_then(move |(client, resp)| {
      assert_eq!(resp, None);
      future::ok(())
    }))
}