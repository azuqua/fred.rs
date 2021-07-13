
use crate::multiplexer::utils as multiplexer_utils;
use crate::utils as client_utils;

use crate::error::*;
use crate::protocol::types::*;
use crate::types::RedisValue;

use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  DerefMut,
  Deref
};

use redis_protocol::types::{
  Frame as ProtocolFrame,
  FrameKind as ProtocolFrameKind
};

use std::collections::{HashMap, BTreeMap, BTreeSet};

use futures::{
  Future,
  Stream
};
use futures::future::{
  Loop,
  loop_fn
};
use tokio_core::reactor::Handle;

use crate::client::RedisClientInner;
use crate::types::ClientState;

use std::time::Duration;

use std::io::Error as IoError;
use std::io::ErrorKind as IoErrorKind;

/// Elasticache adds a '@1122' or some number suffix to the CLUSTER NODES response
fn remove_elasticache_suffix(server: String) -> String {
  if let Some(first) = server.split("@").next() {
    return first.to_owned();
  }

  server
}

pub fn binary_search(slots: &Vec<Arc<SlotRange>>, slot: u16) -> Option<Arc<SlotRange>> {
  if slot > REDIS_CLUSTER_SLOTS {
    return None;
  }

  let (mut low, mut high) = (0, slots.len() - 1);

  while low <= high {
    let mid = (low + high) / 2;

    if slot < slots[mid].start {
      high = mid - 1;
    }else if slot > slots[mid].end {
      low = mid + 1;
    }else{
      let out = Some(slots[mid].clone());
      return out;
    }
  }

  None
}

pub fn parse_cluster_nodes(status: String) -> Result<HashMap<String, BTreeSet<SlotRange>>, RedisError> {
  let mut out: HashMap<String, BTreeSet<SlotRange>> = HashMap::new();
  let mut server_ids: BTreeMap<String, String> = BTreeMap::new();
  let mut migrating: BTreeMap<u16, String> = BTreeMap::new();

  // build out the slot ranges for the primary nodes
  for line in status.lines() {
    let parts: Vec<&str> = line.split(" ").collect();

    if parts.len() < 8 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Invalid cluster node status line {}.", line)
      ));
    }

    let id = parts[0].to_owned();

    if parts[2].contains("master") {
      let mut slots: BTreeSet<SlotRange> = BTreeSet::new();

      let server = remove_elasticache_suffix(parts[1].to_owned());
      server_ids.insert(id.clone(), server.clone());

      for slot in parts[8..].iter() {
        let inner_parts: Vec<&str> = slot.split("-").collect();

        if inner_parts.len() == 1 {
          // looking at an individual slot

          slots.insert(SlotRange {
            start: inner_parts[0].parse::<u16>()?,
            end: inner_parts[0].parse::<u16>()?,
            server: server.to_owned(),
            id: id.clone(),
            replicas: None
          });
        }else if inner_parts.len() == 2 {
          // looking at a slot range

          slots.insert(SlotRange {
            start: inner_parts[0].parse::<u16>()?,
            end: inner_parts[1].parse::<u16>()?,
            server: server.to_owned(),
            id: id.clone(),
            replicas: None
          });
        }else if inner_parts.len() == 3 {
          // looking at a migrating slot
          if inner_parts[0].is_empty() {
            return Err(RedisError::new(
              RedisErrorKind::ProtocolError, format!("Invalid starting hash slot: {}", inner_parts[0])
            ));
          }
          let start_slot = inner_parts[0][1..].parse::<u16>()?;

          if inner_parts[1] == "<" {
            slots.insert(SlotRange {
              start: start_slot,
              end: start_slot,
              server: server.to_owned(),
              id: id.clone(),
              replicas: None
            });
          }else if inner_parts[1] == ">" {
            let len = if inner_parts[2].len() < 2 {
              return Err(RedisError::new(
                RedisErrorKind::ProtocolError, format!("Invalid server ID: {}", inner_parts[2])
              ));
            }else{
              inner_parts[2].len() - 2
            };

            migrating.insert(start_slot, inner_parts[2][..=len].to_owned());
          }else{
            return Err(RedisError::new(
              RedisErrorKind::ProtocolError, format!("Invalid migrating hash slot range: {}", slot)
            ))
          };
        }else{
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError, format!("Invalid redis hash slot range: {}", slot)
          ));
        }
      }

      out.insert(server.clone(), slots);
    }
  }

  for (slot, server_id) in migrating.into_iter() {
    let server_name = match server_ids.get(&server_id) {
      Some(s) => s,
      None => return Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Unknown server ID: {}", server_id)
      ))
    };
    let mut ranges = match out.get_mut(server_name) {
      Some(r) => r,
      None => return Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Missing server with ID: {}", server_id)
      ))
    };

    ranges.insert(SlotRange {
      start: slot,
      end: slot,
      id: server_id,
      server: server_name.to_owned(),
      replicas: None
    });
  }

  out.shrink_to_fit();
  Ok(out)
}

fn first_two_words(s: &str) -> (&str, &str) {
  let mut parts = s.split_whitespace();
  (parts.next().unwrap_or(""), parts.next().unwrap_or(""))
}

pub fn pretty_error(resp: String) -> RedisError {
  let kind = {
    let (first, second) = first_two_words(&resp);

    match first.as_ref(){
      ""          => RedisErrorKind::Unknown,
      "ERR"       => RedisErrorKind::Unknown,
      "WRONGTYPE" => RedisErrorKind::InvalidArgument,
      "NOAUTH"    => RedisErrorKind::Auth,
      "Invalid"   => match second.as_ref() {
        "argument(s)" | "Argument" => RedisErrorKind::InvalidArgument,
        "command" | "Command"      => RedisErrorKind::InvalidCommand,
        _                          => RedisErrorKind::Unknown,
      }
      _ => RedisErrorKind::Unknown
    }
  };
  let details = if resp.is_empty() {
    "No response!".into()
  }else{
    resp
  };

  RedisError::new(kind, details)
}

pub fn frame_to_pubsub(frame: ProtocolFrame) -> Result<(String, RedisValue), RedisError> {
  if let Ok((channel, message)) = frame.parse_as_pubsub() {
   Ok((channel, RedisValue::String(message)))
  }else{
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub message frame."))
  }
}

pub fn command_args(kind: &RedisCommandKind) -> Option<ProtocolFrame> {
  let frame = if kind.is_cluster_command() {
    if let Some(arg) = kind.cluster_args() {
      ProtocolFrame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else if kind.is_client_command() {
    if let Some(arg) = kind.client_args() {
      ProtocolFrame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else if kind.is_config_command() {
    if let Some(arg) = kind.config_args() {
      ProtocolFrame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else if kind.is_memory_command() {
    if let Some(arg) = kind.memory_args() {
      ProtocolFrame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else{
    return None;
  };

  Some(frame)
}

#[cfg(not(feature="ignore-auth-error"))]
pub fn check_auth_error(frame: ProtocolFrame) -> ProtocolFrame {
  frame
}

#[cfg(feature="ignore-auth-error")]
pub fn check_auth_error(frame: ProtocolFrame) -> ProtocolFrame {
  let is_auth_error = match frame {
    ProtocolFrame::Error(ref s) => s == "ERR Client sent AUTH, but no password is set",
    _ => false
  };

  if is_auth_error {
    ProtocolFrame::SimpleString("OK".into())
  }else{
    frame
  }
}

pub fn frame_to_results(frame: ProtocolFrame) -> Result<Vec<RedisValue>, RedisError> {
  match frame {
    ProtocolFrame::SimpleString(s) => Ok(vec![s.into()]),
    ProtocolFrame::Integer(i) => Ok(vec![i.into()]),
    ProtocolFrame::BulkString(b) => {
      String::from_utf8(b)
        .map(|s| vec![s.into()])
        .map_err(|e| e.into())
    },
    ProtocolFrame::Array(mut frames) => {
      let mut out = Vec::with_capacity(frames.len());
      for frame in frames.drain(..) {
        // there shouldn't be errors buried in arrays...
        let mut res = frame_to_results(frame)?;

        if res.len() > 1 {
          // nor should there be more than one layer of nested arrays
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError, "Invalid nested array."
          ));
        }else if res.len() == 0 {
          // shouldn't be possible...
          return Err(RedisError::new(
            RedisErrorKind::Unknown, "Invalid empty frame."
          ));
        }

        out.push(res.pop().unwrap())
      }

      Ok(out)
    },
    ProtocolFrame::Null => Ok(vec![RedisValue::Null]),
    ProtocolFrame::Error(s) => Err(pretty_error(s)),
    _ => Err(RedisError::new(
      RedisErrorKind::ProtocolError, "Invalid frame."
    ))
  }
}

pub fn frame_to_single_result(frame: ProtocolFrame) -> Result<RedisValue, RedisError> {
  match frame {
    ProtocolFrame::SimpleString(s) => Ok(s.into()),
    ProtocolFrame::Integer(i) => Ok(i.into()),
    ProtocolFrame::BulkString(b) => {
      String::from_utf8(b).map(|s| s.into()).map_err(|e| e.into())
    },
    ProtocolFrame::Array(mut frames) => {
      if frames.len() > 1 {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Could not convert multiple frames to RedisValue."
        ));
      }else if frames.is_empty() {
        return Ok(RedisValue::Null);
      }

      let first_frame = frames.pop().unwrap();
      if first_frame.kind() == ProtocolFrameKind::Array || first_frame.kind() == ProtocolFrameKind::Error {
        // there shouldn't be errors buried in arrays, nor should there be more than one layer of nested arrays
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid nested array."
        ));
      }

      frame_to_single_result(first_frame)
    },
    ProtocolFrame::Null => Ok(RedisValue::Null),
    ProtocolFrame::Error(s) => Err(pretty_error(s)),
    _ => Err(RedisError::new(
      RedisErrorKind::ProtocolError, "Invalid frame."
    ))
  }
}

pub fn frame_to_error(frame: ProtocolFrame) -> Option<RedisError> {
  if let ProtocolFrame::Error(s) = frame {
    Some(pretty_error(s))
  }else{
    None
  }
}

/// Reconnect to the server based on the result of the previous connection.
#[allow(unused_variables)]
pub fn reconnect(handle: Handle, inner: Arc<RedisClientInner>, mut result: Result<Option<RedisError>, RedisError>, force_no_delay: bool)
  -> Box<Future<Item=Loop<(), (Handle, Arc<RedisClientInner>)>, Error=RedisError>>
{
  // since framed sockets don't give an error when closed abruptly the client's state is
  // used to determine whether or not the socket was closed intentionally or not
  if client_utils::read_client_state(&inner.state) == ClientState::Disconnecting {
    result = Err(RedisError::new(
      RedisErrorKind::IO, "Redis socket closed abruptly."
    ));
  }

  debug!("{} Starting reconnect logic from error {:?}...", n!(inner), result);

  match result {
    Ok(err) => {
      if let Some(err) = err {
        // socket was closed unintentionally
        debug!("{} Redis client closed abruptly.", n!(inner));
        multiplexer_utils::emit_error(&inner.error_tx, &err);

        let delay = match multiplexer_utils::next_reconnect_delay(&inner.policy) {
          Some(delay) => delay,
          None => return client_utils::future_ok(Loop::Break(()))
        };

        debug!("{} Waiting for {} ms before attempting to reconnect...", n!(inner), delay);

        Box::new(inner.timer.sleep(Duration::from_millis(delay as u64)).from_err::<RedisError>().and_then(move |_| {
          if client_utils::read_closed_flag(&inner.closed) {
            client_utils::set_closed_flag(&inner.closed, false);

            return Err(RedisError::new(
              RedisErrorKind::Canceled, "Client closed while waiting to reconnect."
            ));
          }

          Ok(Loop::Continue((handle, inner)))
        }))
      } else {
        // socket was closed via Quit command
        debug!("{} Redis client closed via Quit.", n!(inner));

        client_utils::set_client_state(&inner.state, ClientState::Disconnected);

        multiplexer_utils::close_error_tx(&inner.error_tx);
        multiplexer_utils::close_reconnect_tx(&inner.reconnect_tx);
        multiplexer_utils::close_connect_tx(&inner.connect_tx);
        multiplexer_utils::close_messages_tx(&inner.message_tx);

        client_utils::future_ok(Loop::Break(()))
      }
    },
    Err(e) => {
      multiplexer_utils::emit_error(&inner.error_tx, &e);

      let delay = match multiplexer_utils::next_reconnect_delay(&inner.policy) {
        Some(delay) => delay,
        None => return client_utils::future_ok(Loop::Break(()))
      };

      debug!("{} Waiting for {} ms before attempting to reconnect...", n!(inner), delay);

      Box::new(inner.timer.sleep(Duration::from_millis(delay as u64)).from_err::<RedisError>().and_then(move |_| {
        if client_utils::read_closed_flag(&inner.closed) {
          client_utils::set_closed_flag(&inner.closed, false);

          return Err(RedisError::new(
            RedisErrorKind::Canceled, "Client closed while waiting to reconnect."
          ));
        }

        Ok(Loop::Continue((handle, inner)))
      }))
    }
  }
}


// ------------------

#[cfg(test)]
mod tests {
  use super::*;
  use crate::types::*;
  use crate::protocol::types::*;
  use std::collections::HashMap;

  fn to_btree(data: Vec<SlotRange>) -> BTreeSet<SlotRange> {
    data.into_iter().fold(BTreeSet::new(), |mut acc, range| {
      acc.insert(range);
      acc
    })
  }

  #[test]
  fn should_parse_cluster_node_status_individual_slot() {
    let status = "2edc9a62355eacff9376c4e09643e2c932b0356a foo.use2.cache.amazonaws.com:6379@1122 master - 0 1565908731456 2950 connected 1242-1696 8195-8245 8247-8423 10923-12287
db2fd89f83daa5fe49110ef760794f9ccee07d06 bar.use2.cache.amazonaws.com:6379@1122 master - 0 1565908731000 2952 connected 332-1241 8152-8194 8424-8439 9203-10112 12288-12346 12576-12685
d9aeabb1525e5656c98545a0ed42c8c99bbacae1 baz.use2.cache.amazonaws.com:6379@1122 master - 0 1565908729402 2956 connected 1697 1815-2291 3657-4089 5861-6770 7531-7713 13154-13197
5671f02def98d0279224f717aba0f95874e5fb89 wibble.use2.cache.amazonaws.com:6379@1122 master - 0 1565908728391 2953 connected 7900-8125 12427 13198-13760 15126-16383
0b1923e386f6f6f3adc1b6deb250ef08f937e9b5 wobble.use2.cache.amazonaws.com:6379@1122 master - 0 1565908731000 2954 connected 5462-5860 6771-7382 8133-8151 10113-10922 12686-12893
1c5d99e3d6fca2090d0903d61d4e51594f6dcc05 qux.use2.cache.amazonaws.com:6379@1122 master - 0 1565908732462 2949 connected 2292-3656 7383-7530 8896-9202 12347-12426 12428-12575
b8553a4fae8ae99fca716d423b14875ebb10fefe quux.use2.cache.amazonaws.com:6379@1122 master - 0 1565908730439 2951 connected 8246 8440-8895 12919-13144 13761-15125
4a58ba550f37208c9a9909986ce808cdb058e31f quuz.use2.cache.amazonaws.com:6379@1122 myself,master - 0 1565908730000 2955 connected 0-331 1698-1814 4090-5461 7714-7899 8126-8132 12894-12918 13145-13153";

    let mut expected: HashMap<String, BTreeSet<SlotRange>> = HashMap::new();
    expected.insert("foo.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 1242,
        end: 1696,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        replicas: None,
      },
      SlotRange {
        start: 8195,
        end: 8245,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        replicas: None,
      },
      SlotRange {
        start: 8247,
        end: 8423,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        replicas: None,
      },
      SlotRange {
        start: 10923,
        end: 12287,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        replicas: None,
      }
    ]));
    expected.insert("bar.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 332,
        end: 1241,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        replicas: None,
      },
      SlotRange {
        start: 8152,
        end: 8194,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        replicas: None,
      },
      SlotRange {
        start: 8424,
        end: 8439,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        replicas: None,
      },
      SlotRange {
        start: 9203,
        end: 10112,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        replicas: None,
      },
      SlotRange {
        start: 12288,
        end: 12346,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        replicas: None,
      },
      SlotRange {
        start: 12576,
        end: 12685,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        replicas: None,
      }
    ]));
    expected.insert("baz.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 1697,
        end: 1697,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        replicas: None,
      },
      SlotRange {
        start: 1815,
        end: 2291,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        replicas: None,
      },
      SlotRange {
        start: 3657,
        end: 4089,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        replicas: None,
      },
      SlotRange {
        start: 5861,
        end: 6770,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        replicas: None,
      },
      SlotRange {
        start: 7531,
        end: 7713,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        replicas: None,
      },
      SlotRange {
        start: 13154,
        end: 13197,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        replicas: None,
      }
    ]));
    expected.insert("wibble.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 7900,
        end: 8125,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        replicas: None,
      },
      SlotRange {
        start: 12427,
        end: 12427,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        replicas: None,
      },
      SlotRange {
        start: 13198,
        end: 13760,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        replicas: None,
      },
      SlotRange {
        start: 15126,
        end: 16383,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        replicas: None,
      }
    ]));
    expected.insert("wobble.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 5462,
        end: 5860,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        replicas: None,
      },
      SlotRange {
        start: 6771,
        end: 7382,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        replicas: None,
      },
      SlotRange {
        start: 8133,
        end: 8151,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        replicas: None,
      },
      SlotRange {
        start: 10113,
        end: 10922,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        replicas: None,
      },
      SlotRange {
        start: 12686,
        end: 12893,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        replicas: None,
      }
    ]));
    expected.insert("qux.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 2292,
        end: 3656,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        replicas: None,
      },
      SlotRange {
        start: 7383,
        end: 7530,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        replicas: None,
      },
      SlotRange {
        start: 8896,
        end: 9202,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        replicas: None,
      },
      SlotRange {
        start: 12347,
        end: 12426,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        replicas: None,
      },
      SlotRange {
        start: 12428,
        end: 12575,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        replicas: None,
      }
    ]));
    expected.insert("quux.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 8246,
        end: 8246,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        replicas: None,
      },
      SlotRange {
        start: 8440,
        end: 8895,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        replicas: None,
      },
      SlotRange {
        start: 12919,
        end: 13144,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        replicas: None,
      },
      SlotRange {
        start: 13761,
        end: 15125,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        replicas: None,
      }
    ]));
    expected.insert("quuz.use2.cache.amazonaws.com:6379".into(), to_btree(vec![
      SlotRange {
        start: 0,
        end: 331,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      },
      SlotRange {
        start: 1698,
        end: 1814,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      },
      SlotRange {
        start: 4090,
        end: 5461,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      },
      SlotRange {
        start: 7714,
        end: 7899,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      },
      SlotRange {
        start: 8126,
        end: 8132,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      },
      SlotRange {
        start: 12894,
        end: 12918,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      },
      SlotRange {
        start: 13145,
        end: 13153,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        replicas: None,
      }
    ]));

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e)
    };
    assert_eq!(actual, expected);

    let cache = ClusterKeyCache::new(Some(status.to_owned())).expect("Failed to build cluster cache");
    let slot = cache.get_server(8246).unwrap();
    assert_eq!(slot.server, "quux.use2.cache.amazonaws.com:6379".to_owned());
    let slot = cache.get_server(1697).unwrap();
    assert_eq!(slot.server, "baz.use2.cache.amazonaws.com:6379".to_owned());
    let slot = cache.get_server(12427).unwrap();
    assert_eq!(slot.server, "wibble.use2.cache.amazonaws.com:6379".to_owned());
    let slot = cache.get_server(8445).unwrap();
    assert_eq!(slot.server, "quux.use2.cache.amazonaws.com:6379".to_owned());
  }

  #[test]
  fn should_parse_cluster_node_status() {
    let status = "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460";

    let mut expected: HashMap<String, BTreeSet<SlotRange>> = HashMap::new();
    expected.insert("127.0.0.1:30002".to_owned(), to_btree(vec![SlotRange {
      start: 5461,
      end: 10922,
      server: "127.0.0.1:30002".to_owned(),
      id: "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1".to_owned(),
      replicas: None
    }]));
    expected.insert("127.0.0.1:30003".to_owned(), to_btree(vec![SlotRange {
      start: 10923,
      end: 16383,
      server: "127.0.0.1:30003".to_owned(),
      id: "292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f".to_owned(),
      replicas: None
    }]));
    expected.insert("127.0.0.1:30001".to_owned(), to_btree(vec![SlotRange {
      start: 0,
      end: 5460,
      server: "127.0.0.1:30001".to_owned(),
      id: "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca".to_owned(),
      replicas: None
    }]));

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e)
    };
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_elasticache_cluster_node_status() {
    let status = "eec2b077ee95c590279115aac13e7eefdce61dba foo.cache.amazonaws.com:6379@1122 master - 0 1530900241038 0 connected 5462-10922
b4fa5337b58e02673f961e22c9557e81dda4b559 bar.cache.amazonaws.com:6379@1122 myself,master - 0 1530900240000 1 connected 0-5461
29d37b842d1bb097ba491be8f1cb00648620d4bd baz.cache.amazonaws.com:6379@1122 master - 0 1530900242042 2 connected 10923-16383";

    let mut expected: HashMap<String, BTreeSet<SlotRange>> = HashMap::new();
    expected.insert("foo.cache.amazonaws.com:6379".to_owned(), to_btree(vec![SlotRange {
      start: 5462,
      end: 10922,
      server: "foo.cache.amazonaws.com:6379".to_owned(),
      id: "eec2b077ee95c590279115aac13e7eefdce61dba".to_owned(),
      replicas: None
    }]));
    expected.insert("bar.cache.amazonaws.com:6379".to_owned(), to_btree(vec![SlotRange {
      start: 0,
      end: 5461,
      server: "bar.cache.amazonaws.com:6379".to_owned(),
      id: "b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned(),
      replicas: None
    }]));
    expected.insert("baz.cache.amazonaws.com:6379".to_owned(), to_btree(vec![SlotRange {
      start: 10923,
      end: 16383,
      server: "baz.cache.amazonaws.com:6379".to_owned(),
      id: "29d37b842d1bb097ba491be8f1cb00648620d4bd".to_owned(),
      replicas: None
    }]));

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e)
    };
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_migrating_cluster_nodes() {
    let status = "eec2b077ee95c590279115aac13e7eefdce61dba foo.cache.amazonaws.com:6379@1122 master - 0 1530900241038 0 connected 5462-10921 [10922->-b4fa5337b58e02673f961e22c9557e81dda4b559]
b4fa5337b58e02673f961e22c9557e81dda4b559 bar.cache.amazonaws.com:6379@1122 myself,master - 0 1530900240000 1 connected 0-5461 [10922-<-eec2b077ee95c590279115aac13e7eefdce61dba]
29d37b842d1bb097ba491be8f1cb00648620d4bd baz.cache.amazonaws.com:6379@1122 master - 0 1530900242042 2 connected 10923-16383";

    let mut expected: HashMap<String, BTreeSet<SlotRange>> = HashMap::new();
    expected.insert("foo.cache.amazonaws.com:6379".to_owned(), to_btree(vec![SlotRange {
      start: 5462,
      end: 10921,
      server: "foo.cache.amazonaws.com:6379".to_owned(),
      id: "eec2b077ee95c590279115aac13e7eefdce61dba".to_owned(),
      replicas: None
    }]));
    expected.insert("bar.cache.amazonaws.com:6379".to_owned(), to_btree(vec![
      SlotRange {
        start: 0,
        end: 5461,
        server: "bar.cache.amazonaws.com:6379".to_owned(),
        id: "b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned(),
        replicas: None
      },
      SlotRange {
        start: 10922,
        end: 10922,
        server: "bar.cache.amazonaws.com:6379".to_owned(),
        id: "b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned(),
        replicas: None
      }
    ]));
    expected.insert("baz.cache.amazonaws.com:6379".to_owned(), to_btree(vec![SlotRange {
      start: 10923,
      end: 16383,
      server: "baz.cache.amazonaws.com:6379".to_owned(),
      id: "29d37b842d1bb097ba491be8f1cb00648620d4bd".to_owned(),
      replicas: None
    }]));

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e)
    };
    assert_eq!(actual, expected);
  }

}
