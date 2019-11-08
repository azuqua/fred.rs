
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

use std::collections::HashMap;

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

pub fn parse_cluster_nodes(status: String) -> Result<HashMap<String, Vec<SlotRange>>, RedisError> {
  let mut out: HashMap<String, Vec<SlotRange>> = HashMap::new();

  // build out the slot ranges for the master nodes
  for line in status.lines() {
    let parts: Vec<&str> = line.split(" ").collect();

    if parts.len() < 8 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Invalid cluster node status line {}.", line)
      ));
    }

    let id = parts[0].to_owned();

    if parts[2].contains("master") {
      let mut slots: Vec<SlotRange> = Vec::new();

      let server = remove_elasticache_suffix(parts[1].to_owned());
      for slot in parts[8..].iter() {
        let inner_parts: Vec<&str> = slot.split("-").collect();

        if inner_parts.len() == 1 {
          // looking at an individual slot

          slots.push(SlotRange {
            start: inner_parts[0].parse::<u16>()?,
            end: inner_parts[0].parse::<u16>()?,
            server: server.to_owned(),
            id: id.clone(),
            slaves: None
          });
        }else if inner_parts.len() == 2 {
          // looking at a slot range

          slots.push(SlotRange {
            start: inner_parts[0].parse::<u16>()?,
            end: inner_parts[1].parse::<u16>()?,
            server: server.to_owned(),
            id: id.clone(),
            slaves: None
          });
        }else{
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError, format!("Invalid redis hash slot range: {}", slot)
          ));
        }
      }

      out.insert(server.clone(), slots);
    }
  }

  // attach the slave nodes to the masters from the first loop
  for line in status.lines() {
    let parts: Vec<&str> = line.split(" ").collect();

    if parts.len() < 8 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Invalid cluster node status line {}.", line)
      ));
    }

    if parts[2].contains("slave") {
      let master_id = parts[3].to_owned();

      if parts[7] != "connected" {
        continue;
      }

      let mut master: Option<&mut SlotRange> = None;
      for (_, mut slots) in out.iter_mut() {
        for mut slot in slots.iter_mut() {
          if slot.id == master_id {
            master = Some(slot);
          }
        }
      }
      let master = match master {
        Some(slot) => slot,
        None => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, format!("Invalid cluster node status line for slave node. (Missing master) {}.", line)
        ))
      };

      let server = remove_elasticache_suffix(parts[1].to_owned());
      let has_slaves = master.slaves.is_some();

      if has_slaves {
        if let Some(ref mut slaves) = master.slaves {
          slaves.add(server);
        }
      }else{
        master.slaves = Some(SlaveNodes::new(vec![server]));
      }
    }
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
    } else {
      return None;
    }
  }else if kind.is_script_command() {
    if let Some(arg) = kind.script_args() {
      ProtocolFrame::BulkString(arg.into_bytes())
    } else {
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
        if frame.is_array() {
          // only allow 2 more levels of nested arrays
          if let ProtocolFrame::Array(inner) = frame {

            for inner_frame in inner.into_iter() {
              if let ProtocolFrame::Array(nested_array) = inner_frame {
                let mut inner_values = Vec::with_capacity(nested_array.len());
                for inner_frame in nested_array.into_iter() {
                  inner_values.push(frame_to_single_result(inner_frame)?);
                }

                out.push(RedisValue::Array(inner_values));
              }else{
                out.push(frame_to_single_result(inner_frame)?);
              }
            }
          }else{
            return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid nested array frame."));
          }
        }else if frame.is_error() {
          if let ProtocolFrame::Error(s) = frame {
            return Err(pretty_error(s));
          }else{
            return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid nested error frame."));
          }
        }else{
          // convert to single result
          out.push(frame_to_single_result(frame)?);
        }
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

    let mut expected: HashMap<String, Vec<SlotRange>> = HashMap::new();
    expected.insert("foo.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 1242,
        end: 1696,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        slaves: None,
      },
      SlotRange {
        start: 8195,
        end: 8245,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        slaves: None,
      },
      SlotRange {
        start: 8247,
        end: 8423,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        slaves: None,
      },
      SlotRange {
        start: 10923,
        end: 12287,
        server: "foo.use2.cache.amazonaws.com:6379".into(),
        id: "2edc9a62355eacff9376c4e09643e2c932b0356a".into(),
        slaves: None,
      }
    ]);
    expected.insert("bar.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 332,
        end: 1241,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        slaves: None,
      },
      SlotRange {
        start: 8152,
        end: 8194,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        slaves: None,
      },
      SlotRange {
        start: 8424,
        end: 8439,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        slaves: None,
      },
      SlotRange {
        start: 9203,
        end: 10112,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        slaves: None,
      },
      SlotRange {
        start: 12288,
        end: 12346,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        slaves: None,
      },
      SlotRange {
        start: 12576,
        end: 12685,
        server: "bar.use2.cache.amazonaws.com:6379".into(),
        id: "db2fd89f83daa5fe49110ef760794f9ccee07d06".into(),
        slaves: None,
      }
    ]);
    expected.insert("baz.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 1697,
        end: 1697,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        slaves: None,
      },
      SlotRange {
        start: 1815,
        end: 2291,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        slaves: None,
      },
      SlotRange {
        start: 3657,
        end: 4089,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        slaves: None,
      },
      SlotRange {
        start: 5861,
        end: 6770,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        slaves: None,
      },
      SlotRange {
        start: 7531,
        end: 7713,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        slaves: None,
      },
      SlotRange {
        start: 13154,
        end: 13197,
        server: "baz.use2.cache.amazonaws.com:6379".into(),
        id: "d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into(),
        slaves: None,
      }
    ]);
    expected.insert("wibble.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 7900,
        end: 8125,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        slaves: None,
      },
      SlotRange {
        start: 12427,
        end: 12427,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        slaves: None,
      },
      SlotRange {
        start: 13198,
        end: 13760,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        slaves: None,
      },
      SlotRange {
        start: 15126,
        end: 16383,
        server: "wibble.use2.cache.amazonaws.com:6379".into(),
        id: "5671f02def98d0279224f717aba0f95874e5fb89".into(),
        slaves: None,
      }
    ]);
    expected.insert("wobble.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 5462,
        end: 5860,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        slaves: None,
      },
      SlotRange {
        start: 6771,
        end: 7382,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        slaves: None,
      },
      SlotRange {
        start: 8133,
        end: 8151,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        slaves: None,
      },
      SlotRange {
        start: 10113,
        end: 10922,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        slaves: None,
      },
      SlotRange {
        start: 12686,
        end: 12893,
        server: "wobble.use2.cache.amazonaws.com:6379".into(),
        id: "0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into(),
        slaves: None,
      }
    ]);
    expected.insert("qux.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 2292,
        end: 3656,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        slaves: None,
      },
      SlotRange {
        start: 7383,
        end: 7530,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        slaves: None,
      },
      SlotRange {
        start: 8896,
        end: 9202,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        slaves: None,
      },
      SlotRange {
        start: 12347,
        end: 12426,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        slaves: None,
      },
      SlotRange {
        start: 12428,
        end: 12575,
        server: "qux.use2.cache.amazonaws.com:6379".into(),
        id: "1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into(),
        slaves: None,
      }
    ]);
    expected.insert("quux.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 8246,
        end: 8246,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        slaves: None,
      },
      SlotRange {
        start: 8440,
        end: 8895,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        slaves: None,
      },
      SlotRange {
        start: 12919,
        end: 13144,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        slaves: None,
      },
      SlotRange {
        start: 13761,
        end: 15125,
        server: "quux.use2.cache.amazonaws.com:6379".into(),
        id: "b8553a4fae8ae99fca716d423b14875ebb10fefe".into(),
        slaves: None,
      }
    ]);
    expected.insert("quuz.use2.cache.amazonaws.com:6379".into(), vec![
      SlotRange {
        start: 0,
        end: 331,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      },
      SlotRange {
        start: 1698,
        end: 1814,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      },
      SlotRange {
        start: 4090,
        end: 5461,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      },
      SlotRange {
        start: 7714,
        end: 7899,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      },
      SlotRange {
        start: 8126,
        end: 8132,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      },
      SlotRange {
        start: 12894,
        end: 12918,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      },
      SlotRange {
        start: 13145,
        end: 13153,
        server: "quuz.use2.cache.amazonaws.com:6379".into(),
        id: "4a58ba550f37208c9a9909986ce808cdb058e31f".into(),
        slaves: None,
      }
    ]);

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
    let status = "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460";

    let mut expected: HashMap<String, Vec<SlotRange>> = HashMap::new();
    expected.insert("127.0.0.1:30002".to_owned(), vec![SlotRange {
      start: 5461,
      end: 10922,
      server: "127.0.0.1:30002".to_owned(),
      id: "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1".to_owned(),
      slaves: Some(SlaveNodes::new(vec![
        "127.0.0.1:30005".to_owned()
      ]))
    }]);
    expected.insert("127.0.0.1:30003".to_owned(), vec![SlotRange {
      start: 10923,
      end: 16383,
      server: "127.0.0.1:30003".to_owned(),
      id: "292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f".to_owned(),
      slaves: Some(SlaveNodes::new(vec![
        "127.0.0.1:30006".to_owned()
      ]))
    }]);
    expected.insert("127.0.0.1:30001".to_owned(), vec![SlotRange {
      start: 0,
      end: 5460,
      server: "127.0.0.1:30001".to_owned(),
      id: "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca".to_owned(),
      slaves: Some(SlaveNodes::new(vec![
        "127.0.0.1:30004".to_owned()
      ]))
    }]);

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

    let mut expected: HashMap<String, Vec<SlotRange>> = HashMap::new();
    expected.insert("foo.cache.amazonaws.com:6379".to_owned(), vec![SlotRange {
      start: 5462,
      end: 10922,
      server: "foo.cache.amazonaws.com:6379".to_owned(),
      id: "eec2b077ee95c590279115aac13e7eefdce61dba".to_owned(),
      slaves: None
    }]);
    expected.insert("bar.cache.amazonaws.com:6379".to_owned(), vec![SlotRange {
      start: 0,
      end: 5461,
      server: "bar.cache.amazonaws.com:6379".to_owned(),
      id: "b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned(),
      slaves: None
    }]);
    expected.insert("baz.cache.amazonaws.com:6379".to_owned(), vec![SlotRange {
      start: 10923,
      end: 16383,
      server: "baz.cache.amazonaws.com:6379".to_owned(),
      id: "29d37b842d1bb097ba491be8f1cb00648620d4bd".to_owned(),
      slaves: None
    }]);

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e)
    };
    assert_eq!(actual, expected);
  }

}
