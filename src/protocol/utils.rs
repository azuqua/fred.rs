
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

        if inner_parts.len() < 2 {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError, format!("Invalid cluster node hash slot range {}.", slot)
          ));
        }

        slots.push(SlotRange {
          start: inner_parts[0].parse::<u16>()?,
          end: inner_parts[1].parse::<u16>()?,
          server: server.to_owned(),
          id: id.clone(),
          slaves: None
        });
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
  if let ProtocolFrame::Array(mut frames) = frame {
    if frames.len() != 3 {
      return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub message frames."));
    }

    let payload = frames.pop().unwrap();
    let channel = frames.pop().unwrap();
    let message_type = frames.pop().unwrap();

    let message_type = match message_type.to_string() {
      Some(s) => s,
      None => {
        return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub message type frame."))
      }
    };

    if message_type == "message" {
      let channel = match channel.to_string() {
        Some(c) => c,
        None => {
          return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub channel frame."))
        }
      };

      // the payload is a bulk string on pubsub messages
      if payload.kind() == ProtocolFrameKind::BulkString {
        let payload = frame_to_single_result(payload)?;

        if !payload.is_string() {
          Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub channel payload."))
        }else{
          Ok((channel, payload))
        }
      }else{
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub payload frame type."))
      }
    }else{
      Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub message type."))
    }
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

  debug!("Starting reconnect logic from error {:?}...", result);

  match result {
    Ok(err) => {
      if let Some(err) = err {
        // socket was closed unintentionally
        debug!("Redis client closed abruptly.");
        multiplexer_utils::emit_error(&inner.error_tx, &err);

        let delay = match multiplexer_utils::next_reconnect_delay(&inner.policy) {
          Some(delay) => delay,
          None => return client_utils::future_ok(Loop::Break(()))
        };

        debug!("Waiting for {} ms before attempting to reconnect...", delay);

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
        debug!("Redis client closed via Quit.");

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

      debug!("Waiting for {} ms before attempting to reconnect...", delay);

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
