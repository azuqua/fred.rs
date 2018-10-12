#![allow(unused_imports)]

use ::error::{
  RedisError,
  RedisErrorKind
};

use std::io;
use std::io::{
  Error as IoError,
  Cursor
};

use std::sync::Arc;

use std::str;
use std::collections::{
  HashMap
};

use std::fmt::{
  Write
};

use bytes::{
  BytesMut,
  BufMut,
  Buf
};

use super::types::{
  CR,
  LF,
  NULL,
  FrameKind,
  Frame,
  SlotRange,
  REDIS_CLUSTER_SLOTS,
  SlaveNodes,
  RedisCommandKind
};

use crc16::{
  State,
  XMODEM
};

use ::types::{
  RedisValue
};

use std::rc::Rc;

/// Elasticache adds a '@1122' or some number suffix to the CLUSTER NODES response
fn remove_elasticache_suffix(server: String) -> String {
  if let Some(first) = server.split("@").next() {
    return first.to_owned();
  }

  server
}

pub fn binary_search(slots: &Vec<Rc<SlotRange>>, slot: u16) -> Option<Rc<SlotRange>> {
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

  Ok(out)
}

fn first_two_words(s: &str) -> (&str, &str) {
  let mut parts = s.split_whitespace();
  (parts.next().unwrap_or(""), parts.next().unwrap_or(""))
}

pub fn better_error(resp: String) -> RedisError {
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

pub fn frame_to_pubsub(frame: Frame) -> Result<(String, RedisValue), RedisError> {
  if let Frame::Array(mut frames) = frame {
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
      if payload.kind() == FrameKind::BulkString {
        let payload = match payload.into_results() {
          Ok(mut r) => r.pop(),
          Err(e) => return Err(e)
        };

        if payload.is_none() {
          Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub channel payload."))
        }else{
          Ok((channel, payload.unwrap()))
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

pub fn command_args(kind: &RedisCommandKind) -> Option<Frame> {
  let frame = if kind.is_cluster_command() {
    if let Some(arg) = kind.cluster_args() {
      Frame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else if kind.is_client_command() {
    if let Some(arg) = kind.client_args() {
      Frame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else if kind.is_config_command() {
    if let Some(arg) = kind.config_args() {
      Frame::BulkString(arg.into_bytes())
    }else{
      return None;
    }
  }else{
    return None;
  };

  Some(frame)
}

#[cfg(not(feature="ignore-auth-error"))]
pub fn check_auth_error(frame: Frame) -> Frame {
  frame
}

// https://i.imgur.com/RjpUxK4.png
#[cfg(feature="ignore-auth-error")]
pub fn check_auth_error(frame: Frame) -> Frame {
  let is_auth_error = match frame {
    Frame::Error(ref s) => s == "ERR Client sent AUTH, but no password is set",
    _ => false
  };

  if is_auth_error {
    Frame::SimpleString("OK".into())
  }else{
    frame
  }
}

// ------------------

#[cfg(test)]
mod tests {
  use super::*;
  use super::super::types::*;
  use super::super::super::types::*;

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
