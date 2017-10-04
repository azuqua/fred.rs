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

// sub module so std::io::Read and std::io::BufRead and bytes::Buf traits don't collide on certain methods (take, etc)
mod readers {
  use std::io::prelude::*;
  use std::io::Cursor;
  use bytes::BytesMut;
  use super::{
    CR,
    LF,
    RedisError,
    pop_with_error
  };

  pub fn read_prefix_len(cursor: &mut Cursor<&mut BytesMut>) -> Result<isize, RedisError> {
    let mut len_buf = Vec::new();
    let _ = cursor.read_until(LF as u8, &mut len_buf)?;

    pop_with_error(&mut len_buf, LF)?;
    pop_with_error(&mut len_buf, CR)?;

    let len_str = String::from_utf8(len_buf)?;
    Ok(len_str.parse::<isize>()?)
  }

  pub fn read_to_crlf(cursor: &mut Cursor<&mut BytesMut>) -> Result<Vec<u8>, RedisError> {
    let mut payload = Vec::new();
    cursor.read_until(LF as u8, &mut payload)?;

    // check and remove the last two bytes
    pop_with_error(&mut payload, LF)?;
    pop_with_error(&mut payload, CR)?;

    Ok(payload)
  }

  pub fn read_exact(cursor: &mut Cursor<&mut BytesMut>, len: u64, buf: &mut Vec<u8>) -> Result<usize, RedisError> {
    let mut take = cursor.take(len);
    Ok(take.read_to_end(buf)?)
  }

}

pub fn crc16_xmodem(key: &str) -> u16 {
  State::<XMODEM>::calculate(key.as_bytes()) % REDIS_CLUSTER_SLOTS
}

/// Maps a key to its hash slot.
pub fn redis_crc16(key: &str) -> u16 {
  let (mut i, mut j): (Option<usize>, Option<usize>) = (None, None);

  for (idx, c) in key.chars().enumerate() {
    if c == '{' {
      i = Some(idx);
      break;
    }
  }

  if i.is_none() || (i.is_some() && i.unwrap() == key.len() - 1) {
    return crc16_xmodem(key);
  }

  let i = i.unwrap();
  for (idx, c) in key[i+1..].chars().enumerate() {
    if c == '}' {
      j = Some(idx);
      break;
    }
  }

  if j.is_none() {
    return crc16_xmodem(key);
  }

  let j = j.unwrap();
  if i+j == key.len() || j == 0 {
    crc16_xmodem(key)
  }else{
    crc16_xmodem(&key[i+1..i+j+1])
  }
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
      return Some(slots[mid].clone());
    }
  }

  None
}

#[allow(unused_mut)]
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

      let server = parts[1];
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

      out.insert(server.to_owned(), slots);
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

      let server = parts[1].to_owned();
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

// Extracts the first and rest words of a string and returns them in a tuple.
fn extract_first_word(s: String) -> (String, String) {
  let mut parts = s.split_whitespace();
  let first = match parts.next() {
    Some(s) => s.to_owned(),
    None => "".to_owned()
  };
  let remaining: Vec<String> = parts.map(|s| s.to_owned()).collect();

  (first, remaining.join(" "))
}

pub fn better_error(resp: String) -> RedisError {
  let (first, rest) = extract_first_word(resp.clone());
  match first.as_ref(){
    ""          => RedisError::new(RedisErrorKind::Unknown, "No response!"),
    "ERR"       => RedisError::new(RedisErrorKind::Unknown, rest),
    "WRONGTYPE" => RedisError::new(RedisErrorKind::InvalidArgument, rest),
    "Invalid"   => {
      let (second, rest) = extract_first_word(rest);
      match second.as_ref() {
        "argument(s)" | "Argument" => RedisError::new(RedisErrorKind::InvalidArgument, rest),
        "command" | "Command"      => RedisError::new(RedisErrorKind::InvalidCommand, rest),
        _                          => RedisError::new(RedisErrorKind::Unknown, resp),
      }
    }
    _   => RedisError::new(RedisErrorKind::Unknown, resp)
  }
}

pub fn pop_with_error<T>(d: &mut Vec<T>, expected: char) -> Result<T, RedisError> {
  match d.pop() {
    Some(c) => Ok(c),
    None => Err(RedisError::new(
      RedisErrorKind::Unknown, format!("Missing final byte {}.", expected)
    ))
  }
}

pub fn pop_trailing_crlf(d: &mut Cursor<&mut BytesMut>) -> Result<(), RedisError> {
  if d.remaining() < 2 {
    return Err(RedisError::new(
      RedisErrorKind::Unknown, "Missing final CRLF."
    ));
  }

  let curr_byte = d.get_u8();
  let next_byte = d.get_u8();

  if curr_byte != CR as u8 || next_byte != LF as u8 {
    Err(RedisError::new(
      RedisErrorKind::Unknown, "Missing final CRLF."
    ))
  }else{
    Ok(())
  }
}

pub fn write_crlf(bytes: &mut BytesMut) {
  bytes.put_u8(CR as u8);
  bytes.put_u8(LF as u8);
}

pub fn is_cluster_error(payload: &str) -> Option<Frame> {
  if payload.starts_with("MOVED") {
    // only keep the IP here since this will result in the client's cluster state cache being reset anyways
    let parts: Vec<&str> = payload.split(" ").collect();
    Some(Frame::Moved(parts[2].to_owned()))
  }else if payload.starts_with("ASK") {
    let parts: Vec<&str> = payload.split(" ").collect();
    Some(Frame::Ask(parts[2].to_owned()))
  }else{
    None
  }
}

// sure hope we have enough error messages
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
      None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub message type frame."))
    };

    if message_type == "message" {
      let channel = match channel.to_string() {
        Some(c) => c,
        None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid pubsub channel frame."))
      };

      // the payload is a bulk string on pubsub messages
      if payload.kind() == FrameKind::BulkString {
        let payload = try!(payload.into_results()).pop();

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

pub fn ends_with_crlf(bytes: &mut BytesMut) -> bool {
  let lf = match bytes.get(bytes.len() - 1) {
    Some(b) => *b == LF as u8,
    None => false
  };
  let cr = match bytes.get(bytes.len() - 2) {
    Some(b) => *b == CR as u8,
    None => false
  };

  lf && cr
}

pub fn command_args(kind: &RedisCommandKind) -> Option<Frame> {
  // sure would be nice if `if let` worked with other expressions
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

pub fn check_expected_size(expected: usize, max: &Option<usize>) -> Result<(), RedisError> {
  match *max {
    Some(ref max) => if expected <= *max {
      Ok(())
    }else{
      Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Max value size exceeded. Actual: {}, Max: {}", expected, max)
      ))
    },
    None => Ok(())
  }
}

// this assumes the cursor starts on a new data type byte offset
pub fn bytes_to_frames(cursor: &mut Cursor<&mut BytesMut>, max_size: &Option<usize>) -> Result<Frame, RedisError> {
  if cursor.remaining() < 1 {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError, "Empty frame bytes."
    ));
  }

  let first_byte = cursor.get_u8();
  let data_type = match FrameKind::from_byte(first_byte) {
    Some(d) => d,
    None => return Err(RedisError::new(
      RedisErrorKind::ProtocolError, format!("Invalid first byte {}.", first_byte)
    ))
  };

  match data_type {
    FrameKind::BulkString | FrameKind::Null => {
      let expected_len = readers::read_prefix_len(cursor)?;

      if expected_len == -1 {
        Ok(Frame::Null)
      }else if expected_len >= 0 {
        let _ = check_expected_size(expected_len as usize, max_size)?;

        let mut payload = Vec::with_capacity(expected_len as usize);
        let _ = readers::read_exact(cursor, expected_len as u64, &mut payload)?;

        // there's still trailing CRLF after bulk strings
        pop_trailing_crlf(cursor)?;

        Ok(Frame::BulkString(payload))
      }else{
        Err(RedisError::new(
          RedisErrorKind::ProtocolError, format!("Invalid payload size: {}.", expected_len)
        ))
      }
    },
    FrameKind::Array => {
      let expected_len = readers::read_prefix_len(cursor)?;

      if expected_len == -1 {
        Ok(Frame::Null)
      }else if expected_len >= 0 {
        let _ = check_expected_size(expected_len as usize, max_size)?;

        let mut frames = Vec::with_capacity(expected_len as usize);
        for _ in 0..expected_len {
          frames.push(bytes_to_frames(cursor, max_size)?);
        }

        Ok(Frame::Array(frames))
      }else{
        Err(RedisError::new(
          RedisErrorKind::ProtocolError, format!("Invalid payload size: {}.", expected_len)
        ))
      }
    },
    FrameKind::SimpleString => {
      let payload = readers::read_to_crlf(cursor)?;
      let parsed = String::from_utf8(payload)?;

      Ok(Frame::SimpleString(parsed))
    },
    FrameKind::Error => {
      let payload = readers::read_to_crlf(cursor)?;
      let parsed = String::from_utf8(payload)?;

      let frame = if let Some(frame) = is_cluster_error(&parsed) {
        frame
      }else{
        Frame::Error(parsed)
      };

      Ok(frame)
    },
    FrameKind::Integer => {
      let payload = readers::read_to_crlf(cursor)?;
      let parsed = String::from_utf8(payload)?;

      let int_val: i64 = parsed.parse()?;

      Ok(Frame::Integer(int_val))
    },
    _ => Err(RedisError::new(
      RedisErrorKind::ProtocolError, "Unknown frame."
    ))
  }
}

pub fn frames_to_bytes(frame: &mut Frame, bytes: &mut BytesMut) -> Result<(), RedisError> {
  let frame_byte = frame.kind().to_byte();
 
  match *frame {
    Frame::BulkString(ref mut buf) => {
      let len_str = buf.len().to_string();
      bytes.reserve(1 + len_str.bytes().len() + 2 + buf.len() + 2);

      trace!("Send {:?} bytes", bytes.len());

      bytes.put_u8(frame_byte);
      bytes.write_str(&len_str)?;
      write_crlf(bytes);

      for byte in buf.drain(..) {
        bytes.put_u8(byte);
      }
      write_crlf(bytes);
    },
    Frame::Array(ref mut inner_frames) => {
      let inner_len = inner_frames.len().to_string();
      bytes.reserve(1 + inner_len.bytes().len() + 2);

      trace!("Send {:?} bytes", bytes.len());

      bytes.put_u8(frame_byte);
      bytes.write_str(&inner_len)?;
      write_crlf(bytes);

      for mut inner_frame in inner_frames.drain(..) {
        frames_to_bytes(&mut inner_frame, bytes)?;
      }
      // no trailing crlf here, the inner values add that
    },
    Frame::Null => {
      bytes.reserve(1 + NULL.bytes().len());

      trace!("Send {:?} bytes", bytes.len());

      bytes.put_u8(frame_byte);
      bytes.write_str(NULL)?;
    },
    // only an array, bulk strings, and null values are allowed on outbound frames
    // the caller is responsible for coercing other types to bulk strings on the way out
    _ => return Err(RedisError::new(
      RedisErrorKind::ProtocolError, format!("Invalid outgoing data frame type {:?}.", frame.kind())
    ))
  };

  Ok(())
}

// ------------------

#[cfg(test)]
mod tests {
  use super::*;
  use super::super::types::*;
  use super::super::super::types::*;

  // int tests
  #[test]
  fn should_encode_llen_req_example() {
    let mut args: RedisCommand = RedisCommand {
      kind: RedisCommandKind::LLen,
      args: vec![
        "mylist".into()
      ],
      tx: None,
      m_tx: None
    };
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";

    let mut frame = args.to_frame().unwrap();
    let mut bytes = BytesMut::new();
    frames_to_bytes(&mut frame, &mut bytes).unwrap();

    assert_eq!(bytes, expected.as_bytes());
    assert_eq!(args.args.len(), 0);
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = Frame::Integer(48293);

    let mut bytes: BytesMut = ":48293\r\n".into();
    let mut cursor = Cursor::new(&mut bytes);

    let actual = bytes_to_frames(&mut cursor, &None).unwrap();

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let mut args: RedisCommand = RedisCommand {
      m_tx: None,
      kind: RedisCommandKind::Incr,
      args: vec![
        "mykey".into()
      ],
      tx: None
    };

    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";

    let mut frame = args.to_frame().unwrap();
    let mut bytes = BytesMut::new();
    frames_to_bytes(&mut frame, &mut bytes).unwrap();

    assert_eq!(bytes, expected.as_bytes());
    assert_eq!(args.args.len(), 0);
  }

  #[test]
  fn should_decode_incr_req_example() {
    let expected = Frame::Integer(666);

    let mut bytes: BytesMut = ":666\r\n".into();
    let mut cursor = Cursor::new(&mut bytes);

    let actual = bytes_to_frames(&mut cursor, &None).unwrap();

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let mut args: RedisCommand = RedisCommand {
      m_tx: None,
      kind: RedisCommandKind::BitCount,
      args: vec![
        "mykey".into()
      ],
      tx: None
    };

    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";

    let mut frame = args.to_frame().unwrap();
    let mut bytes = BytesMut::new();
    frames_to_bytes(&mut frame, &mut bytes).unwrap();

    assert_eq!(bytes, expected.as_bytes());
  }

  #[test]
  fn should_correctly_crc16_123456789() {
    let key = "123456789";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_crc16(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_correctly_crc16_with_brackets() {
    let key = "foo{123456789}bar";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_crc16(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_correctly_crc16_with_brackets_no_padding() {
    let key = "{123456789}";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_crc16(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_correctly_crc16_with_invalid_brackets_lhs() {
    let key = "foo{123456789";
    // 288A
    let expected: u16 = 10378;
    let actual = redis_crc16(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_correctly_crc16_with_invalid_brackets_rhs() {
    let key = "foo}123456789";
    // 5B35 = 23349, 23349 % 16383 = 6966
    let expected: u16 = 6966;
    let actual = redis_crc16(key);

    assert_eq!(actual, expected);
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

// string tests
  #[test]
  fn should_decode_simple_string_test() {
    let expected = Frame::SimpleString("string".to_owned());

    let mut bytes: BytesMut = "+string\r\n".into();
    let mut cursor = Cursor::new(&mut bytes);

    let actual = bytes_to_frames(&mut cursor, &None).unwrap();

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_bulk_string_test() {
    let string1 = vec!['f' as u8 ,'o' as u8, 'o' as u8];
    let expected = Frame::BulkString(string1);

    let mut bytes: BytesMut = "$3\r\nfoo\r\n".into();
    let mut cursor = Cursor::new(&mut bytes);

    let actual = bytes_to_frames(&mut cursor, &None).unwrap();

    assert_eq!(actual, expected);
  }

  // array tests
  #[test]
  fn should_decode_array_simple_strings_test() {
    let mut frame_vec = Vec::new();
    frame_vec.push(Frame::SimpleString("Foo".to_owned()));
    frame_vec.push(Frame::SimpleString("Bar".to_owned()));

    let expected = Frame::Array(frame_vec);

    let mut bytes: BytesMut = "*2\r\n+Foo\r\n+Bar\r\n".into();
    let mut cursor = Cursor::new(&mut bytes);

    let actual = bytes_to_frames(&mut cursor, &None).unwrap();

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let mut args: RedisCommand = RedisCommand {
      m_tx: None,
      kind: RedisCommandKind::Watch,
      args: vec![
        "HONOR!".into(),
        "Apple Jacks".into()
      ],
      tx: None
    };

    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nHONOR!\r\n$11\r\nApple Jacks\r\n";

    let mut frame = args.to_frame().unwrap();
    let mut bytes = BytesMut::new();
    frames_to_bytes(&mut frame, &mut bytes).unwrap();

    assert_eq!(bytes, expected.as_bytes());
  }

  #[test]
  fn should_decode_array_bulk_string_test() {
    let string1 = vec!['f' as u8, 'o' as u8, 'o' as u8];
    let string2 = vec!['b' as u8, 'a' as u8, 'r' as u8];

    let mut frame_vec = Vec::new();
    frame_vec.push(Frame::BulkString(string1));
    frame_vec.push(Frame::BulkString(string2));

    let expected = Frame::Array(frame_vec);

    let mut bytes: BytesMut = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".into();
    let mut cursor = Cursor::new(&mut bytes);

    let actual = bytes_to_frames(&mut cursor, &None).unwrap();

    assert_eq!(actual, expected);
  }


  // test cases from afl
  pub mod fuzz {
    use super::*;

    #[test]
    // panicked at 'assertion failed: self.remaining() >= dst.len()'
    fn should_handle_crash_1() {
      // 24 34 80 ff
      let b = vec![
        36 as u8,
        52 as u8,
        128 as u8,
        255 as u8
      ];

      let mut bytes = BytesMut::from(b);
      let mut cursor = Cursor::new(&mut bytes);
      let _ = bytes_to_frames(&mut cursor, &None);
    }

    #[test]
    // fatal runtime error: allocator memory exhausted
    fn should_handle_crash_2() {
      let max = Some(10000);

      // 24 35 35 35 35 35 35 35 35 35 35 35 35 35 35
      let b = vec![
        36 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8,
        53 as u8
      ];

      let mut bytes = BytesMut::from(b);
      let mut cursor = Cursor::new(&mut bytes);
      let _ = bytes_to_frames(&mut cursor, &max);
    }

    #[test]
    // panicked at 'assertion failed: self.remaining() >= dst.len()
    fn should_handle_crash_3() {
      // 2a 35 00 20
      let b = vec![
        42 as u8,
        53 as u8,
        0 as u8,
        32 as u8
      ];

      let mut bytes = BytesMut::from(b);
      let mut cursor = Cursor::new(&mut bytes);
      let _ = bytes_to_frames(&mut cursor, &None);
    }

    #[test]
    // fatal runtime error: allocator memory exhausted
    fn should_handle_crash_4() {
      let max = Some(10000);

      // 2a 31 39 39 39 39 39 39 39 39 39 39 39 39 39 39 30 39 34
      let b = vec![
        42 as u8,
        49 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        57 as u8,
        48 as u8,
        57 as u8,
        52 as u8
      ];

      let mut bytes = BytesMut::from(b);
      let mut cursor = Cursor::new(&mut bytes);
      let _ = bytes_to_frames(&mut cursor, &max);
    }

  }

}
