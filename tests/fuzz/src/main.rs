#![feature(plugin)]
#![plugin(afl_plugin)]

extern crate redis_client;
extern crate afl;
extern crate bytes;

use redis_client::protocol::types::*;
use redis_client::protocol::utils as protocol_utils;

use bytes::{
  Bytes,
  BytesMut,
  Buf,
  BufMut
};

use std::io::Cursor;

fn main() {
  afl::handle_bytes(|b: Vec<u8>| {
    let mut bytes = BytesMut::from(b);
    let mut cursor = Cursor::new(&mut bytes);
    let _ = protocol_utils::bytes_to_frames(&mut cursor);
  })
}
