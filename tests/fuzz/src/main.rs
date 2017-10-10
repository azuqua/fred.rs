#![feature(plugin)]
#![plugin(afl_plugin)]

extern crate fred;
extern crate afl;
extern crate bytes;

use fred::protocol::types::*;
use fred::protocol::utils as protocol_utils;

use bytes::{
  Bytes,
  BytesMut,
  Buf,
  BufMut
};

use std::io::Cursor;

fn main() {
  afl::handle_bytes(|b: Vec<u8>| {
    let mut empty = BytesMut::new();
    let bytes = BytesMut::from(b);
    let _ = protocol_utils::bytes_to_frames(&mut empty, bytes, Some(1000000));
  })
}
