
pub mod types;
pub mod utils;

use crate::metrics::SizeStats;
use crate::error::*;

use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  DerefMut,
  Deref
};

use bytes::BytesMut;

use redis_protocol::encode::encode_bytes;
use redis_protocol::decode::decode_bytes;
use redis_protocol::types::Frame as ProtocolFrame;

use tokio_io::codec::{
  Decoder,
  Encoder
};


pub struct RedisCodec {
  name: String,
  req_size_stats: Arc<RwLock<SizeStats>>,
  res_size_stats: Arc<RwLock<SizeStats>>,
}

impl RedisCodec {

  pub fn new(name: String, req_size_stats: Arc<RwLock<SizeStats>>, res_size_stats: Arc<RwLock<SizeStats>>) -> Self {
    RedisCodec {
      name,
      req_size_stats,
      res_size_stats
    }
  }

}

impl Decoder for RedisCodec {
  type Item = ProtocolFrame;
  type Error = RedisError;

  fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ProtocolFrame>, RedisError> {
    trace!("{} Recv {} bytes.", self.name, buf.len());

    if buf.len() < 1 {
      return Ok(None);
    }

    let (frame, amt) = decode_bytes(buf)?;

    if let Some(frame) = frame {
      trace!("{} Parsed {} bytes.", self.name, amt);

      buf.split_to(amt);
      self.res_size_stats.write().deref_mut().sample(amt as u64);

      Ok(Some(utils::check_auth_error(frame)))
    }else{
      Ok(None)
    }
  }

}

impl Encoder for RedisCodec {
  type Item = ProtocolFrame;
  type Error = RedisError;

  fn encode(&mut self, frame: ProtocolFrame, buf: &mut BytesMut) -> Result<(), RedisError> {
    let offset = buf.len();

    let res = encode_bytes(buf, &frame)?;
    let len = res.saturating_sub(offset);

    trace!("{} Encoded {} bytes. Buffer len: {}", self.name, len, res);
    self.req_size_stats.write().deref_mut().sample(len as u64);

    Ok(())
  }

}


