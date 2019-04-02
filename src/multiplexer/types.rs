
use futures::sync::oneshot::{
  Sender as OneshotSender
};
use futures::future::Future;
use futures::stream::{
  self,
  Stream
};
use futures::sink::Sink;
use futures::stream::{
  SplitSink,
  SplitStream
};

use std::fmt;
use std::mem;

use std::collections::{
  VecDeque,
  BTreeMap
};

use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::error::{
  RedisError,
  RedisErrorKind
};

use crate::metrics;
use crate::metrics::{
  SizeStats,
  LatencyStats
};
use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  Deref,
  DerefMut
};

use redis_protocol::types::{
  FrameKind as ProtocolFrameKind,
  Frame as ProtocolFrame,
};

pub use redis_protocol::redis_keyslot;

#[cfg(feature="enable-tls")]
use tokio_tls::{
  TlsConnector as TlsConnectorAsync,
  TlsStream
};
#[cfg(feature="native-tls")]
use native_tls::{
  TlsConnector
};

use tokio_core::net::{
  TcpStream
};

use std::rc::Rc;
use std::cell::RefCell;

use crate::protocol::RedisCodec;
use crate::protocol::types::ClusterKeyCache;
use tokio_io::codec::Framed;
use tokio_io::{AsyncWrite,AsyncRead};

use crate::utils as client_utils;
use crate::multiplexer::utils;
use crate::protocol::types::RedisCommandKind::Sinter;
use futures::{StartSend, Poll};


#[cfg(feature="enable-tls")]
pub type TlsTransports = Vec<(String, Framed<TlsStream<TcpStream>, RedisCodec>)>;

#[cfg(not(feature="enable-tls"))]
pub type TlsTransports = Vec<(String, Framed<TcpStream, RedisCodec>)>;

pub type TcpTransports = Vec<(String, Framed<TcpStream, RedisCodec>)>;


#[derive(Clone)]
pub struct SplitCommand {
  pub tx: Arc<RwLock<Option<OneshotSender<Result<Vec<RedisConfig>, RedisError>>>>>,
  pub key: Option<String>
}

impl SplitCommand {

  pub fn take(&mut self) -> (Option<OneshotSender<Result<Vec<RedisConfig>, RedisError>>>, Option<String>) {
    let mut tx_guard = self.tx.write();
    (tx_guard.deref_mut().take(), self.key.take())
  }

}

impl fmt::Debug for SplitCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[SplitCommand]")
  }
}

impl PartialEq for SplitCommand {
  fn eq(&self, other: &SplitCommand) -> bool {
    self.key == other.key
  }
}

impl Eq for SplitCommand {}

pub type FrameStream = Box<Stream<Item=Frame, Error=RedisError>>;

#[cfg(feature="enable-tls")]
pub enum RedisSink {
  Tls(SplitSink<Framed<TlsStream<TcpStream>, RedisCodec>>),
  Tcp(SplitSink<Framed<TcpStream, RedisCodec>>)
}

#[cfg(not(feature="enable-tls"))]
pub enum RedisSink {
  Tls(SplitSink<Framed<TcpStream, RedisCodec>>),
  Tcp(SplitSink<Framed<TcpStream, RedisCodec>>)
}

impl Sink for RedisSink {
  type SinkItem = Frame;
  type SinkError = RedisError;

  fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
    match *self {
      RedisSink::Tls(ref mut inner) => inner.start_send(item),
      RedisSink::Tcp(ref mut inner) => inner.start_send(item)
    }
  }

  fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
    match *self {
      RedisSink::Tls(ref mut inner) => inner.poll_complete(),
      RedisSink::Tcp(ref mut inner) => inner.poll_complete()
    }
  }

  fn close(&mut self) -> Poll<(), Self::SinkError> {
    match *self {
      RedisSink::Tls(ref mut inner) => inner.close(),
      RedisSink::Tcp(ref mut inner) => inner.close()
    }
  }

}

#[cfg(feature="enable-tls")]
pub enum RedisStream {
  Tls(SplitStream<Framed<TlsStream<TcpStream>, RedisCodec>>),
  Tcp(SplitStream<Framed<TcpStream, RedisCodec>>)
}

#[cfg(not(feature="enable-tls"))]
pub enum RedisStream {
  Tls(SplitStream<Framed<TcpStream, RedisCodec>>),
  Tcp(SplitStream<Framed<TcpStream, RedisCodec>>)
}

impl Stream for RedisStream {
  type Item = Frame;
  type Error = RedisError;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    match *self {
      RedisStream::Tls(ref mut inner) => inner.poll(),
      RedisStream::Tcp(ref mut inner) => inner.poll()
    }
  }

}

pub enum Sinks {
  Centralized(Rc<RefCell<Option<RedisSink>>>),
  Clustered {
    cluster_cache: Rc<RefCell<ClusterKeyCache>>,
    sinks: Rc<RefCell<BTreeMap<String, RedisSink>>>
  }
}

impl Clone for Sinks {
  fn clone(&self) -> Self {
    match *self {
      Sinks::Centralized(ref inner) => Sinks::Centralized(inner.clone()),
      Sinks::Clustered {ref cluster_cache, ref sinks} => Sinks::Clustered {
        cluster_cache: cluster_cache.clone(),
        sinks: sinks.clone()
      }
    }
  }
}

pub enum Streams {
  Centralized(Rc<RefCell<Option<RedisStream>>>),
  Clustered(Rc<RefCell<Vec<RedisStream>>>)
}

impl Clone for Streams {
  fn clone(&self) -> Self {
    match *self {
      Streams::Centralized(ref inner) => Streams::Centralized(inner.clone()),
      Streams::Clustered(ref inner) => Streams::Clustered(inner.clone())
    }
  }
}

impl Sinks {

  pub fn set_centralized_sink(&self, sink: RedisSink) {
    if let Sinks::Centralized(ref old_sink) = *self {
      let mut sink_ref = old_sink.borrow_mut();
      *sink_ref = Some(sink);
    }
  }

  pub fn set_clustered_sink(&self, key: String, sink: RedisSink) {
    if let Sinks::Clustered {ref sinks, ..} = *self {
      sinks.borrow_mut().insert(key, sink);
    }
  }

  pub fn set_cluster_cache(&self, cache: ClusterKeyCache) {
    if let Sinks::Clustered {ref cluster_cache, ..} = *self {
      let mut cache_ref = cluster_cache.borrow_mut();
      *cache_ref = cache;
    }
  }

  pub fn centralized_configs(&self, key: Option<String>) -> Result<Vec<RedisConfig>, RedisError> {
    match *self {
      Sinks::Clustered {ref sinks, ..} => {
        let sinks_guard = sinks.borrow();

        let mut configs = Vec::with_capacity(sinks_guard.len());
        for (ip_str, _) in sinks_guard.iter() {
          let mut parts: Vec<String> = ip_str.split(":").map(|part| {
            part.to_owned()
          }).collect();

          if parts.len() != 2 {
            return Err(RedisError::new(
              RedisErrorKind::Unknown, "Invalid host/port in cluster sink cache."
            ));
          }

          let port = match parts.pop().unwrap().parse::<u16>() {
            Ok(p) => p,
            Err(_) => return Err(RedisError::new(
              RedisErrorKind::Unknown, "Invalid port in cluster sink cache."
            ))
          };

          configs.push(RedisConfig::Centralized {
            host: parts.pop().unwrap(),
            port: port,
            key: key.clone(),
            tls: false
          })
        }

        Ok(configs)
      },
      Sinks::Centralized(_) => Err(RedisError::new(
        RedisErrorKind::Unknown, "Client is not using a clustered deployment."
      ))
    }
  }

  pub fn close(&self) {
    match *self {
      Sinks::Centralized(ref sink) => {
        let _ = sink.borrow_mut().take();
      },
      Sinks::Clustered { ref sinks, ref cluster_cache } => {
        sinks.borrow_mut().clear();
        cluster_cache.borrow_mut().clear();
      }
    };
  }

  #[allow(deprecated)]
  pub fn quit(&self, frame: Frame) -> Box<Future<Item=(), Error=RedisError>> {
    debug!("Sending quit command.");

    match *self {
      Sinks::Centralized(_) => {
        self.write_command(None, frame, false)
      },
      Sinks::Clustered { ref sinks, .. } => {
        // close all the cluster sockets in parallel

        let (sinks_iter, sinks_len) = {
          let mut sinks_ref = sinks.borrow_mut();
          let sinks_len = sinks_ref.len();
          let old_sinks = mem::replace(sinks_ref.deref_mut(), BTreeMap::new());

          let mut out = Vec::with_capacity(old_sinks.len());
          for (server, sink) in old_sinks.into_iter() {
            out.push(Ok::<_, RedisError>((server, sink)));
          }

          (out, sinks_len)
        };

        let quit_ft = stream::iter(sinks_iter).map(move |(server, sink)| {
          sink.send(frame.clone()).from_err::<RedisError>().and_then(|sink| {
            Ok((server, sink))
          })
          .from_err::<RedisError>()
        })
        .from_err::<RedisError>()
        .buffer_unordered(sinks_len)
        .fold(sinks.clone(), |sinks_clone, (server, sink)| {
          sinks_clone.borrow_mut().insert(server, sink);
          Ok::<_, RedisError>(sinks_clone)
        })
        .map(|_| ());

        Box::new(quit_ft)
      }
    }
  }

  pub fn write_command(&self, key: Option<String>, frame: Frame, no_cluster: bool) -> Box<Future<Item=(), Error=RedisError>> {

    match *self {
      Sinks::Centralized(ref sink) => {
        let owned_sink = {
          let mut sink_ref = sink.borrow_mut();

          match sink_ref.take() {
            Some(s) => s,
            None => {
              return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Redis socket not found."
              ))
            }
          }
        };

        let sink_copy = sink.clone();

        Box::new(owned_sink.send(frame)
          .map_err(|e| e.into())
          .and_then(move |sink| {
            let mut sink_ref = sink_copy.borrow_mut();
            *sink_ref = Some(sink);

            Ok(())
          }))
      },
      Sinks::Clustered { ref sinks, ref cluster_cache } => {
        let node = if no_cluster {
          let cluster_cache_ref = cluster_cache.borrow();

          match cluster_cache_ref.random_slot() {
            Some(s) => s,
            None => {
              return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Could not find a valid Redis node for command."
              ))
            }
          }
        }else{
          let cluster_cache_ref = cluster_cache.borrow();

          // hash the key to find the right redis node
          let key = match key {
            Some(k) => k,
            None => {
              return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Invalid command. (Missing key)."
              ))
            }
          };

          let slot = redis_keyslot(&key);
          trace!("Mapped key to slot: {:?} -> {:?}", key, slot);

          match cluster_cache_ref.get_server(slot) {
            Some(s) => s,
            None => {
              return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Invalid cluster state. Could not find Redis node for request."
              ))
            }
          }
        };

        // since `send` takes ownership over `self` the sink needs to be removed from the hash
        // and put back after the request has been written to the socket
        let owned_sink = {
          let mut sinks_ref = sinks.borrow_mut();

          trace!("Using redis node at {}", node.server);
          match sinks_ref.remove(&node.server) {
            Some(s) => s,
            None => {
              return client_utils::future_error(RedisError::new(
                RedisErrorKind::Unknown, "Could not find Redis socket for cluster node."
              ))
            }
          }
        };

        let sinks = sinks.clone();

        Box::new(owned_sink.send(frame)
          .map_err(|e| e.into())
          .and_then(move |sink| {
            let mut sinks_ref = sinks.borrow_mut();
            sinks_ref.insert(node.server.clone(), sink);

            Ok(())
          }))
      }
    }
  }

}

impl Streams {

  pub fn close(&self) {
    match *self {
      Streams::Centralized(ref old_stream) => {
        let mut stream_ref = old_stream.borrow_mut();
        let _ = stream_ref.take();
      },
      Streams::Clustered(ref streams) => {
        let mut streams_ref = streams.borrow_mut();
        streams_ref.clear();
      }
    }
  }

  pub fn add_stream(&self, stream: RedisStream) {
    match *self {
      Streams::Centralized(ref old_stream) => {
        let mut stream_ref = old_stream.borrow_mut();
        *stream_ref = Some(stream);
      },
      Streams::Clustered(ref streams) => {
        let mut streams_ref = streams.borrow_mut();
        streams_ref.push(stream);
      }
    }
  }

  pub fn listen(&self) -> Result<FrameStream, RedisError> {
    match *self {
      Streams::Centralized(ref stream) => {
        let mut stream_ref = stream.borrow_mut();

        match stream_ref.take() {
          Some(stream) => Ok(Box::new(stream)),
          None => Err(RedisError::new(
            RedisErrorKind::Unknown, "Redis socket not initialized."
          ))
        }
      },
      Streams::Clustered(ref streams) => {
        let mut streams_ref = streams.borrow_mut();

        // fold all the streams into one
        let memo: Option<FrameStream> = None;

        let merged = streams_ref.drain(..).fold(memo, |memo, stream| {
          match memo {
            Some(last) => Some(Box::new(last.select(stream))),
            None => Some(Box::new(stream))
          }
        });

        match merged {
          Some(stream) => Ok(stream),
          None => Err(RedisError::new(
            RedisErrorKind::Unknown, "Redis sockets not initialized."
          ))
        }
      }
    }
  }

}
