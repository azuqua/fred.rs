
use futures::channel::oneshot::{
  Sender as OneshotSender
};
use futures::future::Future;
use futures::stream::{
  self,
  Stream,
  StreamExt
};
use futures::sink::Sink;
use futures::stream::{
  SplitSink,
  SplitStream
};
use futures::{
  SinkExt,
  FutureExt,
  TryFutureExt
};

use std::pin::Pin;
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
use parking_lot::{Mutex,RwLock};
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

use tokio_02::net::{
  TcpStream
};

use std::cell::RefCell;

use crate::protocol::RedisCodec;
use crate::protocol::types::ClusterKeyCache;
use tokio_util::codec::Framed;
use tokio_02::io::{AsyncWrite,AsyncRead};

use crate::utils as client_utils;
use crate::multiplexer::utils;
use crate::protocol::types::RedisCommandKind::Sinter;
// use futures::{StartSend, Poll};
use std::task::{Poll, Context};


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

pub type FrameStream = Pin<Box<dyn Stream<Item=Result<Frame, RedisError>> + Send>>;

pub type RedisTcpStream = SplitStream<Framed<TcpStream, RedisCodec>>;
pub type RedisTcpSink = SplitSink<Framed<TcpStream, RedisCodec>, Frame>;

#[cfg(feature="enable-tls")]
pub enum RedisSink {
  Tls(SplitSink<Framed<TlsStream<TcpStream>, RedisCodec>>),
  Tcp(SplitSink<Framed<TcpStream, RedisCodec>>)
}

#[cfg(not(feature="enable-tls"))]
pub enum RedisSink {
  Tls(RedisTcpSink),
  Tcp(RedisTcpSink)
}

impl Sink<Frame> for RedisSink {
  // type SinkItem = Frame;
  type Error = RedisError;

  // FIXME: the mass of Box::pin(inner).as_mut() here seems questionable...

  fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match *self {
      RedisSink::Tls(ref mut inner) => Box::pin(inner).as_mut().poll_ready(cx),
      RedisSink::Tcp(ref mut inner) => Box::pin(inner).as_mut().poll_ready(cx)
    }
  }

  fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
    match *self {
      RedisSink::Tls(ref mut inner) => Box::pin(inner).as_mut().start_send(item),
      RedisSink::Tcp(ref mut inner) => Box::pin(inner).as_mut().start_send(item)
    }
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match *self {
      RedisSink::Tls(ref mut inner) => Box::pin(inner).as_mut().poll_flush(cx),
      RedisSink::Tcp(ref mut inner) => Box::pin(inner).as_mut().poll_flush(cx)
    }
  }

  fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match *self {
      RedisSink::Tls(ref mut inner) => Box::pin(inner).as_mut().poll_close(cx),
      RedisSink::Tcp(ref mut inner) => Box::pin(inner).as_mut().poll_close(cx)
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
  type Item = Result<Frame,RedisError>;
  
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match *self {
      RedisStream::Tls(ref mut inner) => Box::pin(inner).as_mut().poll_next(cx),
      RedisStream::Tcp(ref mut inner) => Box::pin(inner).as_mut().poll_next(cx)
    }
  }

}

async fn send_frame(mut sink: RedisSink, frame: Frame) -> (RedisSink, Result<(),RedisError>) {
  let result = sink.send(frame).await;
  (sink, result)
}

/*
fn send_frame_bad(mut sink: RedisSink, frame: Frame) -> Pin<Box<Future<Output=(RedisSink, Result<(),RedisError>)> + Send>> {
  let ft = sink.send(frame).then(move |result| {
    futures::future::ready((sink, result))
  });
  Box::pin(ft)
}
*/

pub enum Sinks {
  Centralized(Arc<Mutex<Option<RedisSink>>>),
  Clustered {
    cluster_cache: Arc<Mutex<ClusterKeyCache>>,
    sinks: Arc<Mutex<BTreeMap<String, RedisSink>>>
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
  Centralized(Arc<Mutex<Option<RedisStream>>>),
  Clustered(Arc<Mutex<Vec<RedisStream>>>)
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
      let mut guard = old_sink.lock();
      *guard = Some(sink);
    }
  }

  pub fn set_clustered_sink(&self, key: String, sink: RedisSink) {
    if let Sinks::Clustered {ref sinks, ..} = *self {
      let mut guard = sinks.lock();
      guard.insert(key, sink);
    }
  }

  pub fn set_cluster_cache(&self, cache: ClusterKeyCache) {
    if let Sinks::Clustered {ref cluster_cache, ..} = *self {
      let mut guard = cluster_cache.lock();
      *guard = cache;
    }
  }

  pub fn centralized_configs(&self, key: Option<String>) -> Result<Vec<RedisConfig>, RedisError> {
    match *self {
      Sinks::Clustered {ref sinks, ..} => {
        let sinks_guard = sinks.lock();

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
        let _ = sink.lock().take();
      },
      Sinks::Clustered { ref sinks, ref cluster_cache } => {
        {
          sinks.lock().clear();
        }
        {
          cluster_cache.lock().clear();
        }
      }
    };
  }

  #[allow(deprecated)]
  pub fn quit(&self, frame: Frame) -> Pin<Box<dyn Future<Output=Result<(), RedisError>> + Send>> {
    match *self {
      Sinks::Centralized(_) => {
        self.write_command(None, frame, false)
      },
      Sinks::Clustered { ref sinks, .. } => {

        // close all the cluster sockets in parallel
        let (sinks_iter, sinks_len) = {
          let old_sinks = {
            let mut guard = sinks.lock();
            mem::replace(&mut *guard, BTreeMap::new())
          };
          let sinks_len = old_sinks.len();

          let mut out = Vec::with_capacity(sinks_len);
          for (server, sink) in old_sinks.into_iter() {
            out.push((server, sink)); // FIXME: can now just copy here
          }

          (out, sinks_len)
        };

        let quit_ft = stream::iter(sinks_iter)
          .map(move |(server, mut sink)| {
            // FIXME: PR: The original logic used and_then here, so any errors would end up as failed
            // futures, which would presumably *not* end up in the stream fed to the fold, and thus
            // not end up in the final multiplexer state (i.e. they would be lost).
            send_frame(sink, frame.clone()).then(|(sink, _result)| {
              futures::future::ready((server, sink)) // currently ignores result
            })
          })
          .buffer_unordered(sinks_len)
          .fold(sinks.clone(), |sinks_clone, (server, sink)| {
            sinks_clone.lock().insert(server, sink);
            futures::future::ready(sinks_clone)
          })
          .map(|_| Ok(()));

        /*
        let quit_ft = stream::iter(sinks_iter).map(move |(server, sink)| {
          sink.send(frame.clone()).err_into::<RedisError>().and_then(|sink| {
            Ok((server, sink))
          })
          .err_into::<RedisError>()
        })
        */
        /*
        let quit_ft = quit_ft
        .err_into::<RedisError>()
        .buffer_unordered(sinks_len)
        .fold(sinks.clone(), |sinks_clone, (server, sink)| {
          sinks_clone.borrow_mut().insert(server, sink);
          Ok::<_, RedisError>(sinks_clone)
        })
        .map(|_| ());
        */

        Box::pin(quit_ft)
      }
    }
  }

  //pub async fn write_command(&self, key: Option<String>, frame: Frame, no_cluster: bool) -> Result<(),RedisError> {
  pub fn write_command(&self, key: Option<String>, frame: Frame, no_cluster: bool) -> Pin<Box<dyn Future<Output=Result<(),RedisError>> + Send>> {
    let ft = match *self {
      Sinks::Centralized(ref sink) => {
        let owned_sink: RedisSink = {
          match sink.lock().take() {
            Some(s) => s,
            None => {
              return futures::future::err(RedisError::new(
                RedisErrorKind::Unknown, "Redis socket not found."
              )).boxed()
            }
          }
        };

        let sink_copy = sink.clone();

        // FIXME: PR: we now always re-add the sink even if the send failed... that may be wrong...
        send_frame(owned_sink, frame).then(move |(sink, _result)| {
            let mut guard = sink_copy.lock();
            *guard = Some(sink);
            futures::future::ok(())
          }).boxed()
      },
      Sinks::Clustered { ref sinks, ref cluster_cache } => {
        let node = if no_cluster {
          match cluster_cache.lock().random_slot() {
            Some(s) => s,
            None => {
              return futures::future::err(RedisError::new(
                RedisErrorKind::Unknown, "Could not find a valid Redis node for command."
              )).boxed()
            }
          }
        }else{

          // hash the key to find the right redis node
          let key = match key {
            Some(k) => k,
            None => {
              return futures::future::err(RedisError::new(
                RedisErrorKind::Unknown, "Invalid command. (Missing key)."
              )).boxed()
            }
          };

          let slot = redis_keyslot(&key);
          trace!("Mapped key to slot: {:?} -> {:?}", key, slot);

          match cluster_cache.lock().get_server(slot) {
            Some(s) => s,
            None => {
              return futures::future::err(RedisError::new(
                RedisErrorKind::Unknown, "Invalid cluster state. Could not find Redis node for request."
              )).boxed()
            }
          }
        };

        // FIXME: update this comment once we decide if we *need* to keep taking ownership
        // since `send` takes ownership over `self` the sink needs to be removed from the hash
        // and put back after the request has been written to the socket
        let owned_sink = {
          trace!("Using redis node at {}", node.server);
          match sinks.lock().remove(&node.server) {
            Some(s) => s,
            None => {
              return futures::future::err(RedisError::new(
                RedisErrorKind::Unknown, "Could not find Redis socket for cluster node."
              )).boxed()
            }
          }
        };

        let sinks = sinks.clone();

        // FIXME: here, unlike above, we're only re-adding the sink on success
        send_frame(owned_sink,frame).then(move |(sink, result)| {
          if result.is_ok() {
            sinks.lock().insert(node.server.clone(), sink);
          };
          futures::future::ready(result)
        }).boxed()
      }
    };
    Box::pin(ft)
  }
}

impl Streams {

  pub fn close(&self) {
    match *self {
      Streams::Centralized(ref old_stream) => {
        let mut guard = old_stream.lock();
        let _ = guard.take();
      },
      Streams::Clustered(ref streams) => {
        let mut guard = streams.lock();
        guard.clear();
      }
    }
  }

  pub fn add_stream(&self, stream: RedisStream) {
    match *self {
      Streams::Centralized(ref old_stream) => {
        let mut guard = old_stream.lock();
        *guard = Some(stream);
      },
      Streams::Clustered(ref streams) => {
        let mut guard = streams.lock();
        guard.push(stream);
      }
    }
  }

  pub fn listen(&self) -> Result<FrameStream, RedisError> {
    match *self {
      Streams::Centralized(ref stream) => {
        let stream = {
          stream.lock().take()
        };

        match stream {
          Some(stream) => Ok(Box::pin(stream)),
          None => Err(RedisError::new(
            RedisErrorKind::Unknown, "Redis socket not initialized."
          ))
        }
      },
      Streams::Clustered(ref streams) => {
        let mut streams: Vec<RedisStream> = {
          streams.lock().drain(..).collect()  // FIXME: is there a better way to take the vector out?
        };

        // fold all the streams into one
        let memo: Option<FrameStream> = None;

        let merged = streams.drain(..).fold(memo, |memo, next| {
          match memo {
            Some(last) => Some(Box::pin(stream::select(last, next))),
            None => Some(Box::pin(next))
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
