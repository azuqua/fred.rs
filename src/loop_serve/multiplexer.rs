
use std::mem;
use std::sync::Arc;
use std::ops::{
  DerefMut
};

use super::utils;
use types::*;
use protocol::types::*;

use std::fmt;

use error::{
  RedisError,
  RedisErrorKind
};

use ::utils as client_utils;
use ::protocol::utils as protocol_utils;

use futures::Future;
use futures::sync::oneshot::{
  Sender as OneshotSender
};
use futures::sync::mpsc::{
  UnboundedSender
};
use futures::stream::Stream;
use futures::stream;
use futures::sink::Sink;

use parking_lot::{
  RwLock
};

use super::{
  RedisSink,
  RedisStream
};

use std::rc::Rc;
use std::cell::RefCell;

use std::collections::{
  VecDeque,
  BTreeMap
};

use ::metrics::{
  SizeTracker,
  LatencyTracker
};

type FrameStream = Box<Stream<Item=Frame, Error=RedisError>>;
type QuitFuture = Box<Future<Item=(String, RedisSink), Error=RedisError>>;

pub enum Sinks {
  Centralized(Rc<RefCell<Option<RedisSink>>>),
  Clustered {
    cluster_cache: Rc<RefCell<ClusterKeyCache>>,
    sinks: Rc<RefCell<BTreeMap<String, RedisSink>>>
  }
}

pub enum Streams {
  Centralized(Rc<RefCell<Option<RedisStream>>>),
  Clustered(Rc<RefCell<Vec<RedisStream>>>)
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
      let mut sinks_ref = sinks.borrow_mut();
      sinks_ref.insert(key, sink);
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
            max_value_size: None
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
        let mut sink_ref = sink.borrow_mut();
        let _ = sink_ref.take();
      },
      Sinks::Clustered {ref sinks, ref cluster_cache} => {
        {
          let mut sinks_ref = sinks.borrow_mut();
          sinks_ref.clear();
        }
        {
          let mut cluster_ref = cluster_cache.borrow_mut();
          cluster_ref.clear();
        }
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

          let iter: Vec<Result<(String, RedisSink), RedisError>> = old_sinks.into_iter().map(|(server, sink)| {
            Ok::<(String, RedisSink), RedisError>((server, sink))
          }).collect();

          (iter, sinks_len)
        };

        let quit_ft = stream::iter(sinks_iter).map(move |(server, sink): (String, RedisSink)| {
          sink.send(frame.clone()).from_err::<RedisError>().and_then(|sink| {
            Ok((server, sink))
          })
          .from_err::<RedisError>()
        })
        .from_err::<RedisError>()
        .buffer_unordered(sinks_len)
        .fold(sinks.clone(), |sinks_clone, (server, sink)| {
          {
            let mut sinks_ref = sinks_clone.borrow_mut();
            sinks_ref.insert(server, sink);
          }

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
        let _guard = flame_start!("redis:write_command:1");
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
            let _guard = flame_start!("redis:write_command:2");

            let mut sink_ref = sink_copy.borrow_mut();
            *sink_ref = Some(sink);

            Ok(())
          }))
      },
      Sinks::Clustered { ref sinks, ref cluster_cache } => {
        let _guard = flame_start!("redis:write_command:3");

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

          let slot = protocol_utils::redis_crc16(&key);
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
            let _guard = flame_start!("redis:write_command:4");

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

/// A struct for multiplexing frames in and out of the TCP socket based on the semantics supported by the Redis API.
///
/// Most commands in the Redis API follow a simple request-response pattern, however the publish-subscribe
/// interface and bl* commands do not. Due to the fact that a client can switch between these interfaces at will
/// a more complex multiplexing layer is needed than is currently supported via the generic pipelined/multiplexed
/// interfaces supported by tokio-proto.
pub struct Multiplexer {
  clustered: bool,
  config: Rc<RefCell<RedisConfig>>,
  pub message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  pub error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  pub command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  pub state: Arc<RwLock<ClientState>>,

  // oneshot sender for the actual result to be sent to the caller
  pub last_request: RefCell<ResponseSender>,
  pub last_request_sent: RefCell<Option<i64>>,
  // oneshot sender for the command stream to be notified when it can start processing the next request
  pub last_caller: RefCell<Option<OneshotSender<RefreshCache>>>,

  pub streams: Streams,
  pub sinks: Sinks,

  /// Latency metrics tracking, enabled with the feature `metrics`.
  pub latency_stats: Arc<RwLock<LatencyTracker>>,
  /// Payload size metrics, enabled with the feature `metrics`.
  pub size_stats: Arc<RwLock<SizeTracker>>
}

impl fmt::Debug for Multiplexer {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Multiplexer]")
  }
}

impl Multiplexer {

  pub fn new(config: Rc<RefCell<RedisConfig>>,
             message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
             error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
             command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
             state: Arc<RwLock<ClientState>>,
             latency: Arc<RwLock<LatencyTracker>>,
             size: Arc<RwLock<SizeTracker>>)
    -> Rc<Multiplexer>
  {

    let (streams, sinks, clustered) = {
      let config_ref = config.borrow();
      let mut clustered = false;

      // since the `Multiplexer` is wrapped with a `Rc` mutability must be contained inside each inner function
      let streams = match *config_ref {
        RedisConfig::Centralized { .. } => Streams::Centralized(Rc::new(RefCell::new(None))),
        RedisConfig::Clustered { .. } => Streams::Clustered(Rc::new(RefCell::new(Vec::new())))
      };
      let sinks = match *config_ref {
        RedisConfig::Centralized { .. } => {
          Sinks::Centralized(Rc::new(RefCell::new(None)))
        },
        RedisConfig::Clustered { .. } => {
          clustered = true;

          Sinks::Clustered {
            // safe b/c when the first arg is None nothing runs that could return an error.
            // see the `ClusterKeyCache::new()` definition
            cluster_cache: Rc::new(RefCell::new(ClusterKeyCache::new(None).unwrap())),
            sinks: Rc::new(RefCell::new(BTreeMap::new()))
          }
        }
      };

      (streams, sinks, clustered)
    };

    Rc::new(Multiplexer {
      clustered: clustered,
      message_tx: message_tx,
      error_tx: error_tx,
      command_tx: command_tx,
      state: state,
      last_request: RefCell::new(None),
      last_request_sent: RefCell::new(None),
      last_caller: RefCell::new(None),
      config: config,
      streams: streams,
      sinks: sinks,
      latency_stats: latency,
      size_stats: size
    })
  }

  pub fn is_clustered(&self) -> bool {
    self.clustered
  }

  pub fn close_commands(&self) -> Result<(), RedisError> {
    let tx_opt = {
      let mut tx_ref = self.command_tx.borrow_mut();
      tx_ref.take()
    };

    match tx_opt {
      Some(command_tx) => {
        debug!("Closing command tx on multiplexer.");

        let command = RedisCommand::new(RedisCommandKind::_Close, vec![], None);
        command_tx.unbounded_send(command).map_err(|e| {
          RedisError::new(RedisErrorKind::Unknown, format!("Could not send close command. {:?}", e))
        })
      },
      None => Ok(())
    }
  }

  pub fn set_last_caller(&self, caller: Option<OneshotSender<RefreshCache>>) {
    utils::set_last_caller(&self.last_caller, caller)
  }

  pub fn take_last_caller(&self) -> Option<OneshotSender<RefreshCache>> {
    utils::take_last_caller(&self.last_caller)
  }

  pub fn set_last_request(&self, sender: ResponseSender) {
    utils::set_last_request_sent_now(&self.last_request_sent);
    utils::set_last_request(&self.last_request, sender);
  }

  pub fn take_last_request(&self) -> ResponseSender {
    utils::sample_latency(&self.last_request_sent, &self.latency_stats);
    utils::take_last_request(&self.last_request)
  }

  /// Listen on the TCP socket(s) for incoming frames. Since the multiplexer instance is used for managing
  /// both incoming and outgoing frames it's necessary for this function to use an `Rc<Multiplexer>` instead
  /// of `self`. The `new()` function returns a `Rc<Multiplexer>` instead of just a `Multiplexer` for
  /// this reason.
  ///
  /// The future returned here resolves when the socket is closed.
  pub fn listen(multiplexer: Rc<Multiplexer>) -> Box<Future<Item=Rc<Multiplexer>, Error=RedisError>> {
    let frame_stream = match multiplexer.streams.listen() {
      Ok(stream) => stream,
      Err(e) => return client_utils::future_error(e)
    };

    Box::new(frame_stream.fold(multiplexer, |multiplexer, frame: Frame| {
      let _guard = flame_start!("redis:listen_frame");
      trace!("Multiplexer stream recv frame.");

      let res = if frame.kind() == FrameKind::Moved || frame.kind() == FrameKind::Ask {
        // pause commands to refresh the cached cluster state
        let _ = multiplexer.close_commands();

        Err::<Rc<Multiplexer>, RedisError>(RedisError::new(
          RedisErrorKind::Cluster, ""
        ))
      }else{
        utils::process_frame(&multiplexer, frame);
        Ok::<Rc<Multiplexer>, RedisError>(multiplexer)
      };

      res
    }))
  }

  /// Send a command to the Redis server(s).
  pub fn write_command(&self, mut request: RedisCommand) -> Box<Future<Item=(), Error=RedisError>> {
    let _guard = flame_start!("redis:write_command");
    trace!("Multiplexer sending command {:?}", request.kind);

    let no_cluster = request.no_cluster();
    let key = if self.is_clustered() {
      request.extract_key().map(|s| s.to_owned())
    }else{
      None
    };

    let frame = match request.to_frame() {
      Ok(f) => f,
      Err(e) => {
        return client_utils::future_error(e);
      }
    };

    let res = if request.kind == RedisCommandKind::Quit {
      self.sinks.quit(frame)
    }else{
      self.sinks.write_command(key, frame, no_cluster)
    };

    res
  }

}
