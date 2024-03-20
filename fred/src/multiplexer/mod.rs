
pub mod types;
pub mod utils;
pub mod connection;
pub mod init;

use futures::{
  Future,
  Stream,
  Sink
};
use futures::sync::mpsc::UnboundedSender;
use futures::sync::oneshot::{
  Sender as OneshotSender,
  channel as oneshot_channel
};

use crate::utils as client_utils;

use std::ops::{
  Deref,
  DerefMut
};

use redis_protocol::prelude::*;
use crate::error::*;
use crate::types::{
  RedisConfig,
  RedisValue,
  RedisKey,
  ClientState
};
use crate::protocol::types::{RedisCommand, ClusterKeyCache, RedisCommandKind};

use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;

use std::collections::{
  BTreeMap,
  VecDeque
};

use crate::protocol::types::ResponseSender;
use crate::client::RedisClientInner;
use crate::multiplexer::types::{
  Streams,
  Sinks
};

use std::sync::Arc;
use parking_lot::RwLock;
use std::time::Instant;

pub type LastCommandCaller = OneshotSender<Option<(RedisCommand, RedisError)>>;

/// A struct for multiplexing frames in and out of the TCP socket based on the semantics supported by the Redis API.
///
/// As opposed to the `RedisClient`, this struct directly references the socket(s) and therefore cannot move between threads.
///
/// Most commands in the Redis API follow a simple request-response pattern, however the publish-subscribe
/// interface and bl* commands do not. Due to the fact that a client can switch between these interfaces at will
/// a more complex multiplexing layer is needed than is currently supported via the generic pipelined/multiplexed
/// interfaces in tokio-proto.
#[derive(Clone)]
pub struct Multiplexer {
  /// Whether or not the multiplexer is interacting with a clustered Redis deployment.
  pub clustered: bool,
  /// The inner client state.
  pub inner: Arc<RedisClientInner>,
  /// A reference to the last request sent to the server, including a reference to the oneshot channel used to notify the caller of the response.
  pub last_request: Rc<RefCell<Option<RedisCommand>>>,
  /// The timestamp of the last request sent.
  pub last_request_sent: Rc<RefCell<Option<Instant>>>,
  /// A oneshot sender for the command stream to be notified when it can start processing the next request.
  ///
  /// In the event of a connection reset the listener stream will send the last request and error to the caller to decide whether or not
  /// to replay the last request and/or to backoff and reconnect based on the kind of error surfaced by the network layer.
  pub last_command_callback: Rc<RefCell<Option<LastCommandCaller>>>,
  /// The incoming stream of frames from the Redis server.
  pub streams: Streams,
  /// Outgoing sinks to the Redis server.
  pub sinks: Sinks,
}

impl fmt::Debug for Multiplexer {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{} [Redis Multiplexer]", n!(self.inner))
  }
}

impl Multiplexer {

  pub fn new(inner: &Arc<RedisClientInner>) -> Multiplexer {
    let inner = inner.clone();

    let (streams, sinks, clustered) = {
      let config_guard = inner.config.read();
      let config_ref = config_guard.deref();

      let mut clustered = false;

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
    let last_request = Rc::new(RefCell::new(None));
    let last_request_sent = Rc::new(RefCell::new(None));
    let last_command_callback = Rc::new(RefCell::new(None));

    Multiplexer {
      inner,
      streams,
      sinks,
      clustered,
      last_command_callback,
      last_request,
      last_request_sent
    }
  }

  pub fn is_clustered(&self) -> bool {
    self.clustered
  }

  pub fn set_last_command_callback(&self, caller: Option<LastCommandCaller>) {
    utils::set_option(&self.last_command_callback, caller);
  }

  pub fn take_last_command_callback(&self) -> Option<LastCommandCaller> {
    utils::take_last_command_callback(&self.last_command_callback)
  }

  pub fn set_last_request(&self, cmd: Option<RedisCommand>) {
    utils::set_option(&self.last_request_sent, Some(Instant::now()));
    utils::set_option(&self.last_request, cmd);
  }

  pub fn take_last_request(&self) -> Option<RedisCommand> {
    utils::take_last_request(&self.last_request_sent, &self.inner, &self.last_request)
  }

  /// Send a command to the Redis server(s).
  pub fn write_command(&self, inner: &Arc<RedisClientInner>, request: &mut RedisCommand) -> Box<Future<Item=(), Error=RedisError>> {
    trace!("{} Multiplexer sending command {:?}", n!(inner), request.kind);
    if request.attempted > 0 {
      client_utils::incr_atomic(&inner.redeliver_count);
    }

    request.incr_attempted();

    let no_cluster = request.no_cluster();
    let key = if self.is_clustered() {
      request.extract_key().map(|s| s.to_owned())
    }else{
      None
    };
    let key_slot = match request.kind {
      RedisCommandKind::Scan(ref s) => s.key_slot.clone(),
      _ => None
    };

    let frame = match request.to_frame() {
      Ok(f) => f,
      Err(e) => return client_utils::future_error(e)
    };

    if request.kind == RedisCommandKind::Quit {
      self.sinks.quit(frame)
    }else{
      self.sinks.write_command(key, frame, no_cluster, key_slot)
    }
  }

  /// Listen on the TCP socket(s) for incoming frames.
  ///
  /// The future returned here resolves when the socket is closed.
  pub fn listen(&self) -> Box<Future<Item=(), Error=()>> {
    let inner = self.inner.clone();
    let last_request = self.last_request.clone();
    let last_request_sent = self.last_request_sent.clone();
    let last_command_callback = self.last_command_callback.clone();
    let streams = self.streams.clone();
    let sinks = self.sinks.clone();

    let frame_stream = match self.streams.listen() {
      Ok(stream) => stream,
      Err(e) => {
        // notify the last caller on the command stream that the new stream couldn't be initialized
        error!("{} Could not listen for protocol frames: {:?}", ne!(inner), e);

        let last_command_callback = match self.last_command_callback.borrow_mut().take() {
          Some(tx) => tx,
          None => return client_utils::future_error_generic(())
        };
        let last_command = match self.last_request.borrow_mut().take() {
          Some(cmd) => cmd,
          None => {
            warn!("{} Couldn't find last command on error in multiplexer frame stream.", nw!(inner));
            RedisCommand::new(RedisCommandKind::Ping, vec![], None)
          }
        };

        if let Err(e) = last_command_callback.send(Some((last_command, e))) {
          warn!("{} Error notifying last command callback of the incoming message stream ending.", nw!(inner));
        }

        return client_utils::future_error_generic(());
      }
    };
    let final_self = self.clone();
    let final_inner = self.inner.clone();

    let final_last_request = last_request.clone();
    let final_last_command_callback = last_command_callback.clone();

    Box::new(frame_stream.fold((inner, last_request, last_request_sent, last_command_callback), |memo, frame: Frame| {
      let (inner, last_request, last_request_sent, last_command_callback) = memo;
      trace!("{} Multiplexer stream recv frame.", n!(inner));

      if frame.kind() == FrameKind::Moved || frame.kind() == FrameKind::Ask {
        // pause commands to refresh the cached cluster state
        warn!("{} Recv MOVED or ASK error.", nw!(inner));
        Err(RedisError::new(RedisErrorKind::Cluster, ""))
      }else{
        utils::process_frame(&inner, &last_request, &last_request_sent, &last_command_callback, frame);
        Ok((inner, last_request, last_request_sent, last_command_callback))
      }
    })
    .then(move |mut result| {
      if let Err(ref e) = result {
        warn!("{} Multiplexer frame stream closed with error? {:?}", nw!(final_inner), e);
      }else{
        warn!("{} Multiplexer frame stream closed without error.", nw!(final_inner));
      }

      if let Ok((ref inner, _, _, _)) = result {
        if client_utils::read_client_state(&inner.state) != ClientState::Disconnecting {
          // if the connection died but the state is not Disconnecting then the user didn't `quit`, so this should be handled as an error so a reconnect occurs
          result = Err(RedisError::new(RedisErrorKind::IO, "Connection closed abruptly."));
        }
      }
      client_utils::set_client_state(&final_inner.state, ClientState::Disconnected);

      streams.close();
      sinks.close();

      match result {
        Ok(_) => {
          // notify the caller that this future has finished via the last callback
          let last_command_callback = match final_last_command_callback.borrow_mut().take() {
            Some(tx) => tx,
            None => return Ok(())
          };

          if let Err(e) = last_command_callback.send(None) {
            warn!("{} Error notifying last command callback of the incoming message stream ending.", nw!(final_inner));
          }
          Ok(())
        },
        Err(e) => {
          debug!("{} Handling error on multiplexer frame stream: {:?}", n!(final_inner), e);

          // send a message to the command stream processing loop with the last message and the error when the stream closed
          let last_command_callback = match final_last_command_callback.borrow_mut().take() {
            Some(tx) => tx,
            None => {
              debug!("{} Couldn't find last command callback on error in multiplexer frame stream.", n!(final_inner));

              // since there's no request pending in the command stream we have to send a message via the message queue in order to force a reconnect event to occur.
              if let Some(ref tx) = final_inner.command_tx.read().deref() {
                tx.unbounded_send(RedisCommand::new(RedisCommandKind::_Close, vec![], None));
              }

              return Ok(());
            }
          };
          let last_command = match final_last_request.borrow_mut().take() {
            Some(cmd) => cmd,
            None => {
              warn!("{} Couldn't find last command on error in multiplexer frame stream.", nw!(final_inner));
              return Ok(());
            }
          };

          if let Err(e) = last_command_callback.send(Some((last_command, e))) {
            error!("{} Error notifying the last command callback of the incoming message stream ending with an error.", ne!(final_inner));
          }
          Ok(())
        }
      }
    }))
  }


}





