

use std::rc::Rc;
use std::cell::{RefCell, Ref};

use std::sync::Arc;
use parking_lot::RwLock;

use std::ops::{
  DerefMut,
  Deref
};
use std::cmp;
use std::i64::MAX;

use std::time::Instant;
use crate::metrics::LatencyStats;

use futures::{
  Stream,
  Future,
  Sink
};
use futures::sync::oneshot::Sender as OneshotSender;

use redis_protocol::prelude::*;

use crate::multiplexer::{Multiplexer, LastCommandCaller};
use crate::error::*;
use crate::utils as client_utils;
use crate::protocol::utils as protocol_utils;
use crate::protocol::types::ResponseKind;

use std::collections::{
  BTreeMap,
  BTreeSet,
  VecDeque
};
use std::mem;
use crate::protocol::types::RedisCommand;
use crate::client::{RedisClientInner, RedisClient};
use crate::types::{RedisConfig, ReconnectPolicy, RedisValue};
use futures::sync::mpsc::UnboundedSender;

pub fn set_option<T>(opt: &Rc<RefCell<Option<T>>>, val: Option<T>)  {
  let mut _ref = opt.borrow_mut();
  *_ref = val;
}

pub fn take_option<T>(opt: &Rc<RefCell<Option<T>>>) -> Option<T> {
  opt.borrow_mut().take()
}

pub fn set_command_tx(inner: &Arc<RedisClientInner>, tx: UnboundedSender<RedisCommand>) {
  let mut guard = inner.command_tx.write();
  let mut guard_ref = guard.deref_mut();
  *guard_ref = Some(tx);
}

pub fn sample_latency(sent: &Rc<RefCell<Option<Instant>>>, tracker: &RwLock<LatencyStats>) {
  let sent = match take_option(sent) {
    Some(i) => i,
    None => return
  };
  let dur = Instant::now().duration_since(sent);
  let dur_ms = cmp::max(0, (dur.as_secs() * 1000) + dur.subsec_millis() as u64) as i64;

  tracker.write().deref_mut().sample(dur_ms);
}

pub fn take_last_command_callback(cmd: &Rc<RefCell<Option<LastCommandCaller>>>) -> Option<LastCommandCaller> {
  take_option(cmd)
}

pub fn take_last_request(time: &Rc<RefCell<Option<Instant>>>, inner: &Arc<RedisClientInner>, req: &Rc<RefCell<Option<RedisCommand>>>) -> Option<RedisCommand> {
  sample_latency(time, &inner.latency_stats);
  take_option(req)
}

pub fn tuple_to_addr_str(host: &str, port: u16) -> String {
  format!("{}:{}", host, port)
}

pub fn next_reconnect_delay(policy: &RwLock<Option<ReconnectPolicy>>) -> Option<u32> {
  let mut guard = policy.write();
  let guard_ref = guard.deref_mut();

  match *guard_ref {
    Some(ref mut p) => p.next_delay(),
    None => None
  }
}

// grab the redis host/port string
pub fn read_centralized_host(config: &RwLock<RedisConfig>) -> Result<String, RedisError> {
  let config_ref = config.read();

  match *config_ref.deref() {
    RedisConfig::Centralized { ref host, ref port, .. } => {
      Ok(vec![host.clone(), port.to_string()].join(":"))
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown, "Invalid redis config. Centralized config expected."
    ))
  }
}

// read all the (host, port) tuples in the config
pub fn read_clustered_hosts(config: &RwLock<RedisConfig>) -> Result<Vec<(String, u16)>, RedisError> {
  let config_guard = config.read();

  match *config_guard.deref() {
    RedisConfig::Clustered { ref hosts, .. } => Ok(hosts.clone()),
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown, "Invalid redis config. Clustered config expected."
    ))
  }
}

pub fn read_auth_key(config: &RwLock<RedisConfig>) -> Option<String> {
  let config_guard = config.read();

  match *config_guard.deref() {
    RedisConfig::Centralized { ref key, .. } => match *key {
      Some(ref s) => Some(s.to_owned()),
      None => None
    },
    RedisConfig::Clustered { ref key, .. } => match *key {
      Some(ref s) => Some(s.to_owned()),
      None => None
    }
  }
}

pub fn close_error_tx(error_tx: &RwLock<VecDeque<UnboundedSender<RedisError>>>) {
  let mut error_tx_guard = error_tx.write();

  for mut error_tx in error_tx_guard.deref_mut().drain(..) {
    debug!("Closing error tx.");

    let _ = error_tx.close();
  }
}

pub fn close_reconnect_tx(reconnect_tx: &RwLock<VecDeque<UnboundedSender<RedisClient>>>) {
  let mut reconnect_tx_guard = reconnect_tx.write();

  for mut reconnect_tx in reconnect_tx_guard.deref_mut().drain(..) {
    debug!("Closing reconnect tx.");

    let _ = reconnect_tx.close();
  }
}

pub fn close_messages_tx(messages_tx: &RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>) {
  let mut messages_tx_guard = messages_tx.write();

  for mut messages_tx in messages_tx_guard.deref_mut().drain(..) {
    debug!("Closing messages tx.");

    let _ = messages_tx.close();
  }
}

pub fn close_connect_tx(connect_tx: &RwLock<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>) {
  debug!("Closing connection tx.");

  let mut connect_tx_guard = connect_tx.write();
  let mut connect_tx_ref = connect_tx_guard.deref_mut();

  if connect_tx_ref.len() > 0 {
    for tx in connect_tx_ref.drain(..) {
      let _ = tx.send(Err(RedisError::new_canceled()));
    }
  }
}

pub fn emit_error(tx: &RwLock<VecDeque<UnboundedSender<RedisError>>>, error: &RedisError) {
  let mut tx_guard = tx.write();
  let mut tx_ref = tx_guard.deref_mut();

  let new_tx = tx_ref.drain(..).filter(|tx| {
    debug!("Emitting error.");

    match tx.unbounded_send(error.clone()) {
      Ok(_) => true,
      Err(_) => false
    }
  })
  .collect();

  *tx_ref = new_tx;
}

pub fn emit_reconnect(reconnect_tx: &RwLock<VecDeque<UnboundedSender<RedisClient>>>, client: &RedisClient) {
  let mut tx_guard = reconnect_tx.write();
  let mut tx_ref = tx_guard.deref_mut();

  let new_tx = tx_ref.drain(..).filter(|tx| {
    debug!("Emitting reconnect.");

    match tx.unbounded_send(client.clone()) {
      Ok(_) => true,
      Err(_) => false
    }
  })
  .collect();

  *tx_ref = new_tx;
}

pub fn emit_connect(connect_tx: &RwLock<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>, client: &RedisClient) {
  debug!("Emitting connect.");

  let mut connect_tx_guard = connect_tx.write();

  for tx in connect_tx_guard.deref_mut().drain(..) {
    let _ = tx.send(Ok(client.clone()));
  }
}

pub fn emit_connect_error(connect_tx: &RwLock<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>, err: &RedisError) {
  debug!("Emitting connect error.");

  let mut connect_tx_guard = connect_tx.write();

  for tx in connect_tx_guard.deref_mut().drain(..) {
    let _ = tx.send(Err(err.clone()));
  }
}

pub fn last_command_has_multiple_response(last_request: &Rc<RefCell<Option<RedisCommand>>>) -> bool {
  last_request.borrow()
    .deref()
    .as_ref()
    .map(|c| c.kind.has_multiple_response_kind())
    .unwrap_or(false)
}

pub fn last_command_has_blocking_response(last_request: &Rc<RefCell<Option<RedisCommand>>>) -> bool {
  last_request.borrow()
    .deref()
    .as_ref()
    .map(|c| c.kind.has_blocking_response_kind())
    .unwrap_or(false)
}

pub fn merge_multiple_frames(frames: &mut VecDeque<Frame>) -> Frame {
  let inner_len = frames.iter().fold(0, |count, frame| {
    count + match frame {
      Frame::Array(ref inner) => inner.len(),
      _ => 1
    }
  });

  let mut out = Vec::with_capacity(inner_len);

  for frame in frames.drain(..) {
    match frame {
      Frame::Array(inner) => {
        for inner_frame in inner.into_iter() {
          out.push(inner_frame);
        }
      },
      _ => out.push(frame)
    };
  }

  Frame::Array(out)
}

pub fn process_frame(inner: &Arc<RedisClientInner>,
                     last_request: &Rc<RefCell<Option<RedisCommand>>>,
                     last_request_sent: &Rc<RefCell<Option<Instant>>>,
                     last_command_callback: &Rc<RefCell<Option<LastCommandCaller>>>,
                     frame: Frame)
{
  if frame.is_pubsub_message() {
    trace!("{} Processing pubsub message", n!(inner));

    let (channel, message) = match protocol_utils::frame_to_pubsub(frame) {
      Ok((c, m)) => (c, m),
      // TODO or maybe send to error stream
      Err(_) => return
    };

    let mut to_remove = BTreeSet::new();
    {
      let message_tx_guard = inner.message_tx.read();
      let message_tx_ref = message_tx_guard.deref();

      // try to do this such that the channel and message only cloned len() times, not len() + 1
      // while also checking for closed receivers during iteration
      let to_send = message_tx_ref.len() - 1;

      for idx in 0..to_send {
        // send clones
        let tx = match message_tx_ref.get(idx) {
          Some(t) => t,
          None => continue
        };

        if let Err(_) = tx.unbounded_send((channel.clone(), message.clone())) {
          to_remove.insert(idx);
        }
      }

      // send original values
      if let Some(ref tx) = message_tx_ref.get(to_send) {
        if let Err(_) = tx.unbounded_send((channel, message)) {
          to_remove.insert(to_send);
        }
      }
    }
    // remove any senders where the receiver was closed
    if to_remove.len() > 0 {
      let mut message_tx_guard = inner.message_tx.write();
      let mut message_tx_ref = message_tx_guard.deref_mut();

      let mut new_listeners = VecDeque::new();
      for (idx, listener) in message_tx_ref.drain(..).enumerate() {
        if !to_remove.contains(&idx) {
          new_listeners.push_back(listener);
        }
      }

      mem::replace(message_tx_ref, new_listeners);
    }
  }else if last_command_has_multiple_response(last_request) {
    let frames = {
      let mut last_request_ref = last_request.borrow_mut();

      if let Some(ref mut last_request) = last_request_ref.deref_mut() {
        let mut response_kind = match last_request.kind.response_kind_mut() {
          Some(k) => k,
          None => {
            warn!("{} Tried to read response kind but it didn't exist.", nw!(inner));
            return;
          }
        };

        if let ResponseKind::Multiple { ref count, ref mut buffer } = response_kind {
          buffer.push_back(frame);

          if buffer.len() < *count {
            // move on
            trace!("{} Waiting for {} more frames from request with multiple responses.", n!(inner), count - buffer.len());
            None
          }else{
            trace!("{} Merging {} frames into one.", n!(inner), buffer.len());
            // consolidate all the frames into one array frame
            Some(merge_multiple_frames(buffer))
          }
        }else {
          warn!("{} Invalid multiple response kind on last command.", nw!(inner));
          return;
        }
      }else{
        warn!("{} Tried to borrow last request when it didn't exist.", nw!(inner));
        return;
      }
    };

    if let Some(frames) = frames {
      if let Some(m_tx) = take_last_command_callback(last_command_callback) {
        trace!("{} Found last caller from multiplexer after multiple frames.", n!(inner));
        let _ = m_tx.send(None);
      }

      let last_request_tx = match take_last_request(last_request_sent, inner, last_request) {
        Some(s) => match s.tx {
          Some(tx) => tx,
          None => return
        },
        None => return
      };
      trace!("{} Responding to last request with frame containing multiple frames.", n!(inner));

      let _ = last_request_tx.send(Ok(frames));
    }
  }else if last_command_has_blocking_response(last_request) {

    error!("{} Using unimplemented blocking response kind.", ne!(inner));
  }else{
    if let Some(m_tx) = take_last_command_callback(last_command_callback) {
      trace!("{} Found last caller from multiplexer.", n!(inner));
      let _ = m_tx.send(None);
    }

    let last_request_tx = match take_last_request(last_request_sent, inner, last_request) {
      Some(s) => match s.tx {
        Some(tx) => tx,
        None => return
      },
      None => return
    };
    trace!("{} Responding to last request with frame.", n!(inner));

    let _ = last_request_tx.send(Ok(frame));
  }
}