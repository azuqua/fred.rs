

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
use futures::channel::oneshot::Sender as OneshotSender;

use redis_protocol::prelude::*;

use crate::multiplexer::{Multiplexer, LastCommandCaller};
use crate::error::*;
use crate::utils as client_utils;
use crate::protocol::utils as protocol_utils;
use crate::protocol::types::{ResponseKind, ValueScanInner, ValueScanResult};

use std::collections::{
  BTreeMap,
  BTreeSet,
  VecDeque
};
use std::mem;
use crate::protocol::types::{RedisCommand, RedisCommandKind};
use crate::client::{RedisClientInner, RedisClient};
use crate::types::{RedisConfig, ReconnectPolicy, RedisValue, RedisKey, ScanResult, ZScanResult, SScanResult, HScanResult};
use futures::channel::mpsc::UnboundedSender;
use crate::protocol::utils::frame_to_single_result;
use crate::utils::send_command;

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

pub fn last_command_is_key_scan(last_request: &Rc<RefCell<Option<RedisCommand>>>) -> bool {
  last_request.borrow()
    .deref()
    .as_ref()
    .map(|c| c.kind.is_scan())
    .unwrap_or(false)
}

pub fn last_command_is_value_scan(last_request: &Rc<RefCell<Option<RedisCommand>>>) -> bool {
  last_request.borrow()
    .deref()
    .as_ref()
    .map(|c| c.kind.is_value_scan())
    .unwrap_or(false)
}

pub fn update_scan_cursor(last_request: &mut RedisCommand, cursor: String) {
  if last_request.kind.is_scan() {
    mem::replace(&mut last_request.args[0], cursor.clone().into());
  }else if last_request.kind.is_value_scan() {
    mem::replace(&mut last_request.args[1], cursor.clone().into());
  }

  let mut old_cursor = match last_request.kind {
    RedisCommandKind::Scan(ref mut inner) => &mut inner.cursor,
    RedisCommandKind::Hscan(ref mut inner) => &mut inner.cursor,
    RedisCommandKind::Sscan(ref mut inner) => &mut inner.cursor,
    RedisCommandKind::Zscan(ref mut inner) => &mut inner.cursor,
    _ => return
  };

  mem::replace(old_cursor, cursor);
}

pub fn handle_key_scan_result(mut frame: Frame) -> Result<(String, Vec<RedisKey>), RedisError> {
  if let Frame::Array(mut frames) = frame {
    if frames.len() == 2 {
      let cursor = match frames[0].to_string() {
        Some(s) => s,
        None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected first SCAN result element to be a bulk string."))
      };

      if let Some(Frame::Array(results)) = frames.pop() {
        let mut keys = Vec::with_capacity(results.len());

        for frame in results.into_iter() {
          let key = match frame.to_string() {
            Some(s) => s,
            None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected an array of strings from second SCAN result."))
          };

          keys.push(RedisKey::new(key));
        }

        Ok((cursor, keys))
      }else{
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected second SCAN result element to be an array."))
      }
    }else{
      Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected two-element bulk string array from SCAN."))
    }
  }else{
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected bulk string array from SCAN."))
  }
}

pub fn handle_value_scan_result(mut frame: Frame) -> Result<(String, Vec<RedisValue>), RedisError> {
  if let Frame::Array(mut frames) = frame {
    if frames.len() == 2 {
      let cursor = match frames[0].to_string() {
        Some(s) => s,
        None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected first result element to be a bulk string."))
      };

      if let Some(Frame::Array(results)) = frames.pop() {
        let mut values = Vec::with_capacity(results.len());

        for frame in results.into_iter() {
          let value = match frame_to_single_result(frame) {
            Ok(v) => v,
            Err(e) => return Err(e)
          };

          values.push(value);
        }

        Ok((cursor, values))
      }else{
        Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected second result element to be an array."))
      }
    }else{
      Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected two-element bulk string array."))
    }
  }else{
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected bulk string array."))
  }
}

pub fn send_key_scan_result(inner: &Arc<RedisClientInner>, mut cmd: RedisCommand, result: Vec<RedisKey>, can_continue: bool) -> Result<(), RedisError> {
  if let RedisCommandKind::Scan(scan_state) = cmd.kind {
    let tx = scan_state.tx.clone();

    let scan_result = ScanResult {
      can_continue,
      inner: inner.clone(),
      scan_state,
      args: cmd.args,
      results: Some(result)
    };

    tx.unbounded_send(Ok(scan_result))
      .map_err(|_| RedisError::new(RedisErrorKind::Unknown, "Error sending SCAN callback."))
  }else{
    Err(RedisError::new(RedisErrorKind::Unknown, "Invalid redis command. Expected SCAN."))
  }
}

pub fn send_key_scan_error(cmd: &RedisCommand, e: RedisError) -> Result<(), RedisError> {
  if let RedisCommandKind::Scan(ref inner) = cmd.kind {
    inner.tx.unbounded_send(Err(e))
      .map_err(|_| RedisError::new(RedisErrorKind::Unknown, "Error sending SCAN callback."))
  }else{
    Err(RedisError::new(RedisErrorKind::Unknown, "Invalid redis command. Expected SCAN."))
  }
}

pub fn send_value_scan_result(inner: &Arc<RedisClientInner>, mut cmd: RedisCommand, result: Vec<RedisValue>, can_continue: bool) -> Result<(), RedisError> {
  let args = cmd.args;

  match cmd.kind {
    RedisCommandKind::Zscan(scan_state) => {
      let tx = scan_state.tx.clone();
      let results = ValueScanInner::transform_zscan_result(result)?;

      let state = ValueScanResult::ZScan(ZScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state,
        args,
        results: Some(results)
      });

      tx.unbounded_send(Ok(state))
        .map_err(|_| RedisError::new(RedisErrorKind::Unknown, "Error sending value scan callback."))
    },
    RedisCommandKind::Sscan(scan_state) => {
      let tx = scan_state.tx.clone();

      let state = ValueScanResult::SScan(SScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state,
        args,
        results: Some(result)
      });

      tx.unbounded_send(Ok(state))
        .map_err(|_| RedisError::new(RedisErrorKind::Unknown, "Error sending value scan callback."))
    },
    RedisCommandKind::Hscan(scan_state) => {
      let tx = scan_state.tx.clone();
      let results = ValueScanInner::transform_hscan_result(result)?;

      let state = ValueScanResult::HScan(HScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state,
        args,
        results: Some(results)
      });

      tx.unbounded_send(Ok(state))
        .map_err(|_| RedisError::new(RedisErrorKind::Unknown, "Error sending value scan callback."))
    },
    _ => Err(RedisError::new(RedisErrorKind::Unknown, "Invalid redis command. Expected HSCAN, SSCAN, or ZSCAN."))
  }
}

pub fn send_value_scan_error(cmd: &RedisCommand, e: RedisError) -> Result<(), RedisError> {
  let inner = match cmd.kind {
    RedisCommandKind::Zscan(ref inner) => inner,
    RedisCommandKind::Sscan(ref inner) => inner,
    RedisCommandKind::Hscan(ref inner) => inner,
    _ => return Err(RedisError::new(RedisErrorKind::Unknown, "Invalid redis command. Expected HSCAN, SSCAN, or ZSCAN."))
  };

  inner.tx.unbounded_send(Err(e))
    .map_err(|_| RedisError::new(RedisErrorKind::Unknown, "Error sending value scan callback."))
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
  }else if last_command_is_key_scan(last_request) {
    trace!("{} Recv key scan result {:?}", n!(inner), frame);

    if let Some(m_tx) = take_last_command_callback(last_command_callback) {
      trace!("{} Found last caller from multiplexer.", n!(inner));
      let _ = m_tx.send(None);
    }

    let mut last_request = match take_last_request(last_request_sent, inner, last_request) {
      Some(s) => s,
      None => return
    };

    let (next_cursor, keys) = match handle_key_scan_result(frame) {
      Ok((c, k)) => (c, k),
      Err(e) => {
        let _ = send_key_scan_error(&last_request, e);
        return;
      }
    };

    let should_stop = next_cursor.as_str() == "0";
    trace!("{} Updating cursor to {} and sending scan result", n!(inner), next_cursor);
    update_scan_cursor(&mut last_request, next_cursor);

    if should_stop {
      // write results and return
      trace!("{} Sending last key scan result", n!(inner));
      let _ = send_key_scan_result(inner, last_request, keys, false);
    }else{
      if let Err(_) = send_key_scan_result(inner, last_request, keys, true) {
        // receiver has been dropped, don't need to continue scanning
        trace!("{} Scan receiver has been dropped, canceling key scan.", n!(inner));
      }
    }
  }else if last_command_is_value_scan(last_request) {
    trace!("{} Recv value scan (HSCAN, SSCAN, ZSCAN) result: {:?}", n!(inner), frame);

    if let Some(m_tx) = take_last_command_callback(last_command_callback) {
      trace!("{} Found last caller from multiplexer.", n!(inner));
      let _ = m_tx.send(None);
    }

    let mut last_request = match take_last_request(last_request_sent, inner, last_request) {
      Some(s) => s,
      None => return
    };

    let (next_cursor, values) = match handle_value_scan_result(frame) {
      Ok((c, k)) => (c, k),
      Err(e) => {
        let _ = send_value_scan_error(&last_request, e);
        return;
      }
    };

    let should_stop = next_cursor.as_str() == "0";
    trace!("{} Updating cursor to {} and sending value scan result", n!(inner), next_cursor);
    update_scan_cursor(&mut last_request, next_cursor);

    if should_stop {
      // write results and return
      trace!("{} Sending last value scan result", n!(inner));
      let _ = send_value_scan_result(inner, last_request, values, false);
    }else{
      if let Err(_) = send_value_scan_result(inner, last_request, values, true) {
        // receiver has been dropped, don't need to continue scanning
        trace!("{} Value scan receiver has been dropped, canceling value scan.", n!(inner));
      }
    }
  }else{
    process_simple_frame(inner, last_request, last_request_sent, last_command_callback, frame);
  }
}

#[cfg(not(feature = "reconnect-on-auth-error"))]
fn process_simple_frame(inner: &Arc<RedisClientInner>,
           last_request: &Rc<RefCell<Option<RedisCommand>>>,
           last_request_sent: &Rc<RefCell<Option<Instant>>>,
           last_command_callback: &Rc<RefCell<Option<LastCommandCaller>>>,
           frame: Frame) {

  // if command channel found send success
  match take_last_command_callback(last_command_callback) {
    Some(m_tx) => {
      trace!("{} Found last caller from multiplexer.", n!(inner));
      let _ = m_tx.send(None);
    },
    None => error!("{} Did not find last request!", n!(inner))
  };

  // if caller channel found send the frame
  let last_request_tx = match take_last_request(last_request_sent, inner, last_request) {
    Some(s) => match s.tx {
      Some(tx) => tx,
      None => {
        error!("{} Did not find last caller callback!", n!(inner));
        return
      }
    },
    None => {
      error!("{} Did not find last caller!", n!(inner));
      return
    }
  };
  trace!("{} Responding to last request with frame.", n!(inner));

  let _ = last_request_tx.send(Ok(frame));
}

#[cfg(feature = "reconnect-on-auth-error")]
fn process_simple_frame(inner: &Arc<RedisClientInner>,
           last_request: &Rc<RefCell<Option<RedisCommand>>>,
           last_request_sent: &Rc<RefCell<Option<Instant>>>,
           last_command_callback: &Rc<RefCell<Option<LastCommandCaller>>>,
           frame: Frame) {

  match parse_redis_auth_error(&frame) {
    None => send_to_channels(inner, last_request, last_request_sent, last_command_callback, frame),
    Some(redis_auth_error) => send_to_channels_error(inner, last_request, last_request_sent, last_command_callback, frame, redis_auth_error)
  }
}

// sends Ok to command channel and frame to caller channel
#[cfg(feature = "reconnect-on-auth-error")]
fn send_to_channels(inner: &Arc<RedisClientInner>,
           last_request: &Rc<RefCell<Option<RedisCommand>>>,
           last_request_sent: &Rc<RefCell<Option<Instant>>>,
           last_command_callback: &Rc<RefCell<Option<LastCommandCaller>>>,
           frame: Frame) {

  // if command channel found send success
  match take_last_command_callback(last_command_callback) {
    Some(m_tx) => {
      trace!("{} Found last caller from multiplexer.", n!(inner));
      let _ = m_tx.send(None);
    },
    None => error!("{} Did not find last request!", n!(inner))
  }

  // if caller channel found send the frame
  let last_request_tx = match take_last_request(last_request_sent, inner, last_request) {
    Some(s) => match s.tx {
      Some(tx) => tx,
      None => {
        error!("{} Did not find last caller callback!", n!(inner));
        return
      }
    },
    None => {
      error!("{} Did not find last caller!", n!(inner));
      return
    }
  };
  trace!("{} Responding to last request with frame.", n!(inner));

  let _ = last_request_tx.send(Ok(frame));
}

// sends error to command channel, unless we are quitting
#[cfg(feature = "reconnect-on-auth-error")]
fn send_to_channels_error(inner: &Arc<RedisClientInner>,
              last_request: &Rc<RefCell<Option<RedisCommand>>>,
              last_request_sent: &Rc<RefCell<Option<Instant>>>,
              last_command_callback: &Rc<RefCell<Option<LastCommandCaller>>>,
              frame: Frame,
              redis_error: RedisError) {

  debug!("redis error {}", redis_error);

  match take_last_request(last_request_sent, inner, last_request) {
    Some(last_request_command) => {
      match take_last_command_callback(last_command_callback) {
        Some(m_tx) => {
          trace!("{} Found last caller from multiplexer.", n!(inner));

          let is_quitting = last_request_command.kind == RedisCommandKind::Quit;

          // if we received a redis error but we were in the process of quitting
          // we do not want to retry the connection, just pass the frame up to the command and caller
          // channels as normal
          if is_quitting {
            let _ = m_tx.send(None);

            let last_request_tx = match last_request_command.tx {
              Some(tx) => tx,
              None => {
                error!("{} Did not find last caller callback!", n!(inner));
                return
              }
            };

            trace!("{} Responding to last request with frame.", n!(inner));
            let _ = last_request_tx.send(Ok(frame));
          }else{
            // send the last command and error to the command channel
            info!("{} Sending error to last request: {}", n!(inner), redis_error);
            let _ = m_tx.send(Some((last_request_command, redis_error)));
          }
        },
        None => error!("{} Did not find last caller!", n!(inner))
      }
    },
    // No command channel was found
    None => error!("{} Did not find last request!", n!(inner))
  };
}

#[cfg(feature = "reconnect-on-auth-error")]
// parses the response frame to see if it's an auth error
fn parse_redis_auth_error(frame: &Frame) -> Option<RedisError> {
  match frame.is_error() {
    false => None,
    true => {
      let frame_result = frame_to_single_result(frame.clone());
      match frame_result {
        Err(e) => match e.kind() {
          RedisErrorKind::Auth => Some(e),
          _ => None
        }
        _ => None
      }
    }
  }
}

