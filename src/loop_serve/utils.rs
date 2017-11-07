
use ::protocol::utils as protocol_utils;
use ::utils as client_utils;

use super::super::error::{
  RedisError,
  RedisErrorKind
};

use std::io::{
  Error as IoError,
  ErrorKind as IoErrorKind
};

use futures::future;
use futures::Future;
use futures::sync::oneshot::{
  Sender as OneshotSender,
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  UnboundedSender,
  UnboundedReceiver,
};
use futures::stream::{
  self, 
  Stream
};
use futures::sink::Sink;
use futures::future::Either;
use futures::future::{
  Loop
};

use parking_lot::{
  RwLock
};

use tokio_core::reactor::{
  Handle
};

use tokio_core::net::TcpStream;
use tokio_io::{AsyncRead};

use tokio_timer::Timer;

use std::sync::Arc;
use std::time::Duration;

use ::types::*;
use ::protocol::types::*;

use super::{
  RedisTransport,
  RedisSink,
  RedisStream,
  SplitTransport
};

use std::net::{
  SocketAddr,
  ToSocketAddrs
};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use super::super::RedisClient;
use super::multiplexer::Multiplexer;

pub const OK: &'static str = "OK";

pub fn close_error_tx(error_tx: &Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>) {
  let mut error_tx_ref = error_tx.borrow_mut();

  for mut error_tx in error_tx_ref.drain(..) {
    debug!("Closing error tx.");

    let _ = error_tx.close();
  }
}

pub fn close_reconnect_tx(reconnect_tx: &Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>) {
  let mut reconnect_tx_ref = reconnect_tx.borrow_mut();

  for mut reconnect_tx in reconnect_tx_ref.drain(..) {
    debug!("Closing reconnect tx.");

    let _ = reconnect_tx.close();
  }
}

pub fn close_messages_tx(messages_tx: &Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>) {
  let mut messages_tx_ref = messages_tx.borrow_mut();

  for mut messages_tx in messages_tx_ref.drain(..) {
    debug!("Closing messages tx.");

    let _ = messages_tx.close();
  }
}

pub fn close_connect_tx(connect_tx: &Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>, remote_tx: &Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>) {
  debug!("Closing connection tx.");

  {
    let mut connect_tx_refs = connect_tx.borrow_mut();
    if connect_tx_refs.len() > 0 {
      for tx in connect_tx_refs.drain(..) {
        let _ = tx.send(Err(RedisError::new_canceled()));
      }
    }
  }
  {
    let mut remote_tx_refs = remote_tx.borrow_mut();
    for tx in remote_tx_refs.drain(..) {
      let _ = tx.send(Err(RedisError::new_canceled()));
    }
  }
}

pub fn emit_error(tx: &Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>, error: &RedisError) {
  let mut tx_ref = tx.borrow_mut();

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

pub fn emit_reconnect(reconnect_tx: &Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>, client: &RedisClient) {
  let mut tx_ref = reconnect_tx.borrow_mut();

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

pub fn emit_connect(connect_tx: &Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>, remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>, client: &RedisClient) {
  debug!("Emitting connect.");

  {
    let mut connect_tx_refs = connect_tx.borrow_mut();
    for tx in connect_tx_refs.drain(..) {
      let _ = tx.send(Ok(client.clone()));
    }
  }
  {
    let mut remote_tx_refs = remote_tx.borrow_mut();
    for tx in remote_tx_refs.drain(..) {
      let _ = tx.send(Ok(()));
    }
  }
}

pub fn emit_connect_error(connect_tx: &Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>, remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>, err: &RedisError) {
  debug!("Emitting connect error.");

  {
    let mut remote_tx_refs = remote_tx.borrow_mut();
    for tx in remote_tx_refs.drain(..) {
      let _ = tx.send(Err(err.clone()));
    }
  }
  {
    let mut connect_tx_refs = connect_tx.borrow_mut();
    for tx in connect_tx_refs.drain(..) {
      let _ = tx.send(Err(err.clone()));
    }
  }
}

pub fn set_reconnect_policy(policy: &Rc<RefCell<Option<ReconnectPolicy>>>, new_policy: ReconnectPolicy) {
  let mut policy_ref = policy.borrow_mut();
  *policy_ref = Some(new_policy);
}

pub fn take_reconnect_policy(policy: &Rc<RefCell<Option<ReconnectPolicy>>>) -> Option<ReconnectPolicy> {
  let mut policy_ref = policy.borrow_mut();
  policy_ref.take()
}

pub fn next_reconnect_delay(policy: &Rc<RefCell<Option<ReconnectPolicy>>>) -> Option<u32> {
  let mut policy_ref = policy.borrow_mut();
  
  match *policy_ref {
    Some(ref mut policy) => policy.next_delay(),
    None => None
  }
}

// grab the redis host/port string
pub fn read_centralized_host(config: &Rc<RefCell<RedisConfig>>) -> Result<String, RedisError> {
  let config_ref = config.borrow();

  match *config_ref {
    RedisConfig::Centralized { ref host, ref port, .. } => {
      Ok(vec![
        host.clone(), 
        port.to_string()
      ].join(":"))
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown, "Invalid redis config. Centralized config expected."
    ))
  }
}

// read all the (host, port) tuples in the config
pub fn read_clustered_hosts(config: &Rc<RefCell<RedisConfig>>) -> Result<Vec<(String, u16)>, RedisError> {
  let config_ref = config.borrow();

  match *config_ref {
    RedisConfig::Clustered { ref hosts, .. } => Ok(hosts.clone()),
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown, "Invalid redis config. Clustered config expected."
    ))
  }
}

pub fn read_auth_key(config: &Rc<RefCell<RedisConfig>>) -> Option<String> {
  let config_ref = config.borrow();

  match *config_ref {
    RedisConfig::Centralized { ref key, .. } => match *key {
      Some(ref s) => Some(s.to_owned()),
      None => None
    },
    RedisConfig::Clustered{ ref key, .. } => match *key {
      Some(ref s) => Some(s.to_owned()),
      None => None
    }
  }
}

pub fn tuple_to_addr_str(host: &str, port: u16) -> String {
  format!("{}:{}", host, port)
}

pub fn take_message_sender(messages: &Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>) -> VecDeque<UnboundedSender<(String, RedisValue)>> {
  let mut messages_ref = messages.borrow_mut();
  let taken = messages_ref.drain(..).collect();
  taken
}

pub fn set_message_sender(messages: &Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>, sender: VecDeque<UnboundedSender<(String, RedisValue)>>) {
  let mut messages_ref = messages.borrow_mut();
  *messages_ref = sender;
}

pub fn take_last_request(last_request: &RefCell<ResponseSender>) -> ResponseSender {
  let mut request_ref = last_request.borrow_mut();
  request_ref.take()
}

pub fn set_last_request(last_request: &RefCell<ResponseSender>, request: ResponseSender) {
  let mut request_ref = last_request.borrow_mut();
  *request_ref = request;
}

pub fn take_last_caller(last_caller: &RefCell<Option<OneshotSender<RefreshCache>>>) -> Option<OneshotSender<RefreshCache>> {
  let mut caller_ref = last_caller.borrow_mut();
  caller_ref.take()
}

pub fn set_last_caller(last_caller: &RefCell<Option<OneshotSender<RefreshCache>>>, caller: Option<OneshotSender<RefreshCache>>) {
  let mut caller_ref = last_caller.borrow_mut();
  *caller_ref = caller;
}

pub fn set_command_tx(command_tx: &Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>, tx: UnboundedSender<RedisCommand>) {
  let mut command_tx_ref = command_tx.borrow_mut();

  if let Some(ref mut tx) = *command_tx_ref {
    let _ = tx.close();
  }

  *command_tx_ref = Some(tx);
}

pub fn create_transport(
  addr: &SocketAddr, 
  handle: &Handle,
  config: Rc<RefCell<RedisConfig>>,
  state: Arc<RwLock<ClientState>>,
) -> Box<Future<Item=(RedisSink, RedisStream), Error=RedisError>>
{
  debug!("Creating redis transport to {:?}", &addr);
  let codec = {
    let config_ref = config.borrow();
    RedisCodec::new(config_ref.get_max_size())
  };

  Box::new(TcpStream::connect(&addr, handle)
    .map_err(|e| e.into())
    .and_then(move |socket| Ok(socket.framed(codec)))
    .and_then(move |transport| {
      authenticate(transport, read_auth_key(&config))
    })
    .and_then(move |transport| {
      client_utils::set_client_state(&state, ClientState::Connected);

      Ok(transport.split())
    })
    .map_err(|e| e.into()))
}

pub fn request_response_split(stream: RedisStream, sink: RedisSink, mut request: RedisCommand) -> Box<Future<Item=(Frame, SplitTransport), Error=RedisError>> {
  let frame = fry!(request.to_frame());

  Box::new(sink.send(frame)
    .map_err(|e| e.into())
    .and_then(|sink| {
      stream.into_future()
        .map_err(|(e, _)| e.into())
        .and_then(|(response, stream)| {
          let response = match response {
            Some(r) => r,
            None => return Err(RedisError::new(
              RedisErrorKind::ProtocolError, "Empty response."
            ))
          };

          Ok((response, (sink, stream)))
        })
    }))
}

pub fn request_response(transport: RedisTransport, mut request: RedisCommand) -> Box<Future<Item=(Frame, RedisTransport), Error=RedisError>> {
  let frame = fry!(request.to_frame());

  Box::new(transport.send(frame)
    .and_then(|transport| transport.into_future().map_err(|(e, _)| e.into()))
    .and_then(|(response, transport)| {
      let response = match response {
        Some(r) => r,
        None => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Empty response."
        ))
      };

      Ok((response, transport))
    }))
}

pub fn authenticate(transport: RedisTransport, key: Option<String>) -> Box<Future<Item=RedisTransport, Error=RedisError>> {
  let key = match key {
    Some(k) => k,
    None => return client_utils::future_ok(transport)
  };

  let command = RedisCommand::new(RedisCommandKind::Auth, vec![key.into()], None);

  debug!("Authenticating Redis client...");

  Box::new(request_response(transport, command).and_then(|(frame, transport)| {
    let inner = match frame {
      Frame::SimpleString(s) => s,
      _ => return Err(RedisError::new(
        RedisErrorKind::ProtocolError, format!("Invalid auth response {:?}.", frame)
      ))
    };

    if inner == OK {
      debug!("Successfully authenticated Redis client.");

      Ok(transport)
    }else{
      Err(RedisError::new(
        RedisErrorKind::Auth, "Authentication failed."
      ))
    }
  }))
}

pub fn process_frame(multiplexer: &Rc<Multiplexer>, frame: Frame) {
  if frame.is_pubsub_message() {
    let (channel, message) = match protocol_utils::frame_to_pubsub(frame) {
      Ok((c, m)) => (c, m),
      // TODO or maybe send to error stream
      Err(_) => return
    };

    let mut to_remove = VecDeque::new();
    {
      let message_tx_ref = multiplexer.message_tx.borrow();

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
          to_remove.push_back(idx);
        }
      }

      // send original values
      if let Some(ref tx) = message_tx_ref.get(to_send) {
        if let Err(_) = tx.unbounded_send((channel, message)) {
          to_remove.push_back(to_send);
        }
      }
    }
    // remove any senders where the receiver was closed
    if to_remove.len() > 0 {
      let mut message_tx_ref = multiplexer.message_tx.borrow_mut();

      for idx in to_remove {
        let _ = message_tx_ref.remove(idx);
      }
    }
  }else{
    if let Some(m_tx) = multiplexer.take_last_caller() {
      let _ = m_tx.send(false);
    }

    let last_request = match multiplexer.take_last_request() {
      Some(s) => s,
      None => return
    };
    let _ = last_request.send(Ok(frame));
  }
}

pub fn create_multiplexer_ft(multiplexer: Rc<Multiplexer>, state: Arc<RwLock<ClientState>>) -> Box<Future<Item=(), Error=RedisError>> {
  Box::new(Multiplexer::listen(multiplexer.clone()).then(move |result| {
    client_utils::set_client_state(&state, ClientState::Disconnected);

    if let Err(ref e) = result {
      debug!("Multiplexer frame stream future closed with error {:?}", e);
    }

    match result {
      Ok(multiplexer) => multiplexer.close_commands(),
      Err(e) => Err(e)
    }
  }))
}

pub fn create_commands_ft(
  rx: UnboundedReceiver<RedisCommand>,
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  multiplexer: Rc<Multiplexer>,
  stream_state: Arc<RwLock<ClientState>>
) -> Box<Future<Item=(), Error=RedisError>>
{
  let state = stream_state.clone();

  Box::new(rx.fold((multiplexer, error_tx), move |(multiplexer, error_tx), mut command| {
    debug!("Redis client running command {:?}", command.kind);

    if command.kind == RedisCommandKind::_Close {
      // socket was closed abruptly, so try to reconnect if necessary
      debug!("Redis transport closed abruptly.");

      client_utils::set_client_state(&state, ClientState::Disconnecting);

      multiplexer.sinks.close();
      multiplexer.streams.close();

      Box::new(Either::A(future::err(())
        .map(|_: ()| (multiplexer, error_tx))))
    }else{
      let resp_tx = command.tx.take();

      Box::new(Either::B(multiplexer.write_command(command).then(move |result| {
        match result {
          Ok(_) => {
            // create a second oneshot channel for notifying when to move on to the next command
            let (m_tx, m_rx) = oneshot_channel();

            multiplexer.set_last_request(resp_tx);
            multiplexer.set_last_caller(Some(m_tx));

            Box::new(m_rx.then(|_| Ok(multiplexer)))
          },
          Err(e) => {
            // send the error and move on right away
            if let Some(tx) = resp_tx {
              let _ = tx.send(Err(e));
            }

            client_utils::future_ok(multiplexer)
          }
        }
      })
      .map_err(|_| ())
      .map(|multiplexer| (multiplexer, error_tx))))
    }
  })
  .then(move |result| {
    match result {
      Ok((multiplexer, _)) => {
        debug!("Command stream closing after quit.");

        // stream was closed due to exit command so close the socket
        client_utils::set_client_state(&stream_state, ClientState::Disconnected);

        multiplexer.sinks.close();
        multiplexer.streams.close();

        client_utils::future_ok(())
      },
      // these errors can only be (), so ignore them
      Err(_) => client_utils::future_ok(())
    }
  })
  .from_err::<RedisError>())
}

pub fn create_connection_ft(command_ft: Box<Future<Item=(), Error=RedisError>>, multiplexer_ft: Box<Future<Item=(), Error=RedisError>>, state: Arc<RwLock<ClientState>>) -> ConnectionFuture {
  Box::new(command_ft.join(multiplexer_ft).then(move |result| {
    debug!("Connection closed with {:?}", result);

    match result {
      Ok(_) => Ok(None),
      Err(e) => Ok(match *e.kind() {
        RedisErrorKind::Canceled => None,
        _ => {
          // errors should trigger a reconnect
          client_utils::set_client_state(&state, ClientState::Disconnecting);

          Some(e.into())
        }
      })
    }
  }))
}

#[allow(deprecated)]
pub fn create_initial_transport(handle: Handle, config: Rc<RefCell<RedisConfig>>) -> Box<Future<Item=Option<RedisTransport>, Error=RedisError>> {
  let hosts = fry!(read_clustered_hosts(&config));
  let found: Option<RedisTransport> = None;

  // find the first available host that can be connected to. would be nice if streams had a `find` function...
  Box::new(stream::iter(hosts.into_iter().map(Ok)).fold((found, handle), move |(found, handle), (host, port)| {
    if found.is_none() {
      let host = host.to_owned();

      let addr_str = tuple_to_addr_str(&host, port);
      let mut addr = match addr_str.to_socket_addrs() {
        Ok(addr) => addr,
        Err(e) => return client_utils::future_error(e.into())
      };

      let addr = match addr.next() {
        Some(a) => a,
        None => return client_utils::future_error(RedisError::new(
          RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
        ))
      };

      let key = read_auth_key(&config);
      let codec = {
        let config_ref = config.borrow();
        RedisCodec::new(config_ref.get_max_size())
      };

      debug!("Creating clustered redis transport to {:?}", &addr);

      Box::new(TcpStream::connect(&addr, &handle)
        .from_err::<RedisError>()
        .and_then(move |socket| Ok(socket.framed(codec)))
        .and_then(move |transport| {
          authenticate(transport, key)
        })
        .and_then(move |transport| {
          Ok((Some(transport), handle))
        })
        .from_err::<RedisError>())
    }else{
      client_utils::future_ok((found, handle))
    }
  })
  .map(|(transport, _)| transport))
}

#[allow(deprecated)]
pub fn create_all_transports(config: Rc<RefCell<RedisConfig>>, handle: Handle, hosts: Vec<(String, u16)>, key: Option<String>) -> Box<Future<Item=Vec<(String, RedisTransport)>, Error=RedisError>> {
  let transports: Vec<(String, RedisTransport)> = Vec::with_capacity(hosts.len());

  Box::new(stream::iter(hosts.into_iter().map(Ok)).fold(transports, move |mut transports, (host, port)| {

    let addr_str = tuple_to_addr_str(&host, port);
    let mut addr = match addr_str.to_socket_addrs() {
      Ok(addr) => addr,
      Err(e) => return client_utils::future_error(e.into())
    };

    let addr = match addr.next() {
      Some(a) => a,
      None => return client_utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
      ))
    };
    let ip_str = format!("{}:{}", addr.ip(), addr.port());

    let key = key.clone();
    let codec = {
      let config_ref = config.borrow();
      RedisCodec::new(config_ref.get_max_size())
    };

    debug!("Creating clustered transport to {:?}", addr);

    Box::new(TcpStream::connect(&addr, &handle)
      .from_err::<RedisError>()
      .and_then(move |socket| Ok(socket.framed(codec)))
      .and_then(move |transport| {
        authenticate(transport, key)
      })
      .and_then(move |transport: RedisTransport| {
        transports.push((ip_str, transport));
        Ok(transports)
      })
      .from_err::<RedisError>())
  }))
}

pub fn build_cluster_cache(handle: Handle, config: &Rc<RefCell<RedisConfig>>) -> Box<Future<Item=ClusterKeyCache, Error=RedisError>> {
  Box::new(create_initial_transport(handle, config.clone()).and_then(|transport| {
    let transport = match transport {
      Some(t) => t,
      None => return client_utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, "Could not connect to any Redis server in config."
      ))
    };

    let command = RedisCommand::new(RedisCommandKind::ClusterNodes, vec![], None);
    debug!("Reading cluster state...");

    request_response(transport, command)
  })
  .and_then(|(frame, mut transport)| {

    let response = if frame.is_error() {
      match frame.into_error() {
        Some(e) => return Err(e),
        None => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Empty response."
        ))
      }
    }else{
      match frame.to_string() {
        Some(s) => s,
        None => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Empty response."
        ))
      }
    };

    let cache = match ClusterKeyCache::new(Some(response)) {
      Ok(c) => c,
      Err(e) => return Err(e)
    };

    let _ = transport.close();
    Ok(cache)
  })
  .from_err::<RedisError>())
}

#[allow(unused_variables)]
pub fn reconnect(
  handle: Handle,
  timer: Timer,
  mut policy: ReconnectPolicy,
  state: Arc<RwLock<ClientState>>,
  closed: Arc<RwLock<bool>>,
  error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
  message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
  command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
  reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
  connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
  remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
  mut result: Result<Option<RedisError>, RedisError>
) -> Box<Future<Item=Loop<(), (Handle, Timer, ReconnectPolicy)>, Error=RedisError>> {

  // since framed sockets don't give an error when closed abruptly the client's state is
  // used to determine whether or not the socket was closed intentionally or not
  if client_utils::read_client_state(&state) == ClientState::Disconnecting {
    let io_err = IoError::new(IoErrorKind::ConnectionReset, "Redis socket closed abruptly.");

    result = Err(RedisError::new(
      RedisErrorKind::IO(io_err), "Redis socket closed."
    ));
  }

  debug!("Starting reconnect logic from error {:?}...", result);

  match result {
    Ok(err) => {
      if let Some(err) = err {
        // socket was closed unintentionally
        debug!("Redis client closed abruptly.");
        emit_error(&error_tx, &err);

        let delay = match policy.next_delay() {
          Some(delay) => delay,
          None => return client_utils::future_ok(Loop::Break(()))
        };

        debug!("Waiting for {} ms before attempting to reconnect...", delay);

        Box::new(timer.sleep(Duration::from_millis(delay as u64)).from_err::<RedisError>().and_then(move |_| {
          if client_utils::read_closed_flag(&closed) {
            client_utils::set_closed_flag(&closed, false);
            return Err(RedisError::new(
              RedisErrorKind::Canceled, "Client closed while waiting to reconnect."
            ));
          }

          Ok(Loop::Continue((handle, timer, policy)))
        }))
      } else {
        // socket was closed via Quit command
        debug!("Redis client closed via Quit.");

        client_utils::set_client_state(&state, ClientState::Disconnected);

        close_error_tx(&error_tx);
        close_reconnect_tx(&reconnect_tx);
        close_connect_tx(&connect_tx, &remote_tx);
        close_messages_tx(&message_tx);

        client_utils::future_ok(Loop::Break(()))
      }
    },
    Err(e) => {
      emit_error(&error_tx, &e);

      let delay = match policy.next_delay() {
        Some(delay) => delay,
        None => return client_utils::future_ok(Loop::Break(()))
      };

      debug!("Waiting for {} ms before attempting to reconnect...", delay);

      Box::new(timer.sleep(Duration::from_millis(delay as u64)).from_err::<RedisError>().and_then(move |_| {
        if client_utils::read_closed_flag(&closed) {
          client_utils::set_closed_flag(&closed, false);
          return Err(RedisError::new(
            RedisErrorKind::Canceled, "Client closed while waiting to reconnect."
          ));
        }

        Ok(Loop::Continue((handle, timer, policy)))
      }))
    }
  }
}