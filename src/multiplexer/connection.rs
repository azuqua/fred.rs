use crate::protocol::utils as protocol_utils;
use crate::utils as client_utils;
use crate::multiplexer::utils as multiplexer_utils;
use crate::async_ng::*;

use crate::error::{
  RedisError,
  RedisErrorKind
};

use std::io::{
  Error as IoError,
  ErrorKind as IoErrorKind
};

use futures::{Future, FutureExt, TryFutureExt,  SinkExt};
use futures::stream::StreamExt;
use futures::future::lazy;
use futures::channel::oneshot::{
  Sender as OneshotSender,
  channel as oneshot_channel
};
use futures::channel::mpsc::{
  UnboundedSender,
  UnboundedReceiver,
};
use futures::stream::{
  self,
  Stream
};
use futures::sink::Sink;
//use futures::future::{
//  Loop
//};

use parking_lot::RwLock;

use tokio_02::net::TcpStream;
//use tokio_timer::Timer;

use std::sync::Arc;
use std::time::Duration;

use crate::types::*;
use crate::protocol::types::*;
use crate::multiplexer::types::{RedisTcpSink, RedisTcpStream};

use std::net::{
  SocketAddr,
  ToSocketAddrs
};

#[cfg(feature="enable-tls")]
use tokio_tls::{
  TlsConnector as TlsConnectorAsync,
  TlsStream
};
#[cfg(feature="native-tls")]
use native_tls::{
  TlsConnector
};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{
  VecDeque,
  HashSet
};

use crate::client::{RedisClient, RedisClientInner};
use crate::multiplexer::Multiplexer;
use crate::metrics::*;

use futures::stream::{
  SplitSink,
  SplitStream
};
use tokio_util::codec::Framed;
use tokio_02::io::{AsyncRead,AsyncWrite};
use crate::protocol::RedisCodec;

use std::env;

use std::ops::{
  DerefMut,
  Deref
};

// use futures::lazy;

pub const OK: &'static str = "OK";

fn should_disable_cert_verification() -> bool {
  match env::var_os("FRED_DISABLE_CERT_VERIFICATION") {
    Some(s) => match s.into_string() {
      Ok(s) => match s.as_ref() {
        "1" | "true" | "TRUE" => true,
        _ => false
      },
      Err(_) => false
    },
    None => false
  }
}

#[cfg(feature="enable-tls")]
fn create_tls_connector() -> Result<TlsConnectorAsync, RedisError> {
  let mut builder = TlsConnector::builder();

  if should_disable_cert_verification() {
    builder.danger_accept_invalid_certs(true);
  }

  builder.build().map(|t| TlsConnectorAsync::from(t)).map_err(|e| RedisError::new(
    RedisErrorKind::Unknown, format!("TLS Error: {:?}", e)
  ))
}

#[cfg(not(feature="enable-tls"))]
pub async fn create_transport_tls(addr: &SocketAddr, spawner: &Spawner, inner: &Arc<RedisClientInner>)
  -> Result<(RedisTcpSink, RedisTcpStream), RedisError>
{
  create_transport(addr, spawner, inner).await
}

#[cfg(feature="enable-tls")]
pub fn create_transport_tls(addr: &SocketAddr, spawner: &Spawner, inner: &Arc<RedisClientInner>)
  -> Box<Future<Item=(SplitSink<Framed<TlsStream<TcpStream>, RedisCodec>>, SplitStream<Framed<TlsStream<TcpStream>, RedisCodec>>), Error=RedisError>>
{
  let codec = RedisCodec::new(inner.client_name(),
                              inner.req_size_stats.clone(),
                              inner.res_size_stats.clone());
  let addr_str = fry!(multiplexer_utils::read_centralized_host(&inner.config));

  let domain = match addr_str.split(":").next() {
    Some(d) => d.to_owned(),
    None => return client_utils::future_error(RedisError::new(
      RedisErrorKind::Unknown, format!("Invalid host/port string {}.", addr_str)
    ))
  };

  let inner = inner.clone();
  debug!("{} Creating redis tls transport to {:?} with domain {}", n!(inner), &addr, domain);

  Box::new(TcpStream::connect2(&addr) // FIXME: can no longer take handle?
    .from_err::<RedisError>()
    .and_then(move |socket| {
      let tls_stream = match create_tls_connector() {
        Ok(t) => t,
        Err(e) => return client_utils::future_error(e)
      };

      Box::new(tls_stream.connect(&domain, socket).map_err(|e| {
        RedisError::new(RedisErrorKind::Unknown, format!("TLS Error: {:?}", e))
      }))
    })
    .and_then(move |socket| Ok(socket.framed(codec)))
    .and_then(move |transport| {
      authenticate(transport, inner.client_name(), multiplexer_utils::read_auth_key(&inner.config))
        .map(move |t| (inner, t))
    })
    .and_then(move |(inner, transport)| {
      client_utils::set_client_state(&inner.state, ClientState::Connected);

      Ok(transport.split())
    })
    .map_err(|e| e.into()))
}

pub async fn create_transport(addr: &SocketAddr, spawner: &Spawner, inner: &Arc<RedisClientInner>)
  -> Result<(RedisTcpSink, RedisTcpStream), RedisError>
{
  debug!("{} Creating redis transport to {:?}", n!(inner), &addr);
  let codec = RedisCodec::new(inner.client_name(),
                              inner.req_size_stats.clone(),
                              inner.res_size_stats.clone());
  let inner = inner.clone();

  // FIXME: tokio 0.2 doesn't seem to support passing the event loop handle here
  let socket = tokio_02::net::TcpStream::connect(&addr).await?; // FIXME: err_into instead?
  let transport = Framed::new(socket, codec);
  let authed = authenticate(transport, inner.client_name(), multiplexer_utils::read_auth_key(&inner.config)).await?;
  client_utils::set_client_state(&inner.state, ClientState::Connected);
  Ok(authed.split())

  /*
  tokio_02::net::TcpStream::connect(&addr)
    .map_err(|e| e.into())
    //.and_then(move |socket| Ok(socket.framed(codec)))
    .and_then(move |socket| Framed::new(socket, codec)) // FIXME: or AsyncRead::framed?
    .and_then(move |transport| {
      authenticate(transport, inner.client_name(), multiplexer_utils::read_auth_key(&inner.config))
        .map(move |t| (inner, t))
    })
    .and_then(move |(inner, transport)| {
      client_utils::set_client_state(&inner.state, ClientState::Connected);

      Ok(transport.split())
    })
    .map_err(|e| e.into())
    .await
  */
}

//pub async fn request_response<T>(transport: Framed<T, RedisCodec>, request: &RedisCommand) -> Result<(Frame, Framed<T, RedisCodec>), RedisError>
pub async fn request_response<T>(mut transport: Framed<T, RedisCodec>, request: &RedisCommand) -> Result<(Frame, Framed<T, RedisCodec>), RedisError>
  where T: AsyncRead + AsyncWrite + Unpin + 'static
{
  //let frame = fry!(request.to_frame());
  let frame = match request.to_frame() {
    Ok(frame) => frame,
    Err(err)  => return Err(err)
  };

  transport.send(frame).await?;

  // FIXME: how does error handling work in this case now that the stream doesn't have the error?
  transport.into_future().map(|(response, transport)| {
      match response {
        Some(Ok(r)) => Ok((r, transport)),
        Some(Err(e)) => Err(e),
        None => Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Empty response."
        ))
      }
  }).await

  /*
  transport.send(frame)
    .and_then(|transport| transport.into_future().map_err(|(e, _)| e.into()))
    .and_then(|(response, transport)| {
      let response = match response {
        Some(r) => r,
        None => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Empty response."
        ))
      };

      Ok((response, transport))
    })
  */
}

pub async fn authenticate<T>(transport: Framed<T, RedisCodec>, name: String, key: Option<String>) -> Result<Framed<T, RedisCodec>, RedisError>
  where T: AsyncRead + AsyncWrite + 'static
{

  unimplemented!();
  /*
  lazy(move |_| {
    if let Some(key) = key {
      let command = RedisCommand::new(RedisCommandKind::Auth, vec![key.into()], None);

      debug!("{} Authenticating Redis client...", name);

      Box::new(request_response(transport, &command).and_then(|(frame, transport)| {
        let inner = match frame {
          Frame::SimpleString(s) => s,
          _ => return Err(RedisError::new(
            RedisErrorKind::ProtocolError, format!("Invalid auth response {:?}.", frame)
          ))
        };

        if inner == OK {
          debug!("{} Successfully authenticated Redis client.", name);

          Ok((name, transport))
        }else{
          Err(RedisError::new(RedisErrorKind::Auth, inner))
        }
      }))
    }else{
      client_utils::future_ok((name, transport))
    }
  })
  .and_then(move |(name, transport)| {
    debug!("{} Changing client name to {}", name, name);

    let command = RedisCommand::new(RedisCommandKind::ClientSetname, vec![name.clone().into()], None);

    request_response(transport, &command).and_then(move |(frame, transport)| {
      let inner = match frame {
        Frame::SimpleString(s) => s,
        _ => {
          warn!("{} Error trying to set the client name: {:?}", name, frame);
          return Ok(transport);
        }
      };

      if inner == OK {
        debug!("{} Successfully set Redis client name.", name);
      }else{
        warn!("{} Unexpected response to client-setname: {}", name, inner);
      }

      Ok(transport)
    })
  })
  */
}

#[cfg(not(feature="enable-tls"))]
pub async fn create_initial_transport_tls(spawner: Spawner, inner: &Arc<RedisClientInner>) -> Result<Option<Framed<TcpStream, RedisCodec>>, RedisError> {
  create_initial_transport(spawner, inner).await
}

#[allow(deprecated)]
#[cfg(feature="enable-tls")]
pub fn create_initial_transport_tls(spawner: Spawner, inner: &Arc<RedisClientInner>) -> Box<Future<Item=Option<Framed<TlsStream<TcpStream>, RedisCodec>>, Error=RedisError>> {
  let hosts = fry!(multiplexer_utils::read_clustered_hosts(&inner.config));
  let found: Option<Framed<TlsStream<TcpStream>, RedisCodec>> = None;
  let inner = inner.clone();

  // find the first available host that can be connected to. would be nice if streams had a `find` function...
  Box::new(stream::iter(hosts.into_iter().map(Ok)).fold((found, spawner), move |(found, spawner), (host, port)| {
    if found.is_none() {
      let host = host.to_string();

      let addr_str = multiplexer_utils::tuple_to_addr_str(&host, port);
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

      let key = multiplexer_utils::read_auth_key(&inner.config);
      let codec = RedisCodec::new(inner.client_name(),
                                  inner.req_size_stats.clone(),
                                  inner.res_size_stats.clone());
      let client_name = inner.client_name();

      debug!("{} Creating clustered redis tls transport to {:?}", client_name, &addr);

      Box::new(tcp_connect(&addr, &spawner)
        .from_err::<RedisError>()
        .and_then(move |socket| {
          let tls_stream = match create_tls_connector() {
            Ok(t) => t,
            Err(e) => return client_utils::future_error(e)
          };

          Box::new(tls_stream.connect(&host, socket).map_err(|e| {
            RedisError::new(RedisErrorKind::Unknown, format!("TLS Error: {:?}", e))
          }))
        })
        .and_then(move |socket| Ok(socket.framed(codec)))
        .and_then(move |transport| {
          authenticate(transport, client_name, key)
        })
        .and_then(move |transport| {
          Ok((Some(transport), spawner))
        })
        .from_err::<RedisError>())
    }else{
      client_utils::future_ok((found, spawner))
    }
  })
  .map(|(transport, _)| transport))
}

#[allow(deprecated)]
pub async fn create_initial_transport(spawner: Spawner, inner: &Arc<RedisClientInner>) -> Result<Option<Framed<TcpStream, RedisCodec>>, RedisError> {
  //let hosts = fry!(multiplexer_utils::read_clustered_hosts(&inner.config));
  let hosts = match multiplexer_utils::read_clustered_hosts(&inner.config) {
    Ok(hosts) => hosts,
    Err(err)  => return Err(err)
  };
  let found: Option<Framed<TcpStream, RedisCodec>> = None;
  let inner = inner.clone();

  // find the first available host that can be connected to. would be nice if streams had a `find` function...
  for (host, port) in hosts.into_iter() {
    let host = host.to_owned();

    let addr_str = multiplexer_utils::tuple_to_addr_str(&host, port);
    let mut addr = match addr_str.to_socket_addrs() {
      Ok(addr) => addr,
      Err(e) => return Err(e.into())
    };

    let addr = match addr.next() {
      Some(a) => a,
      None => return Err(RedisError::new(
        RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
      ))
    };

    let key = multiplexer_utils::read_auth_key(&inner.config);
    let codec = RedisCodec::new(inner.client_name(),
                                inner.req_size_stats.clone(),
                                inner.res_size_stats.clone());
    let client_name = inner.client_name();

    debug!("{} Creating clustered redis transport to {:?}", client_name, &addr);
    let result = tokio_02::net::TcpStream::connect(&addr)
      .err_into::<RedisError>()
      .and_then(move |socket| {
        let transport = Framed::new(socket, codec);
        authenticate(transport, client_name, key)
      }).await;

    // If we connected, return it, otherwise ignore the error and loop
    if let Ok(transport) = result {
      return Ok(Some(transport))
    };
  }

  // Exhausted the available hosts
  Ok(None)

  // FIXME: look over original logic to see how it continued on failure of the connect attempts
  /*
  stream::iter(hosts.into_iter()).fold((found, spawner), move |(found, spawner), (host, port)| {
    if found.is_none() {
      let host = host.to_owned();

      let addr_str = multiplexer_utils::tuple_to_addr_str(&host, port);
      let mut addr = match addr_str.to_socket_addrs() {
        Ok(addr) => addr,
        Err(e) => return futures::future:Err(e.into())
      };

      let addr = match addr.next() {
        Some(a) => a,
        None => return Err(RedisError::new(
          RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
        ))
      };

      let key = multiplexer_utils::read_auth_key(&inner.config);
      let codec = RedisCodec::new(inner.client_name(),
                                  inner.req_size_stats.clone(),
                                  inner.res_size_stats.clone());
      let client_name = inner.client_name();

      debug!("{} Creating clustered redis transport to {:?}", client_name, &addr);

      Box::new(TcpStream::connect2(&addr) // FIXME: provide handle
        .err_into::<RedisError>()
        .and_then(move |socket| Ok(socket.framed(codec)))
        .and_then(move |transport| {
          authenticate(transport, client_name, key)
        })
        .and_then(move |transport| {
          Ok((Some(transport), spawner))
        })
        .from_err::<RedisError>())
    }else{
      client_utils::future_ok((found, spawner))
    }
  })
  .map(|(transport, _)| transport);
  */
}

#[cfg(not(feature="enable-tls"))]
pub async fn create_all_transports_tls(spawner: Spawner, cache: &ClusterKeyCache, key: Option<String>, inner: &Arc<RedisClientInner>)
  -> Result<Vec<(String, Framed<TcpStream, RedisCodec>)>, RedisError>
{
  create_all_transports(spawner, cache, key, inner).await
}

#[allow(deprecated)]
#[cfg(feature="enable-tls")]
pub fn create_all_transports_tls(spawner: Spawner, cache: &ClusterKeyCache, key: Option<String>, inner: &Arc<RedisClientInner>)
  -> Box<Future<Item=Vec<(String, Framed<TlsStream<TcpStream>, RedisCodec>)>, Error=RedisError>>
{
  let hosts: Vec<String> = cache.slots().iter().fold(HashSet::new(), |mut memo, slot| {
    memo.insert(slot.server.clone());
    memo
  }).into_iter().collect();

  let transports: Vec<(String, Framed<TlsStream<TcpStream>, RedisCodec>)> = Vec::with_capacity(hosts.len());
  let inner = inner.clone();

  Box::new(stream::iter(hosts.into_iter().map(Ok)).fold(transports, move |mut transports, addr_str| {
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

    let key = key.clone();
    let codec = RedisCodec::new(inner.client_name(),
                                inner.req_size_stats.clone(),
                                inner.res_size_stats.clone());
    let client_name = inner.client_name();

    let domain = match addr_str.split(":").next() {
      Some(d) => d.to_owned(),
      None => return client_utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, format!("Invalid host/port string {}.", addr_str)
      ))
    };

    debug!("{} Creating clustered tls transport to {:?} with domain {}", client_name, addr, domain);

    Box::new(tcp_connect(&addr, &spawner)
      .from_err::<RedisError>()
      .and_then(move |socket| {
        let tls_stream = match create_tls_connector() {
          Ok(t) => t,
          Err(e) => return client_utils::future_error(e)
        };

        Box::new(tls_stream.connect(&domain, socket).map_err(|e| {
          RedisError::new(RedisErrorKind::Unknown, format!("TLS Error: {:?}", e))
        }))
      })
      .and_then(move |socket| Ok(socket.framed(codec)))
      .and_then(move |transport| {
        authenticate(transport, client_name, key)
      })
      .and_then(move |transport| {
        // when using TLS the FQDN must be used, so the IP string isn't used here like it is below.
        // elasticache supports this by modifying the CLUSTER NODES command to use domain names instead of IPs
        transports.push((addr_str, transport));
        Ok(transports)
      })
      .from_err::<RedisError>())
  }))
}

#[allow(deprecated)]
pub async fn create_all_transports(spawner: Spawner, cache: &ClusterKeyCache, key: Option<String>, inner: &Arc<RedisClientInner>)
  -> Result<Vec<(String, Framed<TcpStream, RedisCodec>)>, RedisError>
{
  let hosts: Vec<String> = cache.slots().iter().fold(HashSet::new(), |mut memo, slot| {
    memo.insert(slot.server.clone());
    memo
  }).into_iter().collect();

  let mut transports: Vec<(String, Framed<TcpStream, RedisCodec>)> = Vec::with_capacity(hosts.len());
  let inner = inner.clone();

  for addr_str in hosts {
    let mut addr = match addr_str.to_socket_addrs() {
      Ok(addr) => addr,
      Err(e) => return Err(e.into())
    };

    let addr = match addr.next() {
      Some(a) => a,
      None => return Err(RedisError::new(
        RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
      ))
    };
    let ip_str = format!("{}:{}", addr.ip(), addr.port());

    let key = key.clone();
    let codec = RedisCodec::new(inner.client_name(),
                                inner.req_size_stats.clone(),
                                inner.res_size_stats.clone());
    let client_name = inner.client_name();

    debug!("{} Creating clustered transport to {:?}", client_name, addr);

    let result = tokio_02::net::TcpStream::connect(&addr)
      .err_into::<RedisError>()
      .and_then(move |socket| {
        let transport = Framed::new(socket, codec);
        authenticate(transport, client_name, key)
      }).await;

    // FIXME: should we really be silently ignoring failure here?
    // If we connected, return it, otherwise ignore the error and loop
    if let Ok(transport) = result {
      transports.push((ip_str, transport));
    };
  }

  Ok(transports)

  /*
  stream::iter(hosts.into_iter().map(Ok)).fold(transports, move |mut transports, addr_str| {
    let mut addr = match addr_str.to_socket_addrs() {
      Ok(addr) => addr,
      Err(e) => return Err(e.into())
    };

    let addr = match addr.next() {
      Some(a) => a,
      None => return Err(RedisError::new(
        RedisErrorKind::Unknown, format!("Could not resolve hostname {}.", addr_str)
      ))
    };
    let ip_str = format!("{}:{}", addr.ip(), addr.port());

    let key = key.clone();
    let codec = RedisCodec::new(inner.client_name(),
                                inner.req_size_stats.clone(),
                                inner.res_size_stats.clone());
    let client_name = inner.client_name();

    debug!("{} Creating clustered transport to {:?}", client_name, addr);

    tokio_02::net::TcpStream::connect(&addr) // FIXME: no longer a version which takes a handle
      .err_into::<RedisError>()
      .and_then(move |socket| Ok(socket.framed(codec)))
      .and_then(move |transport| {
        authenticate(transport, client_name, key)
      })
      .and_then(move |transport| {
        transports.push((ip_str, transport));
        Ok(transports)
      })
      .err_into::<RedisError>()
  })
  */
}

#[cfg(feature="enable-tls")]
fn read_cluster_cache_tls(spawner: Spawner, inner: &Arc<RedisClientInner>) -> Box<Future<Item=Frame, Error=RedisError>> {
  let inner = inner.clone();

  Box::new(create_initial_transport_tls(spawner, &inner).and_then(move |transport| {
    let transport = match transport {
      Some(t) => t,
      None => return client_utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, "Could not connect to any Redis server in config."
      ))
    };

    let command = RedisCommand::new(RedisCommandKind::ClusterNodes, vec![], None);
    debug!("{} Reading cluster state...", n!(inner));

    Box::new(request_response(transport, &command).map(|(frame, mut transport)| {
      let _ = transport.close();
      frame
    }))
  }))
}

#[cfg(not(feature="enable-tls"))]
async fn read_cluster_cache_tls(spawner: Spawner, inner: &Arc<RedisClientInner>) -> Result<Frame, RedisError> {
  read_cluster_cache(spawner, inner).await
}

async fn read_cluster_cache(spawner: Spawner, inner: &Arc<RedisClientInner>) -> Result<Frame, RedisError> {
  let inner = inner.clone();

  /*
  create_initial_transport(spawner, &inner).and_then(move |transport| {
    let transport = match transport {
      Some(t) => t,
      None => return client_utils::future_error(RedisError::new(
        RedisErrorKind::Unknown, "Could not connect to any Redis server in config."
      ))
    };

    let command = RedisCommand::new(RedisCommandKind::ClusterNodes, vec![], None);
    debug!("{} Reading cluster state...", n!(inner));

    request_response(transport, &command).map(|(frame, mut transport)| {
      let _ = transport.close();
      frame
    })
  }).await
   */

  let transport = match create_initial_transport(spawner, &inner).await? {
    Some(t) => t,
    None => return Err(RedisError::new(
      RedisErrorKind::Unknown, "Could not connect to any Redis server in config."
    ))
  };

  let command = RedisCommand::new(RedisCommandKind::ClusterNodes, vec![], None);
  debug!("{} Reading cluster state...", n!(inner));

  request_response(transport, &command).map_ok(|(frame, mut transport)| {
    let _ = transport.close();
    frame
  }).await
}

pub async fn build_cluster_cache(spawner: &Spawner, inner: &Arc<RedisClientInner>) -> Result<ClusterKeyCache, RedisError> {
  let uses_tls = inner.config.read().deref().tls();

  let frame = if uses_tls {
    read_cluster_cache_tls(spawner.clone(), inner).await?
  }else{
    read_cluster_cache(spawner.clone(), inner).await?
  };
  let inner = inner.clone();

  let response = if frame.is_error() {
    match protocol_utils::frame_to_error(frame) {
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

  trace!("{} Cluster state: {}", n!(inner), response);
  ClusterKeyCache::new(Some(response))

  /*
  let ft = if uses_tls {
    read_cluster_cache_tls(spawner.clone(), inner))
  }else{
    Box::pin(read_cluster_cache(spawner.clone(), inner))
  };
  let inner = inner.clone();

  ft.and_then(move |frame| {
    let response = if frame.is_error() {
      match protocol_utils::frame_to_error(frame) {
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

    trace!("{} Cluster state: {}", n!(inner), response);
    ClusterKeyCache::new(Some(response))
  })
  */
}
