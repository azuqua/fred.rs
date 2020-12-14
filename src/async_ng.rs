/*
pub trait SpawnRemote {
  fn spawn_remote_init<F, R>(&self, f: F) -> ()
  where
    F: FnOnce() -> R + Send + 'static,
    R: futures::IntoFuture<Item = (), Error = ()>,
    R::Future: 'static;

  fn spawn_remote<F>(&self, f: F) -> ()
  where
    F: futures::IntoFuture<Item = (), Error = ()>,
    F::Future: 'static;
}
*/

//use futures::*;
//use log::*;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
// use tokio_02::net::{TcpStream, TcpStreamNew};

// FIXME: For the first intermediate release we'll still spawn using the given tokio_compat
// Spawner. This allows us to matinain all the compatability we've maintained so far.
// The subsequienet release can drop that compatability and just spawn tasks on the tokio 0.2
// runtime directly.
//
//pub type Runtime = tokio_compat::runtime::Runtime;
//pub type RuntimeBuilder = tokio_compat::runtime::Builder;
pub type Spawner = tokio_compat::runtime::TaskExecutor;
//pub type Handle = tokio_compat::runtime::TaskExe
pub type JoinHandle<T> = tokio_02::task::JoinHandle<T>;

/*
pub struct SendableFuture<F: Future> {
  ft: F,
}

/// Mark any SendableFuture with the Send trait
unsafe impl<F: Future> Send for SendableFuture<F> {}

/// Implement SendableFuture by polling the underlying Future
impl<F: Future> Future for SendableFuture<F> {
  type Item = F::Item;
  type Error = F::Error;
  fn poll(&mut self) -> futures::Poll<F::Item, F::Error> {
    self.ft.poll()
  }
}

/// Implement SpawnRemote for a tokio-compat executor
///
/// This implementation is potentially unsafe since it wraps the given Future
/// in a SendableFuture unconditionally, however, all such unsafe code can
/// be identified by its path through `spawn_remote{_init}()`
impl SpawnRemote for tokio_compat::runtime::TaskExecutor {
  fn spawn_remote_init<F, R>(&self, f: F) -> ()
  where
    F: FnOnce() -> R + Send + 'static,
    R: futures::IntoFuture<Item = (), Error = ()>,
R::Future: 'static,
  {
    self.spawn(lazy(|| SendableFuture { ft: f().into_future() }))
  }

  fn spawn_remote<F>(&self, f: F) -> ()
  where
    F: futures::IntoFuture<Item = (), Error = ()>,
    F::Future: 'static,
  {
    self.spawn(SendableFuture { ft: f.into_future() })
  }
}

/// Open a TCP stream using the default reactor (ignoring the given Spawner)
///
/// This exists so that existing code (fred) which uses tokio-0.1 can remain
/// consistent regardless of whether it is configured to use legacy async support
/// or ng support. In legacy code (which uses tokio 0.1) it will pass the given
/// Spawner/Handle to TcpStream::connect, in ng code which does not have a tokio 0.1
/// compatible Handle, it calls TcpStream::connect2 to use the default Handle
/// (ignoring the one passed in).
///
/// This is necessary only until an updated version of fred is available (i.e. "soon")
#[deprecated(note = "will be removed after fred update")]
pub fn tcp_connect(addr: &SocketAddr, _spawner: &Spawner) -> TcpStreamNew {
  TcpStream::connect2(&addr) // FIXME: This can go away once fred is updated
}

///
pub trait SpawnRemoteHandle {
  fn spawn_remote_init_handle<F, R>(&self, f: F) -> JoinHandle<Result<(), ()>>
  where
    F: FnOnce() -> R + Send + 'static,
    R: futures::IntoFuture<Item = (), Error = ()>,
    R::Future: 'static;
}

impl SpawnRemoteHandle for tokio_compat::runtime::TaskExecutor {
  fn spawn_remote_init_handle<F, R>(&self, f: F) -> JoinHandle<Result<(), ()>>
  where
    F: FnOnce() -> R + Send + 'static,
    R: futures::IntoFuture<Item = (), Error = ()>,
    R::Future: 'static,
  {
    self.spawn_handle(futures::future::lazy(|| SendableFuture { ft: f().into_future() }))
  }
}
*/
