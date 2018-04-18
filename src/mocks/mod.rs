#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

mod types;
mod commands;
mod utils;

use std::sync::Arc;

use ::types::*;
use ::protocol::types::*;

use ::RedisClient;

use error::{
  RedisError,
  RedisErrorKind
};

use ::protocol::types::{
  RedisCommand,
  RedisCommandKind
};

use ::utils as client_utils;

use futures::future::{
  loop_fn,
  Loop,
  lazy
};
use futures::Future;
use futures::sync::oneshot::{
  Sender as OneshotSender,
};
use futures::sync::mpsc::{
  UnboundedSender,
  UnboundedReceiver,
  unbounded
};
use futures::stream::Stream;

use tokio_core::reactor::{
  Handle
};
use tokio_timer::Timer;

use parking_lot::{
  RwLock
};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

/// A future that resolves when the connection to the Redis server closes.
pub type ConnectionFuture = Box<Future<Item=Option<RedisError>, Error=RedisError>>;

pub fn init_with_policy(client: RedisClient,
                        handle: &Handle,
                        config: Rc<RefCell<RedisConfig>>,
                        state: Arc<RwLock<ClientState>>,
                        closed: Arc<RwLock<bool>>,
                        error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
                        message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
                        command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
                        reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
                        connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
                        remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>,
                        policy: ReconnectPolicy)
  -> Box<Future<Item=(), Error=RedisError>>
{








  unimplemented!()
}

pub fn init(client: RedisClient,
            handle: &Handle,
            config: Rc<RefCell<RedisConfig>>,
            state: Arc<RwLock<ClientState>>,
            error_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisError>>>>,
            message_tx: Rc<RefCell<VecDeque<UnboundedSender<(String, RedisValue)>>>>,
            command_tx: Rc<RefCell<Option<UnboundedSender<RedisCommand>>>>,
            connect_tx: Rc<RefCell<VecDeque<OneshotSender<Result<RedisClient, RedisError>>>>>,
            reconnect_tx: Rc<RefCell<VecDeque<UnboundedSender<RedisClient>>>>,
            remote_tx: Rc<RefCell<VecDeque<OneshotSender<Result<(), RedisError>>>>>)
  -> ConnectionFuture
{





  unimplemented!()
}