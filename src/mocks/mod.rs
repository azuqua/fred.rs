mod utils;
mod types;
mod commands;

use types::*;

use tokio_core::reactor::Handle;

use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  DerefMut,
  Deref
};

use futures::{
  Future,
  Stream
};
use futures::sync::oneshot::{
  channel as oneshot_channel
};
use futures::sync::mpsc::{
  unbounded
};

use crate::multiplexer::Multiplexer;
use crate::client::RedisClientInner;
use crate::error::*;
use crate::protocol::types::*;
use crate::types::*;
use crate::{utils as client_utils, RedisClient};
use crate::protocol::utils as protocol_utils;
use crate::multiplexer::utils as multiplexer_utils;
use std::time::Duration;


pub fn create_commands_ft(handle: Handle, inner: Arc<RedisClientInner>) -> Box<Future<Item=Option<RedisError>, Error=RedisError>> {
  let (tx, rx) = unbounded();
  let expire_tx = tx.clone();
  multiplexer_utils::set_command_tx(&inner, tx);

  let data = utils::global_data_set();

  let expire_ft = inner.timer.interval(Duration::from_secs(1)).map_err(|_| ()).for_each(move |_| {
    trace!("Starting to scan for expired keys.");
    let global_data = utils::global_data_set();

    for data_ref in global_data.read().deref().values() {
      let data_guard = data_ref.read();
      let data = data_guard.deref();
      let expirations = data.expirations.clone();

      let expired = {
        let mut expiration_guard = expirations.write();
        let mut expiration_ref = expiration_guard.deref_mut();

        let expired = expiration_ref.find_expired();
        expiration_ref.cleanup();

        expired
      };

      trace!("Cleaning up mock {} expired keys", expired.len());
      utils::cleanup_keys(&expire_tx, expired);
    }

    Ok::<(), ()>(())
  });
  handle.spawn(expire_ft);

  let connected_inner = inner.clone();
  handle.spawn_fn(move || {
    let client: RedisClient = (&connected_inner).into();

    multiplexer_utils::emit_connect(&connected_inner.connect_tx, &client);
    multiplexer_utils::emit_reconnect(&connected_inner.reconnect_tx, &client);

    Ok::<(), ()>(())
  });

  Box::new(rx.from_err::<RedisError>().fold((handle, inner, 0, None), |(handle, inner, mut db, err), mut command| {
    debug!("{} Handling redis command {:?}", n!(inner), command.kind);
    client_utils::decr_atomic(&inner.cmd_buffer_len);

    if command.kind == RedisCommandKind::Select {
      db = command.args.first().map(|v| {
        match v {
          RedisValue::Integer(i) => *i as u8,
          _ => panic!("Invalid redis database in mock layer.")
        }
      }).unwrap_or(db);
    }
    let data = utils::global_data_set_db(db);

    if command.kind.is_close() {
      debug!("{} Recv close command on the command stream.", n!(inner));

      Err(RedisError::new(
        RedisErrorKind::InvalidCommand, "Close not implemented with mocking layer."
      ))
    } else if command.kind.is_split() {
      Err(RedisError::new(
        RedisErrorKind::InvalidCommand, "Split not implemented with mocking layer."
      ))
    } else {
      let resp_tx = command.tx.take();

      if command.kind == RedisCommandKind::Quit {
        if let Some(resp_tx) = resp_tx {
          let _ = resp_tx.send(utils::ok());
        }

        return Err(RedisError::new_canceled());
      }

      let result = utils::handle_command(&inner, &data, command);
      if let Some(resp_tx) = resp_tx {
        let _ = resp_tx.send(result);
      }

      Ok((handle, inner, db, err))
    }
  })
  .map(|(_, _, _, err)| err)
  .then(|result| match result {
    Ok(e) => Ok(e),
    Err(e) => if e.is_canceled() {
      Ok::<_, RedisError>(None)
    }else{
      Ok::<_, RedisError>(Some(e))
    }
  }))
}