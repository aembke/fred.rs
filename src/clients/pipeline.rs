use crate::error::RedisError;
use crate::interfaces::{AsyncResult, AsyncStream, ClientLike, MultiplexerClient};
use crate::modules::inner::RedisClientInner;
use crate::modules::response::FromRedis;
use crate::prelude::{ReconnectPolicy, RedisConfig, RedisValue};
use crate::protocol::command::{QueuedCommand, RedisCommand};
use crate::types::{
  ClientState, ConnectHandle, CustomCommand, Frame, InfoKind, PerformanceConfig, RespVersion, ShutdownFlags,
};
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub struct Pipeline {
  commands: Arc<Mutex<VecDeque<RedisCommand>>>,
  inner: Arc<RedisClientInner>,
}

impl fmt::Debug for Pipeline {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("Pipeline")
      .field("id", &self.inner.id)
      .field("length", &self.commands.len())
      .finish()
  }
}

impl ClientLike for Pipeline {
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }

  fn send_command<C>(&self, command: C) -> Result<(), RedisError>
  where
    C: Into<RedisCommand>,
  {
    // TODO respond early to the caller here
    self.commands.lock().push_back(command.into());
    Ok(())
  }
}

impl Pipeline {
  /// Send the pipeline and respond with an array of all responses.
  pub async fn send_all<R>(self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send + 'static,
  {
    // TODO figure out what to do with errors here
    unimplemented!()
  }

  /// Send the pipeline and respond with only the result of the last command.
  ///
  /// The first error to be received will be returned instead, if needed.
  pub async fn send_last<R>(self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send + 'static,
  {
    unimplemented!()
  }
}
