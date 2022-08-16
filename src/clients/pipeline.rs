use crate::error::RedisError;
use crate::interfaces::{AsyncResult, AsyncStream, ClientLike, MultiplexerClient};
use crate::modules::inner::RedisClientInner;
use crate::modules::response::FromRedis;
use crate::prelude::{ReconnectPolicy, RedisConfig, RedisValue};
use crate::protocol::command::RedisCommand;
use crate::types::{
  ClientState, ConnectHandle, CustomCommand, Frame, InfoKind, PerformanceConfig, RespVersion, ShutdownFlags,
};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub struct Pipeline {
  commands: VecDeque<RedisCommand>,
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

impl MultiplexerClient for Pipeline {
  fn send_command(&self) -> Result<(), RedisError> {
    // TODO
    unimplemented!()
  }
}

impl ClientLike for Pipeline {
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}
