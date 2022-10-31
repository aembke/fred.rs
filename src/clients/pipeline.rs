use crate::{
  error::RedisError,
  interfaces,
  interfaces::{
    AclInterface,
    AsyncResult,
    AuthInterface,
    ClientInterface,
    ClientLike,
    ClusterInterface,
    ConfigInterface,
    GeoInterface,
    HashesInterface,
    HeartbeatInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    LuaInterface,
    MemoryInterface,
    MetricsInterface,
    PubsubInterface,
    Resp3Frame,
    ServerInterface,
    SetsInterface,
    SlowlogInterface,
    SortedSetsInterface,
    StreamsInterface,
    TransactionInterface,
  },
  modules::{inner::RedisClientInner, response::FromRedis},
  prelude::{ReconnectPolicy, RedisConfig, RedisValue},
  protocol::{
    command::{MultiplexerCommand, RedisCommand},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  types::{
    ClientState,
    ConnectHandle,
    CustomCommand,
    Frame,
    InfoKind,
    PerformanceConfig,
    RespVersion,
    ShutdownFlags,
  },
  utils,
};
use parking_lot::{Mutex, RwLock};
use std::{collections::VecDeque, convert::TryInto, fmt, fmt::Formatter, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// Send a series of commands in a [pipeline](https://redis.io/docs/manual/pipelining/).
///
/// This struct can also be used to ensure that a series of commands run at least once without interruption from other
/// tasks that share the underlying client.
pub struct Pipeline {
  commands: Arc<Mutex<VecDeque<RedisCommand>>>,
  inner:    Arc<RedisClientInner>,
}

#[doc(hidden)]
impl Clone for Pipeline {
  fn clone(&self) -> Self {
    Pipeline {
      commands: self.commands.clone(),
      inner:    self.inner.clone(),
    }
  }
}

impl fmt::Debug for Pipeline {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("Pipeline")
      .field("client", &self.inner.id)
      .field("length", &self.commands.lock().len())
      .finish()
  }
}

impl ClientLike for Pipeline {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }

  #[doc(hidden)]
  fn send_command<C>(&self, command: C) -> Result<(), RedisError>
  where
    C: Into<RedisCommand>,
  {
    let mut command: RedisCommand = command.into();
    if let Some(tx) = command.take_responder() {
      trace!(
        "{}: Respond early to {} command in pipeline.",
        &self.inner.id,
        command.kind.to_str_debug()
      );
      let _ = tx.send(Ok(protocol_utils::queued_frame()));
    }

    self.commands.lock().push_back(command.into());
    Ok(())
  }
}

impl AclInterface for Pipeline {}
impl ClientInterface for Pipeline {}
impl ClusterInterface for Pipeline {}
impl PubsubInterface for Pipeline {}
impl ConfigInterface for Pipeline {}
impl GeoInterface for Pipeline {}
impl HashesInterface for Pipeline {}
impl HyperloglogInterface for Pipeline {}
impl KeysInterface for Pipeline {}
impl ListInterface for Pipeline {}
impl MemoryInterface for Pipeline {}
impl AuthInterface for Pipeline {}
impl ServerInterface for Pipeline {}
impl SlowlogInterface for Pipeline {}
impl SetsInterface for Pipeline {}
impl SortedSetsInterface for Pipeline {}
impl StreamsInterface for Pipeline {}

impl Pipeline {
  /// Send the pipeline and respond with an array of all responses.
  ///
  /// ```rust no_run no_compile
  /// let _ = client.mset(vec![("foo", 1), ("bar", 2)]).await?;
  ///
  /// let pipeline = client.pipeline();
  /// let _ = pipeline.get("foo").await?; // returns when the command is queued in memory
  /// let _ = pipeline.get("bar").await?; // returns when the command is queued in memory
  ///
  /// let results: Vec<i64> = pipeline.all().await?;
  /// assert_eq!(results, vec![1, 2]);
  /// ```
  pub async fn all<R>(self) -> Result<R, RedisError>
  where
    R: FromRedis,
  {
    let commands = { self.commands.lock().drain(..).collect() };
    send_all(&self.inner, commands).await?.convert()
  }

  /// Send the pipeline and respond with only the result of the last command.
  ///
  /// ```rust no_run no_compile
  /// let _ = client.mset(vec![("foo", 1), ("bar", 2)]).await?;
  ///
  /// let pipeline = client.pipeline();
  /// let _ = pipeline.get("foo").await?; // returns when the command is queued in memory
  /// let _ = pipeline.get("bar").await?; // returns when the command is queued in memory
  ///
  /// let result: i64 = pipeline.last().await?;
  /// assert_eq!(results, 2);
  /// ```
  pub async fn last<R>(self) -> Result<R, RedisError>
  where
    R: FromRedis,
  {
    let commands = { self.commands.lock().drain(..).collect() };
    send_last(&self.inner, commands).await?.convert()
  }
}

async fn send_all(inner: &Arc<RedisClientInner>, commands: VecDeque<RedisCommand>) -> Result<RedisValue, RedisError> {
  if commands.is_empty() {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let expected_responses = commands
    .iter()
    .fold(0, |count, cmd| count + cmd.response.expected_response_frames());

  let response = ResponseKind::new_buffer(expected_responses, tx);
  let commands: Vec<RedisCommand> = commands
    .into_iter()
    .enumerate()
    .map(|(idx, mut cmd)| {
      cmd.response = response.clone();
      cmd.response.set_expected_index(idx);
      cmd
    })
    .collect();
  let command = MultiplexerCommand::Pipeline { commands };

  let _ = interfaces::send_to_multiplexer(inner, command)?;
  let frame = utils::apply_timeout(rx, inner.default_command_timeout()).await??;
  protocol_utils::frame_to_results_raw(frame)
}

async fn send_last(
  inner: &Arc<RedisClientInner>,
  commands: VecDeque<RedisCommand>,
) -> Result<RedisValue, RedisError> {
  if commands.is_empty() {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let mut commands: Vec<RedisCommand> = commands.into_iter().collect();
  commands[commands.len() - 1].response = ResponseKind::Respond(Some(tx));
  let command = MultiplexerCommand::Pipeline { commands };

  let _ = interfaces::send_to_multiplexer(inner, command)?;
  let frame = utils::apply_timeout(rx, inner.default_command_timeout()).await??;
  protocol_utils::frame_to_results_raw(frame)
}
