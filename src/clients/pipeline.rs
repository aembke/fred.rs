use crate::{
  error::RedisError,
  interfaces,
  interfaces::{
    AclInterface,
    AuthInterface,
    ClientInterface,
    ClientLike,
    ClusterInterface,
    ConfigInterface,
    GeoInterface,
    HashesInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    MemoryInterface,
    PubsubInterface,
    ServerInterface,
    SetsInterface,
    SlowlogInterface,
    SortedSetsInterface,
    StreamsInterface,
  },
  modules::{inner::RedisClientInner, response::FromRedis},
  prelude::RedisValue,
  protocol::{
    command::{MultiplexerCommand, RedisCommand},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  utils,
};
use parking_lot::Mutex;
use std::{collections::VecDeque, fmt, fmt::Formatter, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// Send a series of commands in a [pipeline](https://redis.io/docs/manual/pipelining/).
///
/// The `auto_pipeline` flag on the [PerformanceConfig](crate::types::PerformanceConfig) determines whether
/// the client will pipeline commands across tasks whereas this struct is used to pipeline commands within one task. A
/// sequence of commands in a `Pipeline` or `Transaction` cannot be interrupted by other tasks.
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

#[doc(hidden)]
impl From<&Arc<RedisClientInner>> for Pipeline {
  fn from(inner: &Arc<RedisClientInner>) -> Self {
    Pipeline {
      inner:    inner.clone(),
      commands: Arc::new(Mutex::new(VecDeque::new())),
    }
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
  /// let pipeline = client.pipeline();
  /// let _ = pipeline.incr("foo").await?; // returns when the command is queued in memory
  /// let _ = pipeline.incr("foo").await?; // returns when the command is queued in memory
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
      cmd.response = response.duplicate().unwrap_or(ResponseKind::Skip);
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
    return Ok(RedisValue::Null);
  }

  let len = commands.len();
  let (tx, rx) = oneshot_channel();
  let mut commands: Vec<RedisCommand> = commands.into_iter().collect();
  commands[len - 1].response = ResponseKind::Respond(Some(tx));
  let command = MultiplexerCommand::Pipeline { commands };

  let _ = interfaces::send_to_multiplexer(inner, command)?;
  let frame = utils::apply_timeout(rx, inner.default_command_timeout()).await??;
  protocol_utils::frame_to_results_raw(frame)
}
