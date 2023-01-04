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
    FunctionInterface,
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
    command::{RouterCommand, RedisCommand},
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
pub struct Pipeline<C: ClientLike> {
  commands: Arc<Mutex<VecDeque<RedisCommand>>>,
  client:   C,
}

#[doc(hidden)]
impl<C: ClientLike> Clone for Pipeline<C> {
  fn clone(&self) -> Self {
    Pipeline {
      commands: self.commands.clone(),
      client:   self.client.clone(),
    }
  }
}

impl<C: ClientLike> fmt::Debug for Pipeline<C> {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("Pipeline")
      .field("client", &self.client.inner().id)
      .field("length", &self.commands.lock().len())
      .finish()
  }
}

#[doc(hidden)]
impl<C: ClientLike> From<C> for Pipeline<C> {
  fn from(client: C) -> Self {
    Pipeline {
      client,
      commands: Arc::new(Mutex::new(VecDeque::new())),
    }
  }
}

impl<C: ClientLike> ClientLike for Pipeline<C> {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.client.inner()
  }

  #[doc(hidden)]
  fn change_command(&self, command: &mut RedisCommand) {
    self.client.change_command(command);
  }

  #[doc(hidden)]
  fn send_command<T>(&self, command: T) -> Result<(), RedisError>
  where
    T: Into<RedisCommand>,
  {
    let mut command: RedisCommand = command.into();
    self.change_command(&mut command);

    if let Some(tx) = command.take_responder() {
      trace!(
        "{}: Respond early to {} command in pipeline.",
        &self.client.inner().id,
        command.kind.to_str_debug()
      );
      let _ = tx.send(Ok(protocol_utils::queued_frame()));
    }

    self.commands.lock().push_back(command.into());
    Ok(())
  }
}

impl<C: AclInterface> AclInterface for Pipeline<C> {}
impl<C: ClientInterface> ClientInterface for Pipeline<C> {}
impl<C: ClusterInterface> ClusterInterface for Pipeline<C> {}
impl<C: PubsubInterface> PubsubInterface for Pipeline<C> {}
impl<C: ConfigInterface> ConfigInterface for Pipeline<C> {}
impl<C: GeoInterface> GeoInterface for Pipeline<C> {}
impl<C: HashesInterface> HashesInterface for Pipeline<C> {}
impl<C: HyperloglogInterface> HyperloglogInterface for Pipeline<C> {}
impl<C: KeysInterface> KeysInterface for Pipeline<C> {}
impl<C: ListInterface> ListInterface for Pipeline<C> {}
impl<C: MemoryInterface> MemoryInterface for Pipeline<C> {}
impl<C: AuthInterface> AuthInterface for Pipeline<C> {}
impl<C: ServerInterface> ServerInterface for Pipeline<C> {}
impl<C: SlowlogInterface> SlowlogInterface for Pipeline<C> {}
impl<C: SetsInterface> SetsInterface for Pipeline<C> {}
impl<C: SortedSetsInterface> SortedSetsInterface for Pipeline<C> {}
impl<C: StreamsInterface> StreamsInterface for Pipeline<C> {}
impl<C: FunctionInterface> FunctionInterface for Pipeline<C> {}

impl<C: ClientLike> Pipeline<C> {
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
    send_all(self.client.inner(), commands).await?.convert()
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
    send_last(self.client.inner(), commands).await?.convert()
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

  let response = ResponseKind::new_buffer_with_size(expected_responses, tx);
  let commands: Vec<RedisCommand> = commands
    .into_iter()
    .enumerate()
    .map(|(idx, mut cmd)| {
      cmd.response = response.duplicate().unwrap_or(ResponseKind::Skip);
      cmd.response.set_expected_index(idx);
      cmd
    })
    .collect();
  let command = RouterCommand::Pipeline { commands };

  let _ = interfaces::send_to_router(inner, command)?;
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
  let command = RouterCommand::Pipeline { commands };

  let _ = interfaces::send_to_router(inner, command)?;
  let frame = utils::apply_timeout(rx, inner.default_command_timeout()).await??;
  protocol_utils::frame_to_results_raw(frame)
}
