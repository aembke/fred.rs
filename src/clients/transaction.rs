use crate::{
  commands,
  error::{RedisError, RedisErrorKind},
  interfaces,
  interfaces::*,
  modules::inner::RedisClientInner,
  prelude::{ReconnectPolicy, RedisConfig, RedisValue},
  protocol::{
    command::{MultiplexerCommand, RedisCommand, RedisCommandKind},
    hashers::ClusterHash,
    responders::ResponseKind,
    utils as protocol_utils,
    utils::frame_to_results_raw,
  },
  types::{ClientState, ConnectHandle, CustomCommand, FromRedis, InfoKind, PerformanceConfig, ShutdownFlags},
  utils,
};
use arcstr::ArcStr;
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp3::prelude::RespVersion;
use std::{collections::VecDeque, convert::TryInto, fmt, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// A client struct for commands in a MULTI/EXEC transaction block.
///
/// This struct can also be used to ensure that a series of commands run without interruption from other tasks that
/// share the underlying client.
///
/// Commands are queued in memory before being sent to the server. See [exec_raw](Self::exec_raw) for more information
/// on handling errors to intermediate commands.
///
/// ```rust no_run no_compile
/// let _ = client.mset(vec![("foo", 1), ("bar", 2)]).await?;
///
/// let trx = client.multi();
/// let _ = trx.get("foo").await?; // returns QUEUED
/// let _ = trx.get("bar").await?; // returns QUEUED
///
/// let result: (i64, i64) = trx.exec(false).await?;
/// assert_eq!(results, (1, 2));
/// ```
pub struct Transaction {
  id:        u64,
  inner:     Arc<RedisClientInner>,
  commands:  Arc<Mutex<VecDeque<RedisCommand>>>,
  hash_slot: Arc<Mutex<Option<u16>>>,
}

#[doc(hidden)]
impl Clone for Transaction {
  fn clone(&self) -> Self {
    Transaction {
      id:        self.id.clone(),
      inner:     self.inner.clone(),
      commands:  self.commands.clone(),
      hash_slot: self.hash_slot.clone(),
    }
  }
}

impl fmt::Debug for Transaction {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Transaction")
      .field("client", &self.inner.id)
      .field("id", &self.id)
      .field("length", &self.commands.lock().len())
      .field("hash_slot", &self.hash_slot.lock())
      .finish()
  }
}

impl ClientLike for Transaction {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }

  #[doc(hidden)]
  fn send_command<C>(&self, command: C) -> Result<(), RedisError>
  where
    C: Into<RedisCommand>, {
    let mut command: RedisCommand = command.into();
    // check cluster slot mappings as commands are added
    let _ = self.update_hash_slot(&command)?;

    if let Some(tx) = command.take_responder() {
      trace!(
        "{}: Respond early to {} command in transaction.",
        &self.inner.id,
        command.kind.to_str_debug()
      );
      let _ = tx.send(Ok(protocol_utils::queued_frame()));
    }
    self.commands.lock().push_back(command);
    Ok(())
  }
}

impl AclInterface for Transaction {}
impl ClientInterface for Transaction {}
impl PubsubInterface for Transaction {}
impl ConfigInterface for Transaction {}
impl GeoInterface for Transaction {}
impl HashesInterface for Transaction {}
impl HyperloglogInterface for Transaction {}
impl MetricsInterface for Transaction {}
impl KeysInterface for Transaction {}
impl ListInterface for Transaction {}
impl MemoryInterface for Transaction {}
impl AuthInterface for Transaction {}
impl ServerInterface for Transaction {}
impl SetsInterface for Transaction {}
impl SortedSetsInterface for Transaction {}

impl Transaction {
  /// Check and update the hash slot for the transaction.
  pub(crate) fn update_hash_slot(&self, command: &RedisCommand) -> Result<(), RedisError> {
    if !self.inner.config.server.is_clustered() {
      return Ok(());
    }

    if let Some(slot) = command.cluster_hash() {
      if let Some(old_slot) = utils::read_mutex(&self.hash_slot) {
        let (old_server, server) = match self.inner.cluster_state.load().as_ref() {
          Some(state) => {
            debug!(
              "{}: Checking transaction hash slots: {}, {}",
              &self.inner.id, old_slot, slot
            );
            (state.get_server(old_slot).clone(), state.get_server(slot).clone())
          },
          None => {
            return Err(RedisError::new(
              RedisErrorKind::Cluster,
              "Missing cluster state routing table.",
            ))
          },
        };

        if old_server != server {
          return Err(RedisError::new(
            RedisErrorKind::Cluster,
            "All transaction commands must map to the same cluster node.",
          ));
        }
      } else {
        utils::set_mutex(&self.hash_slot, Some(slot));
      }
    }

    Ok(())
  }

  /// Executes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// If `abort_on_error` is `true` the client will automatically send `DISCARD` if an error is received from
  /// any of the commands prior to `EXEC`. This does **not** apply to `MOVED` or `ASK` errors, which wll be followed
  /// automatically.
  ///
  /// <https://redis.io/commands/exec>
  ///
  /// ```rust no_run no_compile
  /// let _ = client.mset(vec![("foo", 1), ("bar", 2)]).await?;
  ///
  /// let trx = client.multi();
  /// let _ = trx.get("foo").await?; // returns QUEUED
  /// let _ = trx.get("bar").await?; // returns QUEUED
  ///
  /// let result: (i64, i64) = trx.exec(false).await?;
  /// assert_eq!(results, (1, 2));
  /// ```
  pub async fn exec<R>(self, abort_on_error: bool) -> Result<R, RedisError>
  where
    R: FromRedis, {
    let commands = { self.commands.lock().drain(..).collect() };
    let hash_slot = utils::take_mutex(&self.hash_slot);
    exec(&self.inner, commands, hash_slot, abort_on_error, self.id)
      .await?
      .convert()
  }

  /// Execute the transaction, returning the result or error for each command.
  ///
  /// Transactions are buffered in memory before calling `EXEC` or `DISCARD`, making it difficult to handle errors
  /// from intermediate commands. This function can be used to act on the raw results from each command once the
  /// transaction is complete.
  ///
  /// The `abort_on_error` flag can cause the returned array to contain fewer results than expected.
  ///
  /// ```rust no_run no_compile
  /// let _ = client.set("foo", 1, None, None, false).await?;
  /// let _ = client.set("bar", 2, None, None, false).await?;
  ///
  /// let trx = client.multi();
  /// let _ = trx.get("foo").await?; // returns QUEUED
  /// let _ = trx.sadd("bar", 2).await?; // returns QUEUED
  ///
  /// println!("{:?}", trx.exec_raw(false).await?);
  /// // [Ok("QUEUED"), Err(RedisError { message: "Invalid command ..." })]
  /// ```
  // TODO integration test this
  pub async fn exec_raw(self, abort_on_error: bool) -> Vec<Result<RedisValue, RedisError>> {
    let commands = { self.commands.lock().drain(..).collect() };
    let hash_slot = utils::take_mutex(&self.hash_slot);
    exec_raw(&self.inner, commands, hash_slot, abort_on_error, self.id)
      .await?
      .convert()
  }

  /// Flushes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/discard>
  pub async fn discard(self) -> Result<(), RedisError> {
    // don't need to do anything here since the commands are queued in memory
    Ok(())
  }

  /// Read the hash slot against which this transaction will run, if known.  
  pub fn hash_slot(&self) -> Option<u16> {
    utils::read_mutex(&self.hash_slot)
  }

  /// Read the server ID against which this transaction will run, if known.
  pub fn cluster_node(&self) -> Option<ArcStr> {
    utils::read_mutex(&self.hash_slot).and_then(|slot| {
      self
        .inner
        .with_cluster_state(|state| Ok(state.get_server(slot).cloned()))
        .ok()
        .and_then(|server| server)
    })
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for Transaction {
  fn from(inner: &'a Arc<RedisClientInner>) -> Self {
    Transaction {
      inner:     inner.clone(),
      commands:  Arc::new(Mutex::new(VecDeque::new())),
      hash_slot: Arc::new(Mutex::new(None)),
      id:        utils::random_u64(u64::MAX),
    }
  }
}

async fn exec(
  inner: &Arc<RedisClientInner>,
  commands: VecDeque<RedisCommand>,
  hash_slot: Option<u16>,
  abort_on_error: bool,
  transaction_id: u64,
) -> Result<RedisValue, RedisError> {
  if commands.is_empty() {
    return Ok(RedisValue::Null);
  }
  let (tx, rx) = oneshot_channel();

  // wrap the commands with MULTI/EXEC
  let mut trx_commands = Vec::with_capacity(commands.len() + 2);
  trx_commands.push(RedisCommandKind::Multi.into());
  for mut command in commands.into_iter() {
    // disconnect intermediate commands from the caller
    command.response = ResponseKind::Skip;
    trx_commands.push(command);
  }
  // attach a oneshot response channel to the EXEC command
  trx_commands.push((RedisCommandKind::Exec, Vec::new(), tx).into());

  // override the pipeline flag and cluster slot mapping
  for mut command in trx_commands.iter_mut() {
    command.can_pipeline = false;
    command.skip_backpressure = true;
    command.transaction_id = Some(transaction_id.clone());
    if let Some(hash_slot) = hash_slot.as_ref() {
      command.hasher = ClusterHash::Custom(hash_slot.clone());
    }
  }
  _trace!(
    inner,
    "Sending transaction {} with {} commands to multiplexer.",
    transaction_id,
    trx_commands.len()
  );
  let command = MultiplexerCommand::Transaction {
    commands: trx_commands,
    id: transaction_id,
    abort_on_error,
  };

  let _ = interfaces::send_to_multiplexer(inner, command)?;
  let frame = utils::apply_timeout(rx, inner.default_command_timeout()).await??;
  protocol_utils::frame_to_results_raw(frame)
}

async fn exec_raw(
  inner: &Arc<RedisClientInner>,
  commands: VecDeque<RedisCommand>,
  hash_slot: Option<u16>,
  abort_on_error: bool,
  transaction_id: u64,
) -> Vec<Result<RedisValue, RedisError>> {
  if commands.is_empty() {
    return Vec::new();
  }
  let (tx, rx) = oneshot_channel();
  // buffer the response from EXEC, but not MULTI
  let mut response = ResponseKind::new_buffer(commands.len() + 1, tx);

  // wrap the commands with MULTI/EXEC
  let mut trx_commands = Vec::with_capacity(commands.len() + 2);
  trx_commands.push(RedisCommandKind::Multi.into());
  for (idx, mut command) in commands.into_iter().enumerate() {
    let mut response = response.clone();
    response.set_expected_index(idx);
    command.response = response;
    trx_commands.push(command);
  }

  response.set_expected_index(commands.len());
  trx_commands.push((RedisCommandKind::Exec, response).into());

  // override the pipeline flag and cluster slot mapping
  for mut command in trx_commands.iter_mut() {
    command.can_pipeline = false;
    command.skip_backpressure = true;
    command.transaction_id = Some(transaction_id.clone());
    if let Some(hash_slot) = hash_slot.as_ref() {
      command.hasher = ClusterHash::Custom(hash_slot.clone());
    }
  }
  _trace!(
    inner,
    "Sending raw transaction {} with {} commands to multiplexer.",
    transaction_id,
    trx_commands.len()
  );
  let command = MultiplexerCommand::Transaction {
    commands: trx_commands,
    id: transaction_id,
    abort_on_error,
  };

  let _ = interfaces::send_to_multiplexer(inner, command)?;
  let frame = utils::apply_timeout(rx, inner.default_command_timeout()).await??;

  if let Resp3Frame::Array { data, .. } = frame {
    data.into_iter().map(|frame| frame_to_results_raw(frame)).collect()
  } else {
    vec![Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Invalid raw transaction response. Expected array.",
    ))]
  }
}
