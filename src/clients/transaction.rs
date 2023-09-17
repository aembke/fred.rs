use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  interfaces::*,
  modules::inner::RedisClientInner,
  prelude::RedisValue,
  protocol::{
    command::{RedisCommand, RedisCommandKind, RouterCommand},
    hashers::ClusterHash,
    responders::ResponseKind,
    utils as protocol_utils,
  },
  types::{FromRedis, MultipleKeys, RedisKey, Server},
  utils,
};
use parking_lot::Mutex;
use std::{collections::VecDeque, fmt, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// A client struct for commands in a `MULTI`/`EXEC` transaction block.
pub struct Transaction {
  id:        u64,
  inner:     Arc<RedisClientInner>,
  commands:  Arc<Mutex<VecDeque<RedisCommand>>>,
  watched:   Arc<Mutex<VecDeque<RedisKey>>>,
  hash_slot: Arc<Mutex<Option<u16>>>,
}

#[doc(hidden)]
impl Clone for Transaction {
  fn clone(&self) -> Self {
    Transaction {
      id:        self.id.clone(),
      inner:     self.inner.clone(),
      commands:  self.commands.clone(),
      watched:   self.watched.clone(),
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
    C: Into<RedisCommand>,
  {
    let mut command: RedisCommand = command.into();
    let _ = self.disallow_all_cluster_commands(&command)?;
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
impl StreamsInterface for Transaction {}
impl FunctionInterface for Transaction {}

impl Transaction {
  /// Check and update the hash slot for the transaction.
  pub(crate) fn update_hash_slot(&self, command: &RedisCommand) -> Result<(), RedisError> {
    if !self.inner.config.server.is_clustered() {
      return Ok(());
    }

    if let Some(slot) = command.cluster_hash() {
      if let Some(old_slot) = utils::read_mutex(&self.hash_slot) {
        let (old_server, server) = self.inner.with_cluster_state(|state| {
          debug!(
            "{}: Checking transaction hash slots: {}, {}",
            &self.inner.id, old_slot, slot
          );

          Ok((state.get_server(old_slot).cloned(), state.get_server(slot).cloned()))
        })?;

        if old_server != server {
          return Err(RedisError::new(
            RedisErrorKind::Cluster,
            "All transaction commands must use the same cluster node.",
          ));
        }
      } else {
        utils::set_mutex(&self.hash_slot, Some(slot));
      }
    }

    Ok(())
  }

  pub(crate) fn disallow_all_cluster_commands(&self, command: &RedisCommand) -> Result<(), RedisError> {
    if command.kind.is_all_cluster_nodes() {
      Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Cannot use concurrent cluster commands inside a transaction.",
      ))
    } else {
      Ok(())
    }
  }

  /// Executes all previously queued commands in a transaction.
  ///
  /// If `abort_on_error` is `true` the client will automatically send `DISCARD` if an error is received from
  /// any of the commands prior to `EXEC`. This does **not** apply to `MOVED` or `ASK` errors, which wll be followed
  /// automatically.
  ///
  /// <https://redis.io/commands/exec>
  ///
  /// ```rust no_run
  /// # use fred::prelude::*;
  ///
  /// async fn example(client: &RedisClient) -> Result<(), RedisError> {
  ///   let _ = client.mset(vec![("foo", 1), ("bar", 2)]).await?;
  ///
  ///   let trx = client.multi();
  ///   let _: () = trx.get("foo").await?; // returns QUEUED
  ///   let _: () = trx.get("bar").await?; // returns QUEUED
  ///
  ///   let (foo, bar): (i64, i64) = trx.exec(false).await?;
  ///   assert_eq!((foo, bar), (1, 2));
  ///   Ok(())
  /// }
  /// ```
  pub async fn exec<R>(self, abort_on_error: bool) -> Result<R, RedisError>
  where
    R: FromRedis,
  {
    let commands = { self.commands.lock().drain(..).collect() };
    let watched = { self.watched.lock().drain(..).collect() };
    let hash_slot = utils::take_mutex(&self.hash_slot);
    exec(&self.inner, commands, watched, hash_slot, abort_on_error, self.id)
      .await?
      .convert()
  }

  /// Send the `WATCH` command with the provided keys before starting the transaction.
  pub fn watch_before<K>(&self, keys: K)
  where
    K: Into<MultipleKeys>,
  {
    self.watched.lock().extend(keys.into().inner());
  }

  /// Flushes all previously queued commands in a transaction.
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
  pub fn cluster_node(&self) -> Option<Server> {
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
    let mut commands = VecDeque::with_capacity(4);
    commands.push_back(RedisCommandKind::Multi.into());

    Transaction {
      inner:     inner.clone(),
      commands:  Arc::new(Mutex::new(commands)),
      watched:   Arc::new(Mutex::new(VecDeque::new())),
      hash_slot: Arc::new(Mutex::new(None)),
      id:        utils::random_u64(u64::MAX),
    }
  }
}

async fn exec(
  inner: &Arc<RedisClientInner>,
  commands: VecDeque<RedisCommand>,
  watched: VecDeque<RedisKey>,
  hash_slot: Option<u16>,
  abort_on_error: bool,
  id: u64,
) -> Result<RedisValue, RedisError> {
  if commands.is_empty() {
    return Ok(RedisValue::Null);
  }
  let (tx, rx) = oneshot_channel();

  let commands: Vec<RedisCommand> = commands
    .into_iter()
    .map(|mut command| {
      command.response = ResponseKind::Skip;
      command.can_pipeline = false;
      command.skip_backpressure = true;
      command.transaction_id = Some(id.clone());
      if let Some(hash_slot) = hash_slot.as_ref() {
        command.hasher = ClusterHash::Custom(hash_slot.clone());
      }
      command
    })
    .collect();
  // collapse the watched keys into one command
  let watched = if watched.is_empty() {
    None
  } else {
    let args: Vec<RedisValue> = watched.into_iter().map(|k| k.into()).collect();
    let mut watch_cmd = RedisCommand::new(RedisCommandKind::Watch, args);
    watch_cmd.can_pipeline = false;
    watch_cmd.skip_backpressure = true;
    watch_cmd.transaction_id = Some(id.clone());
    if let Some(hash_slot) = hash_slot.as_ref() {
      watch_cmd.hasher = ClusterHash::Custom(hash_slot.clone());
    }
    Some(watch_cmd)
  };

  _trace!(
    inner,
    "Sending transaction {} with {} commands ({} watched) to router.",
    id,
    commands.len(),
    watched.as_ref().map(|c| c.args().len()).unwrap_or(0)
  );
  let mut command = RouterCommand::Transaction {
    id,
    tx,
    commands,
    watched,
    abort_on_error,
  };
  command.inherit_options(inner);
  let timeout_dur = command.timeout_dur().unwrap_or_else(|| inner.default_command_timeout());

  let _ = interfaces::send_to_router(inner, command)?;
  let frame = utils::apply_timeout(rx, timeout_dur).await??;
  protocol_utils::frame_to_results(frame)
}
