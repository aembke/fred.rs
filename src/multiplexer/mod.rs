use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::modules::inner::RedisClientInner;
use crate::protocol::connection::RedisSink;
use crate::protocol::types::ClusterKeyCache;
use crate::protocol::types::RedisCommand;
use crate::types::ClientState;
use crate::utils as client_utils;
use parking_lot::RwLock;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use tokio::sync::RwLock as AsyncRwLock;

pub mod commands;
pub mod responses;
pub mod types;
pub mod utils;

pub type CloseTx = BroadcastSender<RedisError>;
pub type SentCommands = VecDeque<SentCommand>;

pub enum Backpressure {
  /// The server that received the last command.
  Ok(Arc<String>),
  /// The amount of time to wait and the command to retry after waiting.
  Wait((Duration, RedisCommand)),
  /// Indicates the command was skipped.
  Skipped,
}

#[derive(Debug)]
pub struct SentCommand {
  pub command: RedisCommand,
  pub network_start: Option<Instant>,
  pub multi_queued: bool,
}

impl From<RedisCommand> for SentCommand {
  fn from(cmd: RedisCommand) -> Self {
    SentCommand {
      command: cmd,
      network_start: None,
      multi_queued: false,
    }
  }
}

#[derive(Clone, Debug)]
pub struct Counters {
  pub cmd_buffer_len: Arc<AtomicUsize>,
  pub in_flight: Arc<AtomicUsize>,
  pub feed_count: Arc<AtomicUsize>,
}

impl Counters {
  pub fn new(cmd_buffer_len: &Arc<AtomicUsize>) -> Self {
    Counters {
      cmd_buffer_len: cmd_buffer_len.clone(),
      in_flight: Arc::new(AtomicUsize::new(0)),
      feed_count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn should_send(&self) -> bool {
    client_utils::read_atomic(&self.feed_count) > globals().feed_count()
      || client_utils::read_atomic(&self.cmd_buffer_len) == 0
  }

  pub fn incr_feed_count(&self) -> usize {
    client_utils::incr_atomic(&self.feed_count)
  }

  pub fn incr_in_flight(&self) -> usize {
    client_utils::incr_atomic(&self.in_flight)
  }

  pub fn decr_in_flight(&self) -> usize {
    client_utils::decr_atomic(&self.in_flight)
  }

  pub fn reset_feed_count(&self) {
    client_utils::set_atomic(&self.feed_count, 0);
  }

  pub fn reset_in_flight(&self) {
    client_utils::set_atomic(&self.in_flight, 0);
  }
}

#[derive(Clone, Debug)]
pub enum ConnectionIDs {
  Centralized(Arc<RwLock<Option<i64>>>),
  Clustered(Arc<RwLock<BTreeMap<Arc<String>, i64>>>),
}

#[derive(Clone)]
pub enum Connections {
  Centralized {
    writer: Arc<AsyncRwLock<Option<RedisSink>>>,
    commands: Arc<AsyncRwLock<SentCommands>>,
    counters: Counters,
    server: Arc<String>,
    connection_id: Arc<RwLock<Option<i64>>>,
  },
  Clustered {
    cache: Arc<RwLock<ClusterKeyCache>>,
    counters: Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
    writers: Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
    commands: Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
    connection_ids: Arc<RwLock<BTreeMap<Arc<String>, i64>>>,
  },
}

impl Connections {
  pub fn new_centralized(inner: &Arc<RedisClientInner>, cmd_buffer_len: &Arc<AtomicUsize>) -> Self {
    Connections::Centralized {
      server: utils::centralized_server_name(inner),
      counters: Counters::new(cmd_buffer_len),
      writer: Arc::new(AsyncRwLock::new(None)),
      commands: Arc::new(AsyncRwLock::new(VecDeque::new())),
      connection_id: Arc::new(RwLock::new(None)),
    }
  }

  pub fn new_clustered() -> Self {
    let cache = ClusterKeyCache::new(None).expect("Couldn't initialize empty cluster cache.");

    Connections::Clustered {
      cache: Arc::new(RwLock::new(cache)),
      writers: Arc::new(AsyncRwLock::new(BTreeMap::new())),
      commands: Arc::new(AsyncRwLock::new(BTreeMap::new())),
      counters: Arc::new(RwLock::new(BTreeMap::new())),
      connection_ids: Arc::new(RwLock::new(BTreeMap::new())),
    }
  }
}

#[derive(Clone)]
pub struct Multiplexer {
  pub connections: Connections,
  inner: Arc<RedisClientInner>,
  clustered: bool,
  close_tx: Arc<RwLock<Option<CloseTx>>>,
  synchronizing_tx: Arc<RwLock<VecDeque<OneshotSender<()>>>>,
  synchronizing: Arc<RwLock<bool>>,
}

impl Multiplexer {
  pub fn new(inner: &Arc<RedisClientInner>) -> Self {
    let clustered = inner.config.read().server.is_clustered();
    let connections = if clustered {
      Connections::new_clustered()
    } else {
      Connections::new_centralized(inner, &inner.cmd_buffer_len)
    };

    Multiplexer {
      inner: inner.clone(),
      close_tx: Arc::new(RwLock::new(None)),
      synchronizing_tx: Arc::new(RwLock::new(VecDeque::new())),
      synchronizing: Arc::new(RwLock::new(false)),
      clustered,
      connections,
    }
  }

  pub fn cluster_state(&self) -> Option<ClusterKeyCache> {
    if let Connections::Clustered { ref cache, .. } = self.connections {
      Some(cache.read().clone())
    } else {
      None
    }
  }

  /// Write a command with a custom hash slot, ignoring any keys in the command and skipping any backpressure.
  pub async fn write_with_hash_slot(
    &self,
    mut command: RedisCommand,
    hash_slot: u16,
  ) -> Result<Backpressure, RedisError> {
    let _ = self.wait_for_sync().await;

    if utils::max_attempts_reached(&self.inner, &mut command) {
      return Ok(Backpressure::Skipped);
    }
    if command.attempted > 0 {
      client_utils::incr_atomic(&self.inner.redeliver_count);
    }

    if self.clustered {
      utils::write_clustered_command(&self.inner, &self.connections, command, Some(hash_slot), true).await
    } else {
      utils::write_centralized_command(&self.inner, &self.connections, command, true).await
    }
  }

  /// Write a command to the server(s), signaling back to the caller whether they should implement backpressure.
  pub async fn write(&self, mut command: RedisCommand) -> Result<Backpressure, RedisError> {
    let _ = self.wait_for_sync().await;

    if command.kind.is_all_cluster_nodes() {
      return self.write_all_cluster(command).await;
    }
    if utils::max_attempts_reached(&self.inner, &mut command) {
      return Ok(Backpressure::Skipped);
    }
    if command.attempted > 0 {
      client_utils::incr_atomic(&self.inner.redeliver_count);
    }

    if self.clustered {
      let custom_key_slot = command.key_slot();
      utils::write_clustered_command(&self.inner, &self.connections, command, custom_key_slot, false).await
    } else {
      utils::write_centralized_command(&self.inner, &self.connections, command, false).await
    }
  }

  /// Write a command to all nodes in the cluster.
  pub async fn write_all_cluster(&self, mut command: RedisCommand) -> Result<Backpressure, RedisError> {
    let _ = self.wait_for_sync().await;

    if utils::max_attempts_reached(&self.inner, &mut command) {
      return Ok(Backpressure::Skipped);
    }
    if command.attempted > 0 {
      client_utils::incr_atomic(&self.inner.redeliver_count);
    }

    if let Connections::Clustered {
      ref writers,
      ref commands,
      ref counters,
      ..
    } = self.connections
    {
      utils::write_all_nodes(&self.inner, writers, commands, counters, command).await
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected clustered redis deployment.",
      ))
    }
  }

  pub async fn connect_and_flush(&self) -> Result<(), RedisError> {
    let pending_messages = if self.clustered {
      let messages = utils::connect_clustered(&self.inner, &self.connections, &self.close_tx).await?;
      self.inner.update_cluster_state(self.cluster_state());
      messages
    } else {
      utils::connect_centralized(&self.inner, &self.connections, &self.close_tx).await?
    };
    self
      .inner
      .backchannel
      .write()
      .await
      .set_connection_ids(self.read_connection_ids());

    for command in pending_messages.into_iter() {
      let _ = self.write(command.command).await?;
    }

    Ok(())
  }

  pub fn read_connection_ids(&self) -> ConnectionIDs {
    match self.connections {
      Connections::Clustered { ref connection_ids, .. } => ConnectionIDs::Clustered(connection_ids.clone()),
      Connections::Centralized { ref connection_id, .. } => ConnectionIDs::Centralized(connection_id.clone()),
    }
  }

  pub fn is_synchronizing(&self) -> bool {
    *self.synchronizing.read()
  }

  pub fn set_synchronizing(&self, synchronizing: bool) {
    let mut guard = self.synchronizing.write();
    *guard = synchronizing;
  }

  pub fn check_and_set_sync(&self) -> bool {
    let mut guard = self.synchronizing.write();
    if *guard {
      true
    } else {
      *guard = false;
      false
    }
  }

  pub async fn wait_for_sync(&self) -> Result<(), RedisError> {
    let inner = &self.inner;
    if !self.is_synchronizing() {
      _trace!(inner, "Skip waiting on cluster sync.");
      return Ok(());
    }

    let (tx, rx) = oneshot_channel();
    self.synchronizing_tx.write().push_back(tx);
    _debug!(inner, "Waiting on cluster sync to finish.");
    let _ = rx.await?;
    _debug!(inner, "Finished waiting on cluster sync.");

    Ok(())
  }

  pub async fn sync_cluster(&self) -> Result<(), RedisError> {
    if self.check_and_set_sync() {
      // dont return here. if multiple consecutive repair commands come in while one is running we still want to run them all, but not concurrently.
      let _ = self.wait_for_sync().await?;
    }
    utils::sync_cluster(&self.inner, &self.connections, &self.close_tx).await?;

    self.set_synchronizing(false);
    client_utils::set_client_state(&self.inner.state, ClientState::Connected);
    utils::finish_synchronizing(&self.inner, &self.synchronizing_tx);
    Ok(())
  }
}
