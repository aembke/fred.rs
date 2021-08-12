use crate::client::RedisClientInner;
use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::protocol::connection::RedisSink;
use crate::protocol::types::ClusterKeyCache;
use crate::protocol::types::RedisCommand;
use crate::utils as client_utils;
use parking_lot::RwLock;
use std::collections::{BTreeMap, VecDeque};

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::RwLock as AsyncRwLock;

pub mod commands;
pub mod responses;
pub mod types;
pub mod utils;

pub type Backpressure = Option<(Duration, RedisCommand)>;
pub type CloseTx = BroadcastSender<RedisError>;
pub type SentCommands = VecDeque<SentCommand>;

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

#[derive(Clone)]
pub enum Connections {
  Centralized {
    writer: Arc<AsyncRwLock<Option<RedisSink>>>,
    commands: Arc<AsyncRwLock<SentCommands>>,
    counters: Counters,
    server: Arc<String>,
  },
  Clustered {
    cache: Arc<RwLock<ClusterKeyCache>>,
    counters: Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
    writers: Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
    commands: Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
  },
}

impl Connections {
  pub fn new_centralized(inner: &Arc<RedisClientInner>, cmd_buffer_len: &Arc<AtomicUsize>) -> Self {
    Connections::Centralized {
      server: utils::centralized_server_name(inner),
      counters: Counters::new(cmd_buffer_len),
      writer: Arc::new(AsyncRwLock::new(None)),
      commands: Arc::new(AsyncRwLock::new(VecDeque::new())),
    }
  }

  pub fn new_clustered() -> Self {
    let cache = ClusterKeyCache::new(None).expect("Couldn't initialize empty cluster cache.");

    Connections::Clustered {
      cache: Arc::new(RwLock::new(cache)),
      writers: Arc::new(AsyncRwLock::new(BTreeMap::new())),
      commands: Arc::new(AsyncRwLock::new(BTreeMap::new())),
      counters: Arc::new(RwLock::new(BTreeMap::new())),
    }
  }
}

#[derive(Clone)]
pub struct Multiplexer {
  pub connections: Connections,
  inner: Arc<RedisClientInner>,
  clustered: bool,
  close_tx: Arc<RwLock<Option<CloseTx>>>,
}

impl Multiplexer {
  pub fn new(inner: &Arc<RedisClientInner>) -> Self {
    let clustered = inner.config.read().is_clustered();
    let connections = if clustered {
      Connections::new_clustered()
    } else {
      Connections::new_centralized(inner, &inner.cmd_buffer_len)
    };

    Multiplexer {
      inner: inner.clone(),
      close_tx: Arc::new(RwLock::new(None)),
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
  pub async fn write_with_hash_slot(&self, mut command: RedisCommand, hash_slot: u16) -> Result<(), RedisError> {
    if utils::max_attempts_reached(&self.inner, &mut command) {
      return Ok(());
    }
    if command.attempted > 0 {
      client_utils::incr_atomic(&self.inner.redeliver_count);
    }

    if self.clustered {
      utils::write_clustered_command(&self.inner, &self.connections, command, Some(hash_slot), true)
        .await
        .map(|_| ())
    } else {
      utils::write_centralized_command(&self.inner, &self.connections, command, true)
        .await
        .map(|_| ())
    }
  }

  /// Write a command to the server(s), signaling back to the caller whether they should implement backpressure.
  pub async fn write(&self, mut command: RedisCommand) -> Result<Backpressure, RedisError> {
    if utils::max_attempts_reached(&self.inner, &mut command) {
      return Ok(None);
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
    if utils::max_attempts_reached(&self.inner, &mut command) {
      return Ok(None);
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

    for command in pending_messages.into_iter() {
      let _ = self.write(command.command).await?;
    }

    Ok(())
  }
}
