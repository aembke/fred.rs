use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::Backpressure::Queue;
use crate::protocol::connection::{Counters, RedisSink, RedisWriter, SentCommand};
use crate::protocol::types::ClusterKeyCache;
use crate::protocol::types::RedisCommand;
use crate::protocol::types::RedisCommandKind::Del;
use crate::types::ClientState;
use crate::utils as client_utils;
use arc_swap::ArcSwap;
use arcstr::ArcStr;
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use tokio::sync::RwLock as AsyncRwLock;

pub mod commands;
pub mod responses;
pub mod sentinel;
pub mod types;
pub mod utils;

pub enum Backpressure {
  /// The server that received the last command.
  Ok(ArcStr),
  /// The amount of time to wait and the command to retry after waiting.
  Wait((Duration, RedisCommand)),
  /// Indicates the command was skipped.
  Skipped,
  /// Queue a command to run later.
  Queue(RedisCommand),
}

#[derive(Clone)]
pub enum Connections<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  Centralized {
    writer: Option<RedisWriter<T>>,
  },
  Clustered {
    // shared with `RedisClientInner`
    cache: ClusterKeyCache,
    writers: HashMap<ArcStr, RedisWriter<T>>,
  },
  Sentinel {
    writer: Option<RedisWriter<T>>,
    server_name: Option<ArcStr>,
  },
}

impl<T> Connections<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  pub fn new_centralized() -> Self<T> {
    Connections::Centralized { writer: None }
  }

  pub fn new_sentinel() -> Self<T> {
    Connections::Sentinel {
      writer: None,
      server_name: None,
    }
  }

  pub fn new_clustered() -> Self {
    Connections::Clustered {
      cache: ClusterKeyCache::new(),
      writers: HashMap::new(),
    }
  }

  /// Disconnect and clear local state for all connections.
  pub fn disconnect_all(&mut self) {
    match self {
      Connections::Centralized { ref mut writer } => {
        *writer = None;
      },
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
      } => {
        cache.clear();
        writers.clear();
      },
      Connections::Sentinel {
        ref mut writer,
        ref mut server_name,
      } => {
        *writer = None;
        *server_name = None;
      },
    }
  }
}

pub struct Multiplexer<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  pub connections: Connections<T>,
  pub inner: Arc<RedisClientInner>,
  pub buffer: VecDeque<SentCommand>,
}

impl<T> Multiplexer<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  pub fn new(inner: &Arc<RedisClientInner>) -> Self<T> {
    let connections = if inner.config.server.is_clustered() {
      Connections::new_clustered()
    } else if inner.config.server.is_sentinel() {
      Connections::new_sentinel()
    } else {
      Connections::new_centralized()
    };

    Multiplexer {
      buffer: VecDeque::new(),
      inner: inner.clone(),
      connections,
    }
  }

  /// Enqueue a command to run later.
  ///
  /// The queue is drained and replayed whenever a `RedisCommandKind::_Sync` or `RedisCommandKind::_Reconnect` message is received.
  pub fn enqueue(&mut self, cmd: RedisCommand, hash_slot: Option<u16>, no_backpressure: bool) {
    self.buffer.push_back(SentCommand {
      command: cmd,
      hash_slot,
      no_backpressure,
      ..Default::default()
    });
  }

  pub fn cluster_state(&self) -> Option<&ClusterKeyCache> {
    if let Connections::Clustered { ref cache, .. } = self.connections {
      Some(cache)
    } else {
      None
    }
  }

  /// Write a command with a custom hash slot, ignoring any keys in the command and skipping any backpressure.
  pub async fn write_with_hash_slot(
    &mut self,
    mut command: RedisCommand,
    hash_slot: u16,
  ) -> Result<Backpressure, RedisError> {
    if utils::max_attempts_reached(&self.inner, &mut command) {
      debug!("{}: Dropping command {}", self.inner.id, command.kind.to_str_debug());
      return Ok(Backpressure::Skipped);
    }
    if command.attempted > 0 {
      client_utils::incr_atomic(&self.inner.redeliver_count);
    }

    if self.inner.config.server.is_clustered() {
      utils::write_clustered_command(&self.inner, &self.connections, command, Some(hash_slot), true).await
    } else {
      utils::write_centralized_command(&self.inner, &self.connections, command, true).await
    }
  }

  /// Write a command to the server(s), signaling back to the caller whether they should implement backpressure.
  pub async fn write(&mut self, mut command: RedisCommand) -> Result<Backpressure, RedisError> {
    if command.kind.is_all_cluster_nodes() {
      return self.write_all_cluster(command).await;
    }
    if utils::max_attempts_reached(&self.inner, &mut command) {
      debug!("{}: Dropping command {}", self.inner.id, command.kind.to_str_debug());
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
    } else if client_utils::is_sentinel(&self.inner.config) {
      sentinel::connect_centralized_from_sentinel(&self.inner, &self.connections, &self.close_tx).await?
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

  pub async fn sync_cluster(&self) -> Result<(), RedisError> {
    client_utils::set_client_state(&self.inner.state, ClientState::Connecting);
    utils::sync_cluster(&self.inner, &self.connections).await?;
    client_utils::set_client_state(&self.inner.state, ClientState::Connected);
    Ok(())
  }

  /// Replay a command, running it without backpressure.
  // TODO make sure ^ actually works and no_backpressure still returns queued, etc
  pub async fn replay_command(&mut self, cmd: SentCommand) -> Result<(), RedisError> {
    let backpressure = if self.inner.config.server.is_clustered() {
      self.write_with_hash_slot(cmd.command, cmd.hash_slot).await?;
    } else if sent_command.command.kind.is_all_cluster_nodes() {
      unimplemented!()
    } else {
      unimplemented!()
    };

    // also need to use write function that allows customizing backpressure

    match backpressure {
      Backpressure::Queue(cmd) => {
        warn!(
          "{}: Failed to replay command: {}. Queueing again to try later...",
          self.inner.id,
          cmd.kind.to_str_debug()
        );
        self.enqueue(cmd, sent_command.hash_slot, sent_command.no_backpressure)
      },
      Backpressure::Wait((dur, cmd)) => {
        unimplemented!()
      },
      _ => {},
    }

    Ok(())
  }

  /// Replay all queued commands.
  ///
  // TODO Any errors returned here should initiate a reconnection.
  pub async fn replay_buffer(&mut self) -> Result<(), RedisError> {
    for sent_command in self.buffer.drain(..) {
      let _ = self.replay_command(sent_command).await?;
    }

    Ok(())
  }
}
