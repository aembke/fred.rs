use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::Backpressure::Queue;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::connection::{CommandBuffer, Counters, RedisSink, RedisWriter, SentCommand};
use crate::protocol::types::ClusterRouting;
use crate::types::ServerConfig;
use crate::types::{ClientState, RedisConfig};
use crate::utils as client_utils;
use arc_swap::ArcSwap;
use arcstr::ArcStr;
use futures::future::{join_all, try_join_all};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use tokio::sync::RwLock as AsyncRwLock;

pub mod centralized;
pub mod clustered;
pub mod commands;
pub mod reader;
pub mod responses;
pub mod sentinel;
pub mod types;
pub mod utils;

/// The result of an attempt to send a command to the server.
pub enum Written<'a> {
  /// Apply backpressure to the command before retrying.
  Backpressure((RedisCommand, Backpressure)),
  /// Indicates that the command was sent to the associated server and whether the socket was flushed.
  Sent((&'a ArcStr, bool)),
  /// Disconnect from the provided server and retry the command later.
  Disconnect((&'a ArcStr, Option<RedisCommand>)),
  /// Indicates that the result should be ignored since the command will not be retried.
  Ignore,
}

pub enum Backpressure {
  /// The amount of time to wait.
  Wait(Duration),
  /// Block the client until the command receives a response.
  Block,
  /// Return a backpressure error to the caller of the command.
  Error(RedisError),
}

#[derive(Clone)]
pub enum Connections<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  Centralized {
    /// The connection to the server.
    writer: Option<RedisWriter<T>>,
  },
  Clustered {
    /// The cached cluster routing table used for mapping keys to server IDs.
    cache: ClusterRouting,
    /// A map of server IDs and connections.
    writers: HashMap<ArcStr, RedisWriter<T>>,
  },
  Sentinel {
    /// The connection to the primary server.
    writer: Option<RedisWriter<T>>,
    /// The name of the primary server as provided from the sentinel interface.
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
      cache: ClusterRouting::new(),
      writers: HashMap::new(),
    }
  }

  /// Initialize the underlying connection(s).
  pub async fn initialize(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    utils::initialize_connections(inner, self).await
  }

  /// Read the counters associated with a connection to a server.
  pub fn counters(&self, server: Option<&ArcStr>) -> Option<&Counters> {
    match self {
      Connections::Centralized { ref writer } => writer.as_ref().map(|w| &w.counters),
      Connections::Sentinel { ref writer, .. } => writer.as_ref().map(|w| &w.counters),
      Connections::Clustered { ref writers, .. } => {
        server.and_then(|server| writers.get(server).map(|w| &w.counters))
      },
    }
  }

  /// Disconnect from the provided server, using the default centralized connection if `None` is provided.
  pub async fn disconnect(&mut self, server: Option<&ArcStr>) -> CommandBuffer {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          writer.graceful_close().await
        } else {
          VecDeque::new()
        }
      },
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
        ..
      } => {
        let mut out = VecDeque::new();

        if let Some(server) = server {
          if let Some(writer) = writers.remove(server) {
            let commands = writer.graceful_close().await;
            out.extend(commands.into_iter());
          }
        }
        out
      },
      Connections::Sentinel {
        ref mut writer,
        ref mut server_name,
      } => {
        *server_name = None;
        if let Some(writer) = writer.take() {
          writer.graceful_close().await
        } else {
          VecDeque::new()
        }
      },
    }
  }

  /// Disconnect and clear local state for all connections, returning all in-flight commands.
  pub async fn disconnect_all(&mut self) -> CommandBuffer {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          writer.graceful_close().await
        } else {
          VecDeque::new()
        }
      },
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
        ..
      } => {
        let mut out = VecDeque::new();
        for (_, writer) in writers.drain() {
          let commands = writer.graceful_close().await;
          out.extend(commands.into_iter());
        }
        out
      },
      Connections::Sentinel {
        ref mut writer,
        ref mut server_name,
      } => {
        *server_name = None;
        if let Some(writer) = writer.take() {
          writer.graceful_close().await
        } else {
          VecDeque::new()
        }
      },
    }
  }

  /// Read a map of connection IDs (via `CLIENT ID`) for each inner connection.
  pub fn connection_ids(&self) -> HashMap<ArcStr, i64> {
    let mut out = HashMap::new();

    match self {
      Connections::Centralized { writer } => {
        if let Some(writer) = writer {
          if let Some(id) = writer.id {
            out.insert(writer.server.clone(), id);
          }
        }
      },
      Connections::Sentinel { writer, .. } => {
        if let Some(writer) = writer {
          if let Some(id) = writer.id {
            out.insert(writer.server.clone(), id);
          }
        }
      },
      Connections::Clustered { writers, .. } => {
        for (server, writer) in writers.iter() {
          if let Some(id) = writer.id {
            out.insert(server.clone(), id);
          }
        }
      },
    }

    out
  }

  /// Flush the socket(s) associated with each server if they have pending frames.
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer {
          writer.check_and_flush().await
        } else {
          Ok(())
        }
      },
      Connections::Sentinel { ref mut writer, .. } => {
        if let Some(writer) = writer {
          writer.check_and_flush().await
        } else {
          Ok(())
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        try_join_all(writers.values_mut().map(|writer| writer.check_and_flush())).await
      },
    }
  }

  /// Send a command to the server(s).
  pub async fn write_command(
    &mut self,
    inner: &Arc<RedisClientInner>,
    command: RedisCommand,
  ) -> Result<Written, (RedisError, RedisCommand)> {
    match self {
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
      } => clustered::send_command(inner, writers, cache, command).await,
      Connections::Centralized { ref mut writer } => centralized::send_command(inner, writer, command).await,
      Connections::Sentinel { ref mut writer, .. } => centralized::send_command(inner, writer, command).await,
    }
  }

  /// Send a command to all servers in a cluster.
  pub async fn write_all_cluster(&mut self, command: RedisCommand) -> Result<(), RedisError> {
    // TODO should this revert to `write_command` when the server is not clustered?
    unimplemented!()
  }
}

// TODO
// Moved((slot, server, command)):
//   check if the hash slot maps to the provided server, if so then run the command early
//   sync the cluster state
//   create or drop connections
//     need to check for unknown port format in error message (:6379) and re-use the same endpoint but with that port
//   run the command
// ASK((slot, server, command)):
//   do not sync the cluster, but check if the connection exists
//   if not then sync the cluster
//   send the ASKING command
//   run the command

// TODO
// in a transaction if an ASKING error is received instead of QUEUED
// then change the hash slot on the transaction, send ASKING, and send all the commands

pub struct Multiplexer<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  pub connections: Connections<T>,
  pub inner: Arc<RedisClientInner>,
  pub buffer: VecDeque<RedisCommand>,
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

  /// Read the cluster state, if known.
  pub fn cluster_state(&self) -> Option<&ClusterRouting> {
    if let Connections::Clustered { ref cache, .. } = self.connections {
      Some(cache)
    } else {
      None
    }
  }

  /// Queue the command to run later.
  ///
  /// The internal buffer is drained whenever a `Reconnect` or `Sync` command is processed.
  pub fn queue_command(&mut self, command: RedisCommand) {
    self.buffer.push_back(command);
  }

  /// Drain and return the buffered commands.
  pub fn take_command_buffer(&mut self) -> VecDeque<RedisCommand> {
    self.buffer.drain(..).collect()
  }

  /// Send a command to the server.
  ///
  /// If the command cannot be written:
  ///   * The command will be queued to run later.
  ///   * The associated connection will be dropped.
  ///   * The reader task for that connection will close, sending a `Reconnect` message to the multiplexer.
  ///
  /// Errors are handled internally, but may be returned if the command was queued to run later.
  pub async fn write_command(&mut self, mut command: RedisCommand) -> Result<Written, RedisError> {
    if let Err(e) = command.incr_check_attempted(self.inner.max_command_attempts()) {
      debug!(
        "{}: Skipping command `{}` after too many failed attempts.",
        self.inner.id,
        command.kind.to_str_debug()
      );
      command.respond_to_caller(Err(e));
      return Ok(Written::Ignore);
    }
    if command.attempted > 1 {
      self.inner.counters.incr_redelivery_count();
    }

    if command.kind.is_all_cluster_nodes() {
      self
        .connections
        .write_all_cluster(command)
        .await
        .map(|_| Written::Ignore)
    } else {
      self
        .connections
        .write_command(&self.inner, command)
        .await
        .map_err(|(error, command)| {
          self.buffer.push_back(command);
          error
        })
    }
  }

  /// Disconnect from all the servers, moving the in-flight messages to the internal command buffer and triggering a reconnection, if necessary.
  pub async fn disconnect_all(&mut self) {
    let commands = self.connections.disconnect_all().await;
    self.buffer.extend(commands);
  }

  /// Connect to the server(s), discarding any previous connection state.
  pub async fn connect(&mut self) -> Result<(), RedisError> {
    if self.inner.config.server.is_clustered() {
      unimplemented!()
    } else {
      unimplemented!()
    }
  }

  /// Sync the cached cluster state with the server via `CLUSTER SLOTS`.
  ///
  /// This will also create new connections or drop old connections as needed.
  pub async fn sync_cluster(&self) -> Result<(), RedisError> {
    utils::sync_cluster(&self.inner, &self.connections).await?;
    Ok(())
  }

  /// Replay all queued commands on the internal buffer without backpressure.
  ///
  /// If a command cannot be written the underlying connections will close.
  pub async fn replay_buffer(&mut self) -> Result<(), RedisError> {
    for mut command in self.buffer.drain(..) {
      command.skip_backpressure = true;

      if let Err(e) = self.write_command(command).await {
        warn!("{}: Error replaying command: {:?}", self.inner.id, e);
        self.disconnect().await; // triggers a reconnect if needed
        break;
      }
    }

    Ok(())
  }
}
