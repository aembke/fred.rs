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
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize};
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
  /// The server ID of the connection used to send the command.
  Ok(ArcStr),
  /// The amount of time to wait and the command to retry after waiting.
  Wait((Duration, RedisCommand)),
  /// Block the client until the command receives a response.
  Block,
  /// Indicates the command was skipped.
  Skipped,
  /// Queue the command to run later.
  Queue(RedisCommand),
}

#[derive(Clone)]
pub enum Connections<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  Centralized {
    /// The connection to the server.
    writer: Option<RedisWriter<T>>,
  },
  Clustered {
    /// A list of known nodes in the cluster.
    ///
    /// The cluster topology can change such that at some point an entirely new set of nodes may be exposed where none of the
    /// original endpoints provided in the `RedisConfig` can be reached.
    known_nodes: BTreeSet<(String, u16)>,
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
      known_nodes: BTreeSet::new(),
      writers: HashMap::new(),
    }
  }

  /// Clear the set of known nodes for the server and replace with the initial values in the `RedisConfig`.
  pub fn reset_known_nodes(&mut self, config: &RedisConfig) {
    if let Connections::Clustered {
      ref mut known_nodes, ..
    } = self
    {
      known_nodes.clear();

      if let ServerConfig::Clustered { ref hosts, .. } = config.server {
        for (host, port) in hosts.iter() {
          known_nodes.insert((host.to_owned(), *port));
        }
      }
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
        cache.clear();

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

  /// Send a command to the server(s).
  pub async fn write_command(&mut self, command: RedisCommand) -> Result<Backpressure, RedisError> {
    unimplemented!()
  }

  /// Send a command to all servers in a cluster.
  pub async fn write_all_cluster(&mut self, command: RedisCommand) -> Result<(), RedisError> {
    // TODO should this revert to `write_command` when the server is not clustered?
    unimplemented!()
  }
}

// TODO
// how to handle errors that require a reconnect in a cluster (_Sync or _Reconnect):
// change to Moved((slot, server, Command))
// need to check for unknown port format in error message (:6379) and re-use the same endpoint but with that port
// when a moved error is received check if the slot maps to the server, and if not run sync()
// otherwise run the command again without waiting on sync
// ASK:
//   store it as Ask((slot, server, Command))
//   do not sync the cluster, but check if the connection exists
//   if not then connect and add it to the connection map
//   send the ASKING command
//   replay the command

// TODO
// in a transaction if an ASKING error is received instead of QUEUED
// then change the hash slot on the transaction, send ASKING, and send all the commands
// this implies either no pipelining on transactions, or extra state in transaction response handlers

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

  /// Enqueue a command to run later.
  ///
  /// The queue is drained and replayed whenever a `RedisCommandKind::_Sync` or `RedisCommandKind::_Reconnect` message is received.
  pub fn enqueue(&mut self, cmd: RedisCommand) {
    self.buffer.push_back(cmd);
  }

  /// Read the cluster state, if known.
  pub fn cluster_state(&self) -> Option<&ClusterRouting> {
    if let Connections::Clustered { ref cache, .. } = self.connections {
      Some(cache)
    } else {
      None
    }
  }

  /// Send a command to the server.
  pub async fn write_command(&mut self, mut command: RedisCommand) -> Result<Backpressure, RedisError> {
    if let Err(e) = command.incr_check_attempted(self.inner.max_command_attempts()) {
      if let Some(tx) = command.take_responder() {
        let _ = tx.send(Err(e));
      }
      debug!(
        "{}: Skipping command `{}` after too many failed attempts.",
        self.inner.id,
        command.kind.to_str_debug()
      );
      return Ok(Backpressure::Skipped);
    }
    if command.attempted > 0 {
      self.inner.counters.incr_redelivery_count();
    }

    if command.kind.is_all_cluster_nodes() {
      self
        .connections
        .write_all_cluster(command)
        .await
        .map(Backpressure::Skipped)
    } else {
      self.connections.write_command(command).await
    }
  }

  /// Disconnect from the server(s), moving all in-flight messages to the internal command buffer.
  pub async fn disconnect(&mut self) {
    let commands = self.connections.disconnect_all().await;
    self.buffer.extend(commands);
    client_utils::set_bool_atomic(&self.all_connected, false);
    client_utils::set_locked(&self.inner.state, ClientState::Disconnected);
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
    client_utils::set_client_state(&self.inner.state, ClientState::Connecting);
    utils::sync_cluster(&self.inner, &self.connections).await?;
    client_utils::set_client_state(&self.inner.state, ClientState::Connected);
    Ok(())
  }

  /// Replay all queued commands.
  pub async fn replay_buffer(&mut self) -> Result<(), RedisError> {
    for mut command in self.buffer.drain(..) {
      command.skip_backpressure = true;
      match self.write_command(command).await {
        Ok(Backpressure::Queue(command)) => {
          self.buffer.push_back(command);
          break;
        },
        Err(e) => {
          warn!("{}: Error replaying command: {:?}", self.inner.id, e);
          self.disconnect().await;
          break;
        },
        _ => unimplemented!(),
      }
    }

    Ok(())
  }
}
