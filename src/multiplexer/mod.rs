use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, MultiplexerReceiver, RedisCommand, RedisCommandKind},
    connection::{self, CommandBuffer, Counters, RedisTransport, RedisWriter},
    responders::ResponseKind,
    types::ClusterRouting,
    utils::{parse_cluster_error, server_to_parts},
  },
  types::{ClientState, RedisConfig, ServerConfig},
  utils as client_utils,
};
use arc_swap::ArcSwap;
use arcstr::ArcStr;
use futures::future::{join_all, try_join_all};
use parking_lot::{Mutex, RwLock};
use std::{
  collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
  future::Future,
  sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
  },
  time::{Duration, Instant},
};
use tokio::{
  io::{AsyncRead, AsyncWrite},
  sync::{
    broadcast::Sender as BroadcastSender,
    oneshot::{channel as oneshot_channel, Sender as OneshotSender},
    RwLock as AsyncRwLock,
  },
};

pub mod centralized;
pub mod clustered;
pub mod commands;
pub mod reader;
pub mod responses;
pub mod sentinel;
pub mod transactions;
pub mod types;
pub mod utils;

/// The result of an attempt to send a command to the server.
pub enum Written<'a> {
  /// Apply backpressure to the command before retrying.
  Backpressure((RedisCommand, Backpressure)),
  /// Indicates that the command was sent to the associated server and whether the socket was flushed.
  Sent((&'a ArcStr, bool)),
  /// Disconnect from the provided server and retry the command later.
  Disconnect((&'a ArcStr, Option<RedisCommand>, RedisError)),
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

impl Backpressure {
  /// Apply the backpressure policy.
  pub async fn run(
    self,
    inner: &Arc<RedisClientInner>,
    command: &mut RedisCommand,
  ) -> Result<Option<MultiplexerReceiver>, RedisError> {
    match self {
      Backpressure::Error(e) => Err(e),
      Backpressure::Wait(duration) => {
        let _ = inner.wait_with_interrupt(duration).await?;
        Ok(None)
      },
      Backpressure::Block => {
        command.skip_backpressure = true;
        Ok(Some(command.create_multiplexer_channel()))
      },
    }
  }
}

pub enum Connections {
  Centralized {
    /// The connection to the server.
    writer: Option<RedisWriter>,
  },
  Clustered {
    /// The cached cluster routing table used for mapping keys to server IDs.
    cache:   ClusterRouting,
    /// A map of server IDs and connections.
    writers: HashMap<ArcStr, RedisWriter>,
  },
  Sentinel {
    /// The connection to the primary server.
    writer: Option<RedisWriter>,
  },
}

impl Connections {
  pub fn new_centralized() -> Self {
    Connections::Centralized { writer: None }
  }

  pub fn new_sentinel() -> Self {
    Connections::Sentinel { writer: None }
  }

  pub fn new_clustered() -> Self {
    Connections::Clustered {
      cache:   ClusterRouting::new(),
      writers: HashMap::new(),
    }
  }

  /// Whether or not the connection map has a connection to the provided `host:port`.
  pub fn has_server_connection(&self, server: &str) -> bool {
    match self {
      Connections::Centralized { ref writer } => writer
        .as_ref()
        .map(|writer| utils::compare_servers(&writer.server, server, &writer.default_host))
        .unwrap_or(false),
      Connections::Sentinel { ref writer } => writer
        .as_ref()
        .map(|writer| utils::compare_servers(&writer.server, server, &writer.default_host))
        .unwrap_or(false),
      Connections::Clustered { ref writers, .. } => {
        for (_, writer) in writers.iter() {
          if utils::compare_servers(&writer.server, server, &writer.default_host) {
            return true;
          }
        }

        false
      },
    }
  }

  /// Get the connection writer half for the provided server.
  pub fn get_connection_mut(&mut self, server: &str) -> Option<&mut RedisWriter> {
    match self {
      Connections::Centralized { ref mut writer } => writer.as_mut().and_then(|writer| {
        if utils::compare_servers(&writer.server, server, &writer.default_host) {
          Some(writer)
        } else {
          None
        }
      }),
      Connections::Sentinel { ref mut writer } => writer.as_mut().and_then(|writer| {
        if utils::compare_servers(&writer.server, server, &writer.default_host) {
          Some(writer)
        } else {
          None
        }
      }),
      Connections::Clustered { ref mut writers, .. } => writers.iter_mut().find_map(|(_, writer)| {
        if utils::compare_servers(&writer.server, server, &writer.default_host) {
          Some(writer)
        } else {
          None
        }
      }),
    }
  }

  /// Initialize the underlying connection(s) and update the cached backchannel information.
  pub async fn initialize(
    &mut self,
    inner: &Arc<RedisClientInner>,
    buffer: &mut CommandBuffer,
  ) -> Result<(), RedisError> {
    let result = if inner.config.server.is_clustered() {
      clustered::initialize_connections(inner, self, buffer).await
    } else if inner.config.server.is_centralized() {
      centralized::initialize_connection(inner, self, buffer).await
    } else if inner.config.server.is_sentinel() {
      sentinel::initialize_connection(inner, self, buffer).await
    } else {
      return Err(RedisError::new(RedisErrorKind::Config, "Invalid client configuration."));
    };

    if result.is_ok() {
      let mut backchannel = inner.backchannel.write().await;
      backchannel.connection_ids = self.connection_ids();
    }
    result
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
  pub async fn disconnect(&mut self, inner: &Arc<RedisClientInner>, server: Option<&ArcStr>) -> CommandBuffer {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
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
            _debug!(inner, "Disconnecting from {}", writer.server);
            let commands = writer.graceful_close().await;
            out.extend(commands.into_iter());
          }
        }
        out
      },
      Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.graceful_close().await
        } else {
          VecDeque::new()
        }
      },
    }
  }

  /// Disconnect and clear local state for all connections, returning all in-flight commands.
  pub async fn disconnect_all(&mut self, inner: &Arc<RedisClientInner>) -> CommandBuffer {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
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
          _debug!(inner, "Disconnecting from {}", writer.server);
          let commands = writer.graceful_close().await;
          out.extend(commands.into_iter());
        }
        out
      },
      Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
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
  pub async fn check_and_flush(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    _trace!(inner, "Checking and flushing sockets...");

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
        try_join_all(writers.values_mut().map(|writer| writer.check_and_flush()))
          .await
          .map(|_| ())
      },
    }
  }

  /// Send a command to the server(s).
  pub async fn write_command(
    &mut self,
    inner: &Arc<RedisClientInner>,
    command: RedisCommand,
  ) -> Result<Written<'_>, (RedisError, RedisCommand)> {
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
  pub async fn write_all_cluster(
    &mut self,
    inner: &Arc<RedisClientInner>,
    command: RedisCommand,
  ) -> Result<Written<'_>, RedisError> {
    if let Connections::Clustered { ref mut writers, .. } = self {
      let _ = clustered::send_all_cluster_command(inner, writers, command).await?;
      Ok(Written::Ignore)
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected clustered configuration.",
      ))
    }
  }

  /// Check if the provided `server` node owns the provided `slot`.
  pub fn check_cluster_owner(&self, slot: u16, server: &str) -> bool {
    match self {
      Connections::Clustered { ref cache, .. } => cache
        .get_server(slot)
        .map(|owner| {
          trace!("Comparing cached cluster owner for {}: {} == {}", slot, owner, server);
          owner.as_str() == server
        })
        .unwrap_or(false),
      _ => false,
    }
  }

  /// Connect or reconnect to the provided `host:port`.
  pub async fn add_connection(&mut self, inner: &Arc<RedisClientInner>, server: &str) -> Result<(), RedisError> {
    if let Connections::Clustered { ref mut writers, .. } = self {
      let (host, port) = server_to_parts(server)?;
      let mut transport = connection::create(inner, host.to_owned(), port, None).await?;
      let _ = transport.setup(inner).await?;

      let (server, writer) = connection::split_and_initialize(inner, transport, clustered::spawn_reader_task)?;
      writers.insert(server, writer);
      Ok(())
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected clustered configuration.",
      ))
    }
  }
}

/// A struct for routing commands to the server(s).
pub struct Multiplexer {
  pub connections: Connections,
  pub inner:       Arc<RedisClientInner>,
  pub buffer:      CommandBuffer,
}

impl Multiplexer {
  /// Create a new `Multiplexer` without connecting to the server(s).
  pub fn new(inner: &Arc<RedisClientInner>) -> Self {
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

  /// Whether the multiplexer has buffered commands that need to be retried.
  pub fn has_buffered_commands(&self) -> bool {
    self.buffer.len() > 0
  }

  /// Read the connection identifier for the provided command.
  pub fn find_connection(&self, command: &RedisCommand) -> Option<&str> {
    match self.connections {
      Connections::Centralized { ref writer } => writer.as_ref().map(|w| w.server.as_str()),
      Connections::Sentinel { ref writer } => writer.as_ref().map(|w| w.server.as_str()),
      Connections::Clustered { ref writers, ref cache } => command
        .cluster_hash()
        .and_then(|slot| cache.get_server(slot))
        .map(|s| s.as_str()),
    }
  }

  /// Route and write the command to the server(s).
  ///
  /// If the command cannot be written:
  /// * The command will be queued to run later.
  /// * The associated connection will be dropped.
  /// * The reader task for that connection will close, sending a `Reconnect` message to the multiplexer.
  ///
  /// Errors are handled internally, but may be returned if the command was queued to run later.
  pub async fn write_command(&mut self, mut command: RedisCommand) -> Result<Written<'_>, RedisError> {
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
      self.connections.write_all_cluster(&self.inner, command).await
    } else {
      match self.connections.write_command(&self.inner, command).await {
        Ok(result) => Ok(result),
        Err((error, command)) => {
          self.buffer.push_back(command);
          Err(error)
        },
      }
    }
  }

  /// Attempt to write the command to a specific server without backpressure, returning the error and command on
  /// failure.
  ///
  /// The associated connection will be dropped if needed. The caller is responsible for returning errors.
  pub async fn write_direct(
    &mut self,
    mut command: RedisCommand,
    server: &str,
  ) -> Result<(), (RedisError, RedisCommand)> {
    debug!(
      "{}: Direct write `{}` command to {}",
      self.inner.id,
      command.kind.to_str_debug(),
      server
    );

    let mut writer = match self.connections.get_connection_mut(server) {
      Some(writer) => writer,
      None => {
        let err = RedisError::new(
          RedisErrorKind::Unknown,
          format!("Failed to find connection for {}", server),
        );
        return Err((err, command));
      },
    };

    let frame = match utils::prepare_command(&self.inner, &writer.counters, &mut command) {
      Ok((frame, _)) => frame,
      Err(e) => {
        warn!(
          "{}: Frame encoding error for {}",
          self.inner.id,
          command.kind.to_str_debug()
        );
        // do not retry commands that trigger frame encoding errors
        command.respond_to_caller(Err(e));
        return Ok(());
      },
    };

    // always flush the socket in this case
    if let Err(e) = writer.write_frame(frame, true).await {
      debug!(
        "{}: Error sending command {}: {:?}",
        self.inner.id,
        command.kind.to_str_debug(),
        e
      );
      Err((e, command))
    } else {
      if command.blocks_connection() {
        self.inner.backchannel.write().await.set_blocked(&writer.server);
      }

      trace!(
        "{}: Successfully sent command {}",
        self.inner.id,
        command.kind.to_str_debug()
      );
      writer.push_command(command);
      Ok(())
    }
  }

  /// Write the command once without checking for backpressure, returning any connection errors and queueing the
  /// command to run later if needed.
  ///
  /// The associated connection will be dropped if needed.
  pub async fn write_once(&mut self, command: RedisCommand, server: &str) -> Result<(), RedisError> {
    debug!(
      "{}: Writing `{}` command once to {}",
      self.inner.id,
      command.kind.to_str_debug(),
      server
    );

    let mut writer = match self.connections.get_connection_mut(server) {
      Some(writer) => writer,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::Unknown,
          format!("Failed to find connection for {}", server),
        ))
      },
    };
    let is_blocking = command.blocks_connection();

    match utils::write_command(&self.inner, writer, command, true).await {
      Written::Disconnect((server, command, error)) => {
        // TODO need to rework this to avoid borrowck errors
        let buffer = self.connections.disconnect(&self.inner, Some(server)).await;
        self.buffer.extend(buffer.into_iter());

        if let Some(command) = command {
          debug!(
            "{}: Dropping command after write failure in write_once: {}",
            self.inner.id,
            command.kind.to_str_debug()
          );
        }
        // the connection error is sent to the caller in `write_command`
        Err(error)
      },
      Written::Sent((server, flushed)) => {
        trace!("{}: Sent command to {} (flushed: {})", self.inner.id, server, flushed);
        if is_blocking {
          self.inner.backchannel.write().await.set_blocked(&server);
        }
        if !flushed {
          let _ = self.check_and_flush().await?;
        }

        Ok(())
      },
      Written::Ignore => Err(RedisError::new(RedisErrorKind::Unknown, "Could not send command.")),
      Written::Backpressure(_) => Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Unexpected backpressure flag.",
      )),
    }
  }

  /// Disconnect from all the servers, moving the in-flight messages to the internal command buffer and triggering a
  /// reconnection, if necessary.
  pub async fn disconnect_all(&mut self) {
    let commands = self.connections.disconnect_all(&self.inner).await;
    self.buffer.extend(commands);
  }

  /// Connect to the server(s), discarding any previous connection state.
  pub async fn connect(&mut self) -> Result<(), RedisError> {
    self.connections.initialize(&self.inner, &mut self.buffer).await
  }

  /// Sync the cached cluster state with the server via `CLUSTER SLOTS`.
  ///
  /// This will also create new connections or drop old connections as needed.
  pub async fn sync_cluster(&mut self) -> Result<(), RedisError> {
    clustered::sync(&self.inner, &mut self.connections, &mut self.buffer).await?;
    self.retry_buffer().await;
    Ok(())
  }

  /// Attempt to replay all queued commands on the internal buffer without backpressure.
  ///
  /// If a command cannot be written the underlying connections will close and the unsent commands will remain on the
  /// internal buffer.
  pub async fn retry_buffer(&mut self) {
    let mut commands: Vec<RedisCommand> = self.buffer.drain(..).collect();
    let mut failed_command = None;

    for mut command in commands.drain(..) {
      command.skip_backpressure = true;

      match self.write_command(command).await {
        Ok(Written::Disconnect((server, command, error))) => {
          if let Some(command) = command {
            failed_command = Some(command);
          }

          warn!("{}: Disconnect while replaying command: {:?}", self.inner.id, error);
          self.disconnect_all().await; // triggers a reconnect if needed
          break;
        },
        Err(error) => {
          warn!("{}: Error replaying command: {:?}", self.inner.id, error);
          self.disconnect_all().await; // triggers a reconnect if needed
          break;
        },
        _ => {
          continue;
        },
      }
    }

    if let Some(command) = failed_command {
      self.buffer.push_back(command);
    }
    self.buffer.extend(commands.into_iter());
  }

  /// Check each connection for pending frames that have not been flushed, and flush the connection if needed.
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    self.connections.check_and_flush(&self.inner).await
  }

  /// Returns whether or not the provided `server` owns the provided `slot`.
  pub fn cluster_node_owns_slot(&self, slot: u16, server: &str) -> bool {
    match self.connections {
      Connections::Clustered { ref cache, .. } => cache
        .get_server(slot)
        .map(|node| node.as_str() == server)
        .unwrap_or(false),
      _ => false,
    }
  }

  /// Modify connection state according to the cluster redirection error.
  ///
  /// * Synchronizes the cached cluster state in response to MOVED
  /// * Connects and sends ASKING to the provided server in response to ASKED
  pub async fn cluster_redirection(
    &mut self,
    kind: ClusterErrorKind,
    slot: u16,
    server: &str,
  ) -> Result<(), RedisError> {
    debug!(
      "{}: Handling cluster redirect {:?} {} {}",
      &self.inner.id, kind, slot, server
    );

    if kind == ClusterErrorKind::Moved {
      let should_sync = self
        .inner
        .with_cluster_state(|state| Ok(state.get_server(slot).map(|owner| server == owner).unwrap_or(true)))
        .unwrap_or(true);

      if should_sync {
        let _ = self.sync_cluster().await?;
      }
    } else if kind == ClusterErrorKind::Ask {
      if !self.connections.has_server_connection(server) {
        let _ = self.connections.add_connection(&self.inner, server).await?;
        self
          .inner
          .backchannel
          .write()
          .await
          .update_connection_ids(&self.connections);
      }

      // can't use request_response since there may be pipelined commands ahead of this
      let (tx, rx) = oneshot_channel();
      let mut command = RedisCommand::new_asking(slot);
      command.response = ResponseKind::Respond(Some(tx));

      let _ = self.write_once(command, &server).await?;
      let _ = rx.await??;
    }

    Ok(())
  }
}
