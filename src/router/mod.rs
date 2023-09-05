use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RouterReceiver},
    connection::{self, CommandBuffer, Counters, RedisWriter},
    responders::ResponseKind,
    types::{ClusterRouting, Server},
  },
  trace,
  utils as client_utils,
};
use futures::future::try_join_all;
use semver::Version;
use std::{
  collections::{HashMap, VecDeque},
  fmt,
  fmt::Formatter,
  sync::Arc,
  time::Duration,
};
use tokio::sync::oneshot::channel as oneshot_channel;

pub mod centralized;
pub mod clustered;
pub mod commands;
pub mod reader;
pub mod replicas;
pub mod responses;
pub mod sentinel;
pub mod transactions;
pub mod types;
pub mod utils;

#[cfg(feature = "replicas")]
use crate::router::replicas::Replicas;

/// The result of an attempt to send a command to the server.
pub enum Written {
  /// Apply backpressure to the command before retrying.
  Backpressure((RedisCommand, Backpressure)),
  /// Indicates that the command was sent to the associated server and whether the socket was flushed.
  Sent((Server, bool)),
  /// Indicates that the command was sent to all servers.
  SentAll,
  /// Disconnect from the provided server and retry the command later.
  Disconnect((Option<Server>, Option<RedisCommand>, RedisError)),
  /// Indicates that the result should be ignored since the command will not be retried.
  Ignore,
  /// (Cluster only) Synchronize the cached cluster routing table and retry.
  Sync(RedisCommand),
}

impl fmt::Display for Written {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", match self {
      Written::Backpressure(_) => "Backpressure",
      Written::Sent(_) => "Sent",
      Written::SentAll => "SentAll",
      Written::Disconnect(_) => "Disconnect",
      Written::Ignore => "Ignore",
      Written::Sync(_) => "Sync",
    })
  }
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
  pub async fn wait(
    self,
    inner: &Arc<RedisClientInner>,
    command: &mut RedisCommand,
  ) -> Result<Option<RouterReceiver>, RedisError> {
    match self {
      Backpressure::Error(e) => Err(e),
      Backpressure::Wait(duration) => {
        _debug!(inner, "Backpressure policy (wait): {}ms", duration.as_millis());
        trace::backpressure_event(&command, Some(duration.as_millis()));
        let _ = inner.wait_with_interrupt(duration).await?;
        Ok(None)
      },
      Backpressure::Block => {
        _debug!(inner, "Backpressure (block)");
        trace::backpressure_event(&command, None);
        if !command.has_router_channel() {
          _trace!(
            inner,
            "Blocking router for backpressure for {}",
            command.kind.to_str_debug()
          );
          command.skip_backpressure = true;
          Ok(Some(command.create_router_channel()))
        } else {
          Ok(None)
        }
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
    writers: HashMap<Server, RedisWriter>,
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

  /// Discover and return a mapping of replica nodes to their associated primary node.
  #[cfg(feature = "replicas")]
  pub async fn replica_map(&mut self, inner: &Arc<RedisClientInner>) -> Result<HashMap<Server, Server>, RedisError> {
    Ok(match self {
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer {
          writer
            .discover_replicas(inner)
            .await?
            .into_iter()
            .map(|replica| (replica, writer.server.clone()))
            .collect()
        } else {
          HashMap::new()
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        let mut out = HashMap::with_capacity(writers.len());

        for (primary, writer) in writers.iter_mut() {
          let replicas = inner
            .with_cluster_state(|state| Ok(state.replicas(primary)))
            .ok()
            .unwrap_or(Vec::new());

          if replicas.is_empty() {
            for replica in writer.discover_replicas(inner).await? {
              out.insert(replica, primary.clone());
            }
          } else {
            for replica in replicas.into_iter() {
              out.insert(replica, primary.clone());
            }
          }
        }
        out
      },
    })
  }

  /// Whether or not the connection map has a connection to the provided server`.
  ///
  /// The connection is tested by calling `flush`.
  pub fn has_server_connection(&mut self, server: &Server) -> bool {
    match self {
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.as_mut() {
          if writer.server == *server {
            writer.is_working()
          } else {
            false
          }
        } else {
          false
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        for (_, writer) in writers.iter_mut() {
          if writer.server == *server {
            return writer.is_working();
          }
        }

        false
      },
    }
  }

  /// Get the connection writer half for the provided server.
  pub fn get_connection_mut(&mut self, server: &Server) -> Option<&mut RedisWriter> {
    match self {
      Connections::Centralized { ref mut writer } => {
        writer
          .as_mut()
          .and_then(|writer| if writer.server == *server { Some(writer) } else { None })
      },
      Connections::Sentinel { ref mut writer } => {
        writer
          .as_mut()
          .and_then(|writer| if writer.server == *server { Some(writer) } else { None })
      },
      Connections::Clustered { ref mut writers, .. } => {
        writers
          .iter_mut()
          .find_map(|(_, writer)| if writer.server == *server { Some(writer) } else { None })
      },
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
      if let Some(version) = self.server_version() {
        inner.server_state.write().kind.set_server_version(version);
      }

      let mut backchannel = inner.backchannel.write().await;
      backchannel.connection_ids = self.connection_ids();
    }
    result
  }

  /// Read the counters associated with a connection to a server.
  pub fn counters(&self, server: Option<&Server>) -> Option<&Counters> {
    match self {
      Connections::Centralized { ref writer } => writer.as_ref().map(|w| &w.counters),
      Connections::Sentinel { ref writer, .. } => writer.as_ref().map(|w| &w.counters),
      Connections::Clustered { ref writers, .. } => {
        server.and_then(|server| writers.get(server).map(|w| &w.counters))
      },
    }
  }

  /// Read the server version, if known.
  pub fn server_version(&self) -> Option<Version> {
    match self {
      Connections::Centralized { ref writer } => writer.as_ref().and_then(|w| w.version.clone()),
      Connections::Clustered { ref writers, .. } => writers.iter().find_map(|(_, w)| w.version.clone()),
      Connections::Sentinel { ref writer, .. } => writer.as_ref().and_then(|w| w.version.clone()),
    }
  }

  /// Disconnect from the provided server, using the default centralized connection if `None` is provided.
  pub async fn disconnect(&mut self, inner: &Arc<RedisClientInner>, server: Option<&Server>) -> CommandBuffer {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.graceful_close().await
        } else {
          VecDeque::new()
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        let mut out = VecDeque::new();

        if let Some(server) = server {
          if let Some(writer) = writers.remove(server) {
            _debug!(inner, "Disconnecting from {}", writer.server);
            let commands = writer.graceful_close().await;
            out.extend(commands);
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
      Connections::Clustered { ref mut writers, .. } => {
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
  pub fn connection_ids(&self) -> HashMap<Server, i64> {
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
          writer.flush().await
        } else {
          Ok(())
        }
      },
      Connections::Sentinel { ref mut writer, .. } => {
        if let Some(writer) = writer {
          writer.flush().await
        } else {
          Ok(())
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        try_join_all(writers.values_mut().map(|writer| writer.flush()))
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
    force_flush: bool,
  ) -> Result<Written, (RedisError, RedisCommand)> {
    match self {
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
      } => clustered::send_command(inner, writers, cache, command, force_flush).await,
      Connections::Centralized { ref mut writer } => {
        centralized::send_command(inner, writer, command, force_flush).await
      },
      Connections::Sentinel { ref mut writer, .. } => {
        centralized::send_command(inner, writer, command, force_flush).await
      },
    }
  }

  /// Send a command to all servers in a cluster.
  pub async fn write_all_cluster(
    &mut self,
    inner: &Arc<RedisClientInner>,
    command: RedisCommand,
  ) -> Result<Written, RedisError> {
    if let Connections::Clustered { ref mut writers, .. } = self {
      let _ = clustered::send_all_cluster_command(inner, writers, command).await?;
      Ok(Written::SentAll)
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected clustered configuration.",
      ))
    }
  }

  /// Check if the provided `server` node owns the provided `slot`.
  pub fn check_cluster_owner(&self, slot: u16, server: &Server) -> bool {
    match self {
      Connections::Clustered { ref cache, .. } => cache
        .get_server(slot)
        .map(|owner| {
          trace!("Comparing cached cluster owner for {}: {} == {}", slot, owner, server);
          owner == server
        })
        .unwrap_or(false),
      _ => false,
    }
  }

  /// Connect or reconnect to the provided `host:port`.
  pub async fn add_connection(&mut self, inner: &Arc<RedisClientInner>, server: &Server) -> Result<(), RedisError> {
    if let Connections::Clustered { ref mut writers, .. } = self {
      let mut transport = connection::create(inner, server, None).await?;
      let _ = transport.setup(inner, None).await?;

      let (server, writer) = connection::split_and_initialize(inner, transport, false, clustered::spawn_reader_task)?;
      writers.insert(server, writer);
      Ok(())
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected clustered configuration.",
      ))
    }
  }

  /// Read the list of active/working connections.
  pub fn active_connections(&self) -> Vec<Server> {
    match self {
      Connections::Clustered { ref writers, .. } => writers
        .iter()
        .filter_map(|(server, writer)| {
          if writer.is_working() {
            Some(server.clone())
          } else {
            None
          }
        })
        .collect(),
      Connections::Centralized { ref writer } | Connections::Sentinel { ref writer, .. } => writer
        .as_ref()
        .and_then(|writer| {
          if writer.is_working() {
            Some(vec![writer.server.clone()])
          } else {
            None
          }
        })
        .unwrap_or(Vec::new()),
    }
  }
}

/// A struct for routing commands to the server(s).
pub struct Router {
  pub connections: Connections,
  pub inner:       Arc<RedisClientInner>,
  pub buffer:      CommandBuffer,
  #[cfg(feature = "replicas")]
  pub replicas:    Replicas,
}

impl Router {
  /// Create a new `Router` without connecting to the server(s).
  pub fn new(inner: &Arc<RedisClientInner>) -> Self {
    let connections = if inner.config.server.is_clustered() {
      Connections::new_clustered()
    } else if inner.config.server.is_sentinel() {
      Connections::new_sentinel()
    } else {
      Connections::new_centralized()
    };

    Router {
      buffer: VecDeque::new(),
      inner: inner.clone(),
      connections,
      #[cfg(feature = "replicas")]
      replicas: Replicas::new(),
    }
  }

  #[cfg(feature = "check-unresponsive")]
  pub fn sync_network_timeout_state(&self) {
    self.inner.network_timeouts.state().sync(&self.inner, &self.connections);
  }

  #[cfg(not(feature = "check-unresponsive"))]
  pub fn sync_network_timeout_state(&self) {}

  /// Read the connection identifier for the provided command.
  pub fn find_connection(&self, command: &RedisCommand) -> Option<&Server> {
    match self.connections {
      Connections::Centralized { ref writer } => writer.as_ref().map(|w| &w.server),
      Connections::Sentinel { ref writer } => writer.as_ref().map(|w| &w.server),
      Connections::Clustered { ref cache, .. } => command.cluster_hash().and_then(|slot| cache.get_server(slot)),
    }
  }

  /// Route and write the command to the server(s).
  ///
  /// If the command cannot be written:
  /// * The command will be queued to run later.
  /// * The associated connection will be dropped.
  /// * The reader task for that connection will close, sending a `Reconnect` message to the router.
  ///
  /// Errors are handled internally, but may be returned if the command was queued to run later.
  pub async fn write_command(&mut self, mut command: RedisCommand, force_flush: bool) -> Result<Written, RedisError> {
    let send_all_cluster_nodes = command.kind.is_all_cluster_nodes()
      || (command.kind.closes_connection() && self.inner.config.server.is_clustered());

    if send_all_cluster_nodes {
      self.connections.write_all_cluster(&self.inner, command).await
    } else {
      match self.connections.write_command(&self.inner, command, force_flush).await {
        Ok(result) => Ok(result),
        Err((error, command)) => {
          self.buffer_command(command);
          Err(error)
        },
      }
    }
  }

  /// Write a command to a replica node if possible, falling back to a primary node if configured.
  #[cfg(feature = "replicas")]
  pub async fn write_replica_command(
    &mut self,
    mut command: RedisCommand,
    force_flush: bool,
  ) -> Result<Written, RedisError> {
    if !command.use_replica {
      return self.write_command(command, force_flush).await;
    }

    let primary = match self.find_connection(&command) {
      Some(server) => server.clone(),
      None => {
        if self.inner.connection.replica.primary_fallback {
          debug!(
            "{}: Fallback to primary node connection for {} ({})",
            self.inner.id,
            command.kind.to_str_debug(),
            command.debug_id()
          );

          command.use_replica = false;
          return self.write_command(command, force_flush).await;
        } else {
          command.respond_to_caller(Err(RedisError::new(
            RedisErrorKind::Replica,
            "Missing primary node connection.",
          )));
          return Ok(Written::Ignore);
        }
      },
    };

    let result = self
      .replicas
      .write_command(&self.inner, &primary, command, force_flush)
      .await;
    match result {
      Ok(result) => {
        if let Err(e) = self.replicas.check_and_flush().await {
          error!("{}: Error flushing replica connections: {:?}", self.inner.id, e);
        }

        Ok(result)
      },
      Err((error, mut command)) => {
        if self.inner.connection.replica.primary_fallback {
          debug!(
            "{}: Fall back to primary node for {} ({}) after replica error: {:?}",
            self.inner.id,
            command.kind.to_str_debug(),
            command.debug_id(),
            error
          );

          command.use_replica = false;
          return self.write_command(command, force_flush).await;
        } else {
          trace!(
            "{}: Add {} ({}) to replica retry buffer.",
            self.inner.id,
            command.kind.to_str_debug(),
            command.debug_id()
          );
          self.replicas.add_to_retry_buffer(command);
        }
        Err(error)
      },
    }
  }

  /// Write a command to a replica node if possible, falling back to a primary node if configured.
  #[cfg(not(feature = "replicas"))]
  pub async fn write_replica_command(
    &mut self,
    command: RedisCommand,
    force_flush: bool,
  ) -> Result<Written, RedisError> {
    self.write_command(command, force_flush).await
  }

  /// Attempt to write the command to a specific server without backpressure, returning the error and command on
  /// failure.
  ///
  /// The associated connection will be dropped if needed. The caller is responsible for returning errors.
  pub async fn write_direct(
    &mut self,
    mut command: RedisCommand,
    server: &Server,
  ) -> Result<(), (RedisError, RedisCommand)> {
    debug!(
      "{}: Direct write `{}` command to {}, ID: {}",
      self.inner.id,
      command.kind.to_str_debug(),
      server,
      command.debug_id()
    );

    let writer = match self.connections.get_connection_mut(server) {
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

    let blocks_connection = command.blocks_connection();

    // always flush the socket in this case
    writer.push_command(&self.inner, command);
    if let Err(e) = writer.write_frame(frame, true).await {
      let command = match writer.pop_recent_command() {
        Some(cmd) => cmd,
        None => {
          error!(
            "{}: Failed to take recent command off queue after write failure.",
            self.inner.id
          );
          return Ok(());
        },
      };

      debug!(
        "{}: Error sending command {}: {:?}",
        self.inner.id,
        command.kind.to_str_debug(),
        e
      );
      Err((e, command))
    } else {
      if blocks_connection {
        self.inner.backchannel.write().await.set_blocked(&writer.server);
      }
      Ok(())
    }
  }

  /// Write the command once without checking for backpressure, returning any connection errors and queueing the
  /// command to run later if needed.
  ///
  /// The associated connection will be dropped if needed.
  pub async fn write_once(&mut self, command: RedisCommand, server: &Server) -> Result<(), RedisError> {
    let inner = self.inner.clone();
    _debug!(
      inner,
      "Writing `{}` command once to {}",
      command.kind.to_str_debug(),
      server
    );

    let is_blocking = command.blocks_connection();
    let write_result = {
      let writer = match self.connections.get_connection_mut(server) {
        Some(writer) => writer,
        None => {
          return Err(RedisError::new(
            RedisErrorKind::Unknown,
            format!("Failed to find connection for {}", server),
          ))
        },
      };

      utils::write_command(&inner, writer, command, true).await
    };

    match write_result {
      Written::Disconnect((server, command, error)) => {
        let buffer = self.connections.disconnect(&inner, server.as_ref()).await;
        self.buffer_commands(buffer);
        self.sync_network_timeout_state();

        if let Some(command) = command {
          _debug!(
            inner,
            "Dropping command after write failure in write_once: {}",
            command.kind.to_str_debug()
          );
        }
        // the connection error is sent to the caller in `write_command`
        Err(error)
      },
      Written::Sync(command) => {
        _debug!(inner, "Missing hash slot. Disconnecting and syncing cluster.");
        let buffer = self.connections.disconnect_all(&inner).await;
        self.buffer_commands(buffer);
        self.buffer_command(command);
        self.sync_network_timeout_state();

        Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Invalid or missing hash slot.",
        ))
      },
      Written::SentAll => {
        let _ = self.check_and_flush().await?;
        Ok(())
      },
      Written::Sent((server, flushed)) => {
        trace!("{}: Sent command to {} (flushed: {})", self.inner.id, server, flushed);
        if is_blocking {
          inner.backchannel.write().await.set_blocked(&server);
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
    self.buffer_commands(commands);
    self.sync_network_timeout_state();
    self.disconnect_replicas().await;
  }

  /// Disconnect from all the servers, moving the in-flight messages to the internal command buffer and triggering a
  /// reconnection, if necessary.
  #[cfg(feature = "replicas")]
  pub async fn disconnect_replicas(&mut self) {
    if let Err(e) = self.replicas.clear_connections(&self.inner).await {
      warn!("{}: Error disconnecting replicas: {:?}", self.inner.id, e);
    }
  }

  #[cfg(not(feature = "replicas"))]
  pub async fn disconnect_replicas(&mut self) {}

  /// Add the provided commands to the retry buffer.
  pub fn buffer_commands(&mut self, commands: VecDeque<RedisCommand>) {
    for command in commands.into_iter() {
      self.buffer_command(command);
    }
  }

  /// Add the provided command to the retry buffer.
  pub fn buffer_command(&mut self, command: RedisCommand) {
    trace!(
      "{}: Adding {} ({}) command to retry buffer.",
      self.inner.id,
      command.kind.to_str_debug(),
      command.debug_id()
    );
    self.buffer.push_back(command);
  }

  /// Clear all the commands in the retry buffer.
  pub fn clear_retry_buffer(&mut self) {
    trace!(
      "{}: Clearing retry buffer with {} commands.",
      self.inner.id,
      self.buffer.len()
    );
    self.buffer.clear();
  }

  /// Connect to the server(s), discarding any previous connection state.
  pub async fn connect(&mut self) -> Result<(), RedisError> {
    self.disconnect_all().await;
    let result = self.connections.initialize(&self.inner, &mut self.buffer).await;
    self.sync_network_timeout_state();

    if result.is_ok() {
      if let Err(e) = self.sync_replicas().await {
        warn!("{}: Error syncing replicas: {:?}", self.inner.id, e);
        if !self.inner.ignore_replica_reconnect_errors() {
          return Err(e);
        }
      }

      Ok(())
    } else {
      result
    }
  }

  /// Sync the cached cluster state with the server via `CLUSTER SLOTS`.
  ///
  /// This will also create new connections or drop old connections as needed.
  pub async fn sync_cluster(&mut self) -> Result<(), RedisError> {
    let result = clustered::sync(&self.inner, &mut self.connections, &mut self.buffer).await;
    self.sync_network_timeout_state();

    if result.is_ok() {
      self.retry_buffer().await;

      if let Err(e) = self.sync_replicas().await {
        if !self.inner.ignore_replica_reconnect_errors() {
          return Err(e);
        }
      }
    }

    result
  }

  /// Rebuild the cached replica routing table based on the primary node connections.
  #[cfg(feature = "replicas")]
  pub async fn sync_replicas(&mut self) -> Result<(), RedisError> {
    debug!("{}: Syncing replicas...", self.inner.id);
    let _ = self.replicas.clear_connections(&self.inner).await?;
    let replicas = self.connections.replica_map(&self.inner).await?;

    for (mut replica, primary) in replicas.into_iter() {
      let should_use = if let Some(filter) = self.inner.config.replica.filter.as_ref() {
        filter.filter(&primary, &replica).await
      } else {
        true
      };

      if should_use {
        replicas::map_replica_tls_names(&self.inner, &primary, &mut replica);

        let _ = self
          .replicas
          .add_connection(&self.inner, primary, replica, false)
          .await?;
      }
    }

    self
      .inner
      .server_state
      .write()
      .update_replicas(self.replicas.routing_table());

    self.replicas.retry_buffer(&self.inner);
    Ok(())
  }

  /// Rebuild the cached replica routing table based on the primary node connections.
  #[cfg(not(feature = "replicas"))]
  pub async fn sync_replicas(&mut self) -> Result<(), RedisError> {
    Ok(())
  }

  /// Attempt to replay all queued commands on the internal buffer without backpressure.
  ///
  /// If a command cannot be written the underlying connections will close and the unsent commands will remain on the
  /// internal buffer.
  pub async fn retry_buffer(&mut self) {
    let mut commands: VecDeque<RedisCommand> = self.buffer.drain(..).collect();
    let mut failed_command = None;

    for mut command in commands.drain(..) {
      if client_utils::read_bool_atomic(&command.timed_out) {
        debug!(
          "{}: Ignore retrying timed out command: {}",
          self.inner.id,
          command.kind.to_str_debug()
        );
        continue;
      }

      command.skip_backpressure = true;
      trace!(
        "{}: Retry `{}` ({}) command, attempts left: {}",
        self.inner.id,
        command.kind.to_str_debug(),
        command.debug_id(),
        command.attempts_remaining,
      );
      match self.write_command(command, true).await {
        Ok(Written::Disconnect((server, command, error))) => {
          if let Some(command) = command {
            failed_command = Some(command);
          }

          warn!(
            "{}: Disconnect from {:?} while replaying command: {:?}",
            self.inner.id, server, error
          );
          self.disconnect_all().await; // triggers a reconnect if needed
          break;
        },
        Ok(Written::Sync(command)) => {
          failed_command = Some(command);

          warn!("{}: Disconnect and re-sync cluster state.", self.inner.id);
          self.disconnect_all().await; // triggers a reconnect if needed
          break;
        },
        Err(error) => {
          warn!("{}: Error replaying command: {:?}", self.inner.id, error);
          self.disconnect_all().await; // triggers a reconnect if needed
          break;
        },
        Ok(written) => {
          warn!("{}: Unexpected retry result: {}", self.inner.id, written);
          continue;
        },
      }
    }

    if let Some(command) = failed_command {
      self.buffer_command(command);
    }
    self.buffer_commands(commands);
  }

  /// Check each connection for pending frames that have not been flushed, and flush the connection if needed.
  #[cfg(feature = "replicas")]
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    if let Err(e) = self.replicas.check_and_flush().await {
      warn!("{}: Error flushing replica connections: {:?}", self.inner.id, e);
    }
    self.connections.check_and_flush(&self.inner).await
  }

  #[cfg(not(feature = "replicas"))]
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    self.connections.check_and_flush(&self.inner).await
  }

  /// Returns whether or not the provided `server` owns the provided `slot`.
  pub fn cluster_node_owns_slot(&self, slot: u16, server: &Server) -> bool {
    match self.connections {
      Connections::Clustered { ref cache, .. } => cache.get_server(slot).map(|node| node == server).unwrap_or(false),
      _ => false,
    }
  }

  /// Modify connection state according to the cluster redirection error.
  ///
  /// * Synchronizes the cached cluster state in response to MOVED
  /// * Connects and sends ASKING to the provided server in response to ASKED
  pub async fn cluster_redirection(
    &mut self,
    kind: &ClusterErrorKind,
    slot: u16,
    server: &Server,
  ) -> Result<(), RedisError> {
    debug!(
      "{}: Handling cluster redirect {:?} {} {}",
      &self.inner.id, kind, slot, server
    );

    if *kind == ClusterErrorKind::Moved {
      let should_sync = self
        .inner
        .with_cluster_state(|state| Ok(state.get_server(slot).map(|owner| server == owner).unwrap_or(true)))
        .unwrap_or(true);

      if should_sync {
        let _ = self.sync_cluster().await?;
      }
    } else if *kind == ClusterErrorKind::Ask {
      if !self.connections.has_server_connection(server) {
        let _ = self.connections.add_connection(&self.inner, server).await?;
        self
          .inner
          .backchannel
          .write()
          .await
          .update_connection_ids(&self.connections);
        self.sync_network_timeout_state();
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
