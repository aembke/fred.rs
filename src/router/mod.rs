use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RouterReceiver},
    connection::{self, CommandBuffer, Counters, RedisWriter},
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

#[cfg(feature = "transactions")]
use crate::{protocol::command::ClusterErrorKind, protocol::responders::ResponseKind};
#[cfg(feature = "replicas")]
use std::collections::HashSet;
#[cfg(feature = "transactions")]
use tokio::sync::oneshot::channel as oneshot_channel;

pub mod centralized;
pub mod clustered;
pub mod commands;
pub mod reader;
pub mod replicas;
pub mod responses;
pub mod sentinel;
pub mod types;
pub mod utils;

#[cfg(feature = "transactions")]
pub mod transactions;

#[cfg(feature = "replicas")]
use crate::router::replicas::Replicas;

/// The result of an attempt to send a command to the server.
// This is not an ideal pattern, but it mostly comes from the requirement that the shared buffer interface take
// ownership over the command.
pub enum Written {
  /// Apply backpressure to the command before retrying.
  Backpressure((RedisCommand, Backpressure)),
  /// Indicates that the command was sent to the associated server and whether the socket was flushed.
  Sent((Server, bool)),
  /// Indicates that the command was sent to all servers.
  SentAll,
  /// The command could not be written since the connection is down.  
  Disconnected((Option<Server>, Option<RedisCommand>, RedisError)),
  /// Ignore the result and move on to the next command.
  Ignore,
  /// The command could not be routed to any server.
  NotFound(RedisCommand),
  /// A fatal error that should interrupt the router.
  Error((RedisError, Option<RedisCommand>)),
  /// Restart the write process on a primary node connection.
  #[cfg(feature = "replicas")]
  Fallback(RedisCommand),
}

impl fmt::Display for Written {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", match self {
      Written::Backpressure(_) => "Backpressure",
      Written::Sent(_) => "Sent",
      Written::SentAll => "SentAll",
      Written::Disconnected(_) => "Disconnected",
      Written::Ignore => "Ignore",
      Written::NotFound(_) => "NotFound",
      Written::Error(_) => "Error",
      #[cfg(feature = "replicas")]
      Written::Fallback(_) => "Fallback",
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
        _debug!(inner, "Backpressure policy (wait): {:?}", duration);
        trace::backpressure_event(command, Some(duration.as_millis()));
        inner.wait_with_interrupt(duration).await?;
        Ok(None)
      },
      Backpressure::Block => {
        _debug!(inner, "Backpressure (block)");
        trace::backpressure_event(command, None);
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

/// Connection maps for the supported deployment types.
pub enum Connections {
  Centralized {
    /// The connection to the primary server.
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
      Connections::Clustered { ref mut writers, .. } => writers.get_mut(server),
    }
  }

  /// Initialize the underlying connection(s) and update the cached backchannel information.
  pub async fn initialize(
    &mut self,
    inner: &Arc<RedisClientInner>,
    buffer: &mut VecDeque<RedisCommand>,
  ) -> Result<(), RedisError> {
    let result = if inner.config.server.is_clustered() {
      clustered::initialize_connections(inner, self, buffer).await
    } else if inner.config.server.is_centralized() || inner.config.server.is_unix_socket() {
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
          Vec::new()
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
        out.into_iter().collect()
      },
      Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.graceful_close().await
        } else {
          Vec::new()
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
          Vec::new()
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        let mut out = VecDeque::new();
        for (_, writer) in writers.drain() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          let commands = writer.graceful_close().await;
          out.extend(commands.into_iter());
        }
        out.into_iter().collect()
      },
      Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.graceful_close().await
        } else {
          Vec::new()
        }
      },
    }
  }

  /// Read a map of connection IDs (via `CLIENT ID`) for each inner connections.
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
  pub async fn write(&mut self, inner: &Arc<RedisClientInner>, command: RedisCommand, force_flush: bool) -> Written {
    match self {
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
      } => clustered::write(inner, writers, cache, command, force_flush).await,
      Connections::Centralized { ref mut writer } => centralized::write(inner, writer, command, force_flush).await,
      Connections::Sentinel { ref mut writer, .. } => centralized::write(inner, writer, command, force_flush).await,
    }
  }

  /// Send a command to all servers in a cluster.
  pub async fn write_all_cluster(&mut self, inner: &Arc<RedisClientInner>, command: RedisCommand) -> Written {
    if let Connections::Clustered { ref mut writers, .. } = self {
      if let Err(error) = clustered::send_all_cluster_command(inner, writers, command).await {
        Written::Disconnected((None, None, error))
      } else {
        Written::SentAll
      }
    } else {
      Written::Error((
        RedisError::new(RedisErrorKind::Config, "Expected clustered configuration."),
        None,
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
      transport.setup(inner, None).await?;

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
  /// The connection map for each deployment type.
  pub connections: Connections,
  /// The inner client state associated with the router.
  pub inner:       Arc<RedisClientInner>,
  /// Storage for commands that should be deferred or retried later.
  pub buffer:      VecDeque<RedisCommand>,
  /// The replica routing interface.
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

  /// Read the server that should receive the provided command.
  #[cfg(any(feature = "transactions", feature = "replicas"))]
  pub fn find_connection(&self, command: &RedisCommand) -> Option<&Server> {
    match self.connections {
      Connections::Centralized { ref writer } => writer.as_ref().map(|w| &w.server),
      Connections::Sentinel { ref writer } => writer.as_ref().map(|w| &w.server),
      Connections::Clustered { ref cache, .. } => command.cluster_hash().and_then(|slot| cache.get_server(slot)),
    }
  }

  pub fn has_healthy_centralized_connection(&self) -> bool {
    match self.connections {
      Connections::Centralized { ref writer } | Connections::Sentinel { ref writer } => {
        writer.as_ref().map(|w| w.is_working()).unwrap_or(false)
      },
      Connections::Clustered { .. } => false,
    }
  }

  /// Attempt to send the command to the server.
  pub async fn write(&mut self, command: RedisCommand, force_flush: bool) -> Written {
    let send_all_cluster_nodes =
      self.inner.config.server.is_clustered() && (command.is_all_cluster_nodes() || command.kind.closes_connection());

    if command.write_attempts >= 1 {
      self.inner.counters.incr_redelivery_count();
    }
    if send_all_cluster_nodes {
      self.connections.write_all_cluster(&self.inner, command).await
    } else {
      self.connections.write(&self.inner, command, force_flush).await
    }
  }

  /// Write a command to a replica node if possible, falling back to a primary node if configured.
  #[cfg(feature = "replicas")]
  pub async fn write_replica(&mut self, mut command: RedisCommand, force_flush: bool) -> Written {
    if !command.use_replica {
      return self.write(command, force_flush).await;
    }

    let primary = match self.find_connection(&command) {
      Some(server) => server.clone(),
      None => {
        return if self.inner.connection.replica.primary_fallback {
          debug!(
            "{}: Fallback to primary node connection for {} ({})",
            self.inner.id,
            command.kind.to_str_debug(),
            command.debug_id()
          );

          command.use_replica = false;
          self.write(command, force_flush).await
        } else {
          command.finish(
            &self.inner,
            Err(RedisError::new(
              RedisErrorKind::Replica,
              "Missing primary node connection.",
            )),
          );

          Written::Ignore
        }
      },
    };

    let result = self.replicas.write(&self.inner, &primary, command, force_flush).await;
    match result {
      Written::Fallback(mut command) => {
        debug!(
          "{}: Fall back to primary node for {} ({}) after replica error",
          self.inner.id,
          command.kind.to_str_debug(),
          command.debug_id(),
        );

        utils::defer_replica_sync(&self.inner);
        command.use_replica = false;
        self.write(command, force_flush).await
      },
      _ => result,
    }
  }

  /// Write a command to a replica node if possible, falling back to a primary node if configured.
  #[cfg(not(feature = "replicas"))]
  pub async fn write_replica(&mut self, command: RedisCommand, force_flush: bool) -> Written {
    self.write(command, force_flush).await
  }

  /// Attempt to write the command to a specific server without backpressure.
  pub async fn write_direct(&mut self, mut command: RedisCommand, server: &Server) -> Written {
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
        trace!("{}: Missing connection to {}", self.inner.id, server);
        return Written::NotFound(command);
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
        command.finish(&self.inner, Err(e));
        return Written::Ignore;
      },
    };
    let blocks_connection = command.blocks_connection();
    command.write_attempts += 1;

    if !writer.is_working() {
      let error = RedisError::new(RedisErrorKind::IO, "Connection closed.");
      debug!("{}: Error sending command: {:?}", self.inner.id, error);
      return Written::Disconnected((Some(writer.server.clone()), Some(command), error));
    }

    let no_incr = command.has_no_responses();
    writer.push_command(&self.inner, command);
    if let Err(err) = writer.write_frame(frame, true, no_incr).await {
      Written::Disconnected((Some(writer.server.clone()), None, err))
    } else {
      if blocks_connection {
        self.inner.backchannel.write().await.set_blocked(&writer.server);
      }
      Written::Sent((writer.server.clone(), true))
    }
  }

  /// Disconnect from all the servers, moving the in-flight messages to the internal command buffer and triggering a
  /// reconnection, if necessary.
  pub async fn disconnect_all(&mut self) {
    let commands = self.connections.disconnect_all(&self.inner).await;
    self.buffer_commands(commands);
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
  pub fn buffer_commands(&mut self, commands: impl IntoIterator<Item = RedisCommand>) {
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

    if result.is_ok() {
      if let Err(e) = self.sync_replicas().await {
        if !self.inner.ignore_replica_reconnect_errors() {
          return Err(e);
        }
      }

      self.retry_buffer().await;
    }

    result
  }

  /// Rebuild the cached replica routing table based on the primary node connections.
  #[cfg(feature = "replicas")]
  pub async fn sync_replicas(&mut self) -> Result<(), RedisError> {
    debug!("{}: Syncing replicas...", self.inner.id);
    self.replicas.drop_broken_connections().await;
    let old_connections = self.replicas.active_connections();
    let new_replica_map = self.connections.replica_map(&self.inner).await?;

    let old_connections_idx: HashSet<_> = old_connections.iter().collect();
    let new_connections_idx: HashSet<_> = new_replica_map.keys().collect();
    let remove: Vec<_> = old_connections_idx.difference(&new_connections_idx).collect();

    for server in remove.into_iter() {
      debug!("{}: Dropping replica connection to {}", self.inner.id, server);
      self.replicas.drop_writer(&server).await;
      self.replicas.remove_replica(&server);
    }

    for (mut replica, primary) in new_replica_map.into_iter() {
      let should_use = if let Some(filter) = self.inner.connection.replica.filter.as_ref() {
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
    Ok(())
  }

  /// Rebuild the cached replica routing table based on the primary node connections.
  #[cfg(not(feature = "replicas"))]
  pub async fn sync_replicas(&mut self) -> Result<(), RedisError> {
    Ok(())
  }

  /// Attempt to replay all queued commands on the internal buffer without backpressure.
  pub async fn retry_buffer(&mut self) {
    let mut failed_commands: VecDeque<_> = VecDeque::new();
    let mut commands: VecDeque<_> = self.buffer.drain(..).collect();
    #[cfg(feature = "replicas")]
    commands.extend(self.replicas.take_retry_buffer());

    for mut command in commands.drain(..) {
      if client_utils::read_bool_atomic(&command.timed_out) {
        debug!(
          "{}: Ignore retrying timed out command: {}",
          self.inner.id,
          command.kind.to_str_debug()
        );
        continue;
      }

      if let Err(e) = command.decr_check_attempted() {
        command.finish(&self.inner, Err(e));
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

      let result = if command.use_replica {
        self.write_replica(command, true).await
      } else {
        self.write(command, true).await
      };

      match result {
        Written::Disconnected((server, command, error)) => {
          if let Some(command) = command {
            failed_commands.push_back(command);
          }

          debug!(
            "{}: Disconnect while retrying after write error: {:?}",
            &self.inner.id, error
          );
          self.connections.disconnect(&self.inner, server.as_ref()).await;
          utils::defer_reconnect(&self.inner);
          continue;
        },
        Written::NotFound(command) => {
          failed_commands.push_back(command);

          warn!(
            "{}: Disconnect and re-sync cluster state after routing error while retrying commands.",
            self.inner.id
          );
          self.disconnect_all().await;
          utils::defer_reconnect(&self.inner);
          break;
        },
        Written::Error((error, command)) => {
          warn!("{}: Error replaying command: {:?}", self.inner.id, error);
          if let Some(command) = command {
            command.finish(&self.inner, Err(error));
          }
          self.disconnect_all().await;
          utils::defer_reconnect(&self.inner);
          break;
        },
        _ => {},
      }
    }

    failed_commands.extend(commands);
    self.buffer_commands(failed_commands);
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
  #[cfg(feature = "transactions")]
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
        .with_cluster_state(|state| Ok(state.get_server(slot).map(|owner| server != owner).unwrap_or(true)))
        .unwrap_or(true);

      if should_sync {
        self.sync_cluster().await?;
      }
    } else if *kind == ClusterErrorKind::Ask {
      if !self.connections.has_server_connection(server) {
        self.connections.add_connection(&self.inner, server).await?;
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
      command.skip_backpressure = true;

      match self.write_direct(command, server).await {
        Written::Error((error, _)) => return Err(error),
        Written::Disconnected((_, _, error)) => return Err(error),
        Written::NotFound(_) => return Err(RedisError::new(RedisErrorKind::Cluster, "Connection not found.")),
        _ => {},
      };

      let _ = client_utils::apply_timeout(rx, self.inner.internal_command_timeout()).await??;
    }

    Ok(())
  }
}
