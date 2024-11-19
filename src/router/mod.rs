pub mod centralized;
pub mod clustered;
pub mod commands;
pub mod connections;
pub mod replicas;
pub mod responses;
pub mod sentinel;
pub mod types;
pub mod utils;

use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{command::RedisCommand, connection::Counters, types::Server},
  router::{connections::Connections, utils::next_frame},
  runtime::RefCount,
  trace,
  types::Resp3Frame,
  utils as client_utils,
};
use futures::future::{select_all, try_join_all};
use std::{collections::VecDeque, fmt, fmt::Formatter, time::Duration};

#[cfg(feature = "replicas")]
use std::collections::HashSet;

#[cfg(feature = "transactions")]
pub mod transactions;

#[cfg(feature = "replicas")]
use crate::router::replicas::Replicas;

/// The result of an attempt to send a command to the server.
pub enum WriteResult {
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

impl fmt::Display for WriteResult {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", match self {
      WriteResult::Backpressure(_) => "Backpressure",
      WriteResult::Sent(_) => "Sent",
      WriteResult::SentAll => "SentAll",
      WriteResult::Disconnected(_) => "Disconnected",
      WriteResult::Ignore => "Ignore",
      WriteResult::NotFound(_) => "NotFound",
      WriteResult::Error(_) => "Error",
      #[cfg(feature = "replicas")]
      WriteResult::Fallback(_) => "Fallback",
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
  pub fn should_wait(&self) -> bool {
    matches!(self, Backpressure::Wait(_))
  }

  /// Apply the `wait` backpressure policy.
  pub async fn wait(self, inner: &RefCount<RedisClientInner>, command: &mut RedisCommand) -> Result<(), RedisError> {
    match self {
      Backpressure::Wait(duration) => {
        _debug!(inner, "Backpressure policy (wait): {:?}", duration);
        trace::backpressure_event(command, Some(duration.as_millis()));
        inner.wait_with_interrupt(duration).await?;
        Ok(())
      },
      _ => Ok(()),
    }
  }
}

/// A struct for routing commands to the server(s).
pub struct Router {
  pub inner:        RefCount<RedisClientInner>,
  /// The connection map for each deployment type.
  pub connections:  Connections,
  /// Storage for commands that should be deferred or retried later.
  pub retry_buffer: VecDeque<RedisCommand>,
  /// The replica routing interface.
  #[cfg(feature = "replicas")]
  pub replicas:     Replicas,
}

impl Router {
  /// Create a new `Router` without connecting to the server(s).
  pub fn new(inner: &RefCount<RedisClientInner>) -> Self {
    let connections = if inner.config.server.is_clustered() {
      Connections::new_clustered()
    } else if inner.config.server.is_sentinel() {
      Connections::new_sentinel()
    } else {
      Connections::new_centralized()
    };

    Router {
      inner: inner.clone(),
      retry_buffer: VecDeque::new(),
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

  /// Attempt to send the command to the server.
  #[inline(always)]
  pub async fn write(&mut self, mut command: RedisCommand, force_flush: bool) -> WriteResult {
    let send_all_cluster_nodes =
      self.inner.config.server.is_clustered() && (command.is_all_cluster_nodes() || command.kind.closes_connection());

    if command.write_attempts >= 1 {
      self.inner.counters.incr_redelivery_count();
    }
    if send_all_cluster_nodes {
      if let Err(err) = self.drain().await {
        WriteResult::Disconnected((None, Some(command), err))
      } else {
        self.connections.write_all_cluster(&self.inner, command).await
      }
    } else {
      // note: the logic in these branches used to be abstracted into separate `write` functions in the centralized
      // and clustered modules. this was much easier to read, but the added async/await overhead of that function call
      // reduced throughput by ~10%, so this logic was inlined here instead. it's ugly but significantly faster.
      match self.connections {
        Connections::Clustered {
          ref mut writers,
          ref mut cache,
        } => {
          let has_custom_server = command.cluster_node.is_some();
          let server = match clustered::route_command(&self.inner, &cache, &command) {
            Some(server) => server,
            None => {
              return if has_custom_server {
                debug!(
                  "{}: Respond to caller with error from missing cluster node override ({:?})",
                  self.inner.id, command.cluster_node
                );
                command.respond_to_caller(Err(RedisError::new(
                  RedisErrorKind::Cluster,
                  "Missing cluster node override.",
                )));

                WriteResult::Ignore
              } else {
                // these errors usually mean the cluster is partially down or misconfigured
                warn!(
                  "{}: Possible cluster misconfiguration. Missing hash slot owner for {:?}.",
                  self.inner.id,
                  command.cluster_hash()
                );
                WriteResult::NotFound(command)
              };
            },
          };

          if let Some(writer) = writers.get_mut(server) {
            debug!(
              "{}: Writing command `{}` to {}",
              self.inner.id,
              command.kind.to_str_debug(),
              server
            );
            utils::write_command(&self.inner, writer, command, force_flush).await
          } else {
            // a reconnect message should already be queued from the reader task
            debug!(
              "{}: Failed to read connection {} for {}",
              self.inner.id,
              server,
              command.kind.to_str_debug()
            );

            WriteResult::Disconnected((
              Some(server.clone()),
              Some(command),
              RedisError::new(RedisErrorKind::IO, "Missing connection."),
            ))
          }
        },
        Connections::Centralized { ref mut writer } => {
          if let Some(writer) = writer.as_mut() {
            utils::write_command(&self.inner, writer, command, force_flush).await
          } else {
            debug!(
              "{}: Failed to read connection for {}",
              self.inner.id,
              command.kind.to_str_debug()
            );
            WriteResult::Disconnected((
              None,
              Some(command),
              RedisError::new(RedisErrorKind::IO, "Missing connection."),
            ))
          }
        },
        Connections::Sentinel { ref mut writer, .. } => {
          if let Some(writer) = writer.as_mut() {
            utils::write_command(&self.inner, writer, command, force_flush).await
          } else {
            debug!(
              "{}: Failed to read connection for {}",
              self.inner.id,
              command.kind.to_str_debug()
            );
            WriteResult::Disconnected((
              None,
              Some(command),
              RedisError::new(RedisErrorKind::IO, "Missing connection."),
            ))
          }
        },
      }
    }
  }

  /// Write a command to a replica node if possible, falling back to a primary node if configured.
  #[cfg(feature = "replicas")]
  pub async fn write_replica(&mut self, mut command: RedisCommand, force_flush: bool) -> WriteResult {
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
          command.respond_to_caller(Err(RedisError::new(
            RedisErrorKind::Replica,
            "Missing primary node connection.",
          )));

          WriteResult::Ignore
        }
      },
    };

    let result = self.replicas.write(&self.inner, &primary, command, force_flush).await;
    match result {
      WriteResult::Fallback(mut command) => {
        debug!(
          "{}: Fall back to primary node for {} ({}) after replica error",
          self.inner.id,
          command.kind.to_str_debug(),
          command.debug_id(),
        );

        command.use_replica = false;
        self.write(command, force_flush).await
      },
      _ => result,
    }
  }

  /// Write a command to a replica node if possible, falling back to a primary node if configured.
  #[cfg(not(feature = "replicas"))]
  pub async fn write_replica(&mut self, command: RedisCommand, force_flush: bool) -> WriteResult {
    self.write(command, force_flush).await
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
    self.retry_buffer.push_back(command);
  }

  /// Clear all the commands in the retry buffer.
  pub fn clear_retry_buffer(&mut self) {
    trace!(
      "{}: Clearing retry buffer with {} commands.",
      self.inner.id,
      self.retry_buffer.len()
    );
    self.retry_buffer.clear();
  }

  /// Connect to the server(s), discarding any previous connection state.
  pub async fn connect(&mut self) -> Result<(), RedisError> {
    self.disconnect_all().await;
    let result = self.connections.initialize(&self.inner, &mut self.retry_buffer).await;

    if result.is_ok() {
      #[cfg(feature = "replicas")]
      self.refresh_replica_routing().await?;

      Ok(())
    } else {
      result
    }
  }

  /// Gracefully reset the replica routing table.
  #[cfg(feature = "replicas")]
  pub async fn refresh_replica_routing(&mut self) -> Result<(), RedisError> {
    self.replicas.clear_routing();
    if let Err(e) = self.sync_replicas().await {
      if !self.inner.ignore_replica_reconnect_errors() {
        return Err(e);
      }
    }

    Ok(())
  }

  /// Sync the cached cluster state with the server via `CLUSTER SLOTS`.
  ///
  /// This will also create new connections or drop old connections as needed.
  pub async fn sync_cluster(&mut self) -> Result<(), RedisError> {
    match self.connections {
      Connections::Clustered {
        ref mut writers,
        ref mut cache,
      } => {
        let result = clustered::sync(&self.inner, writers, cache, &mut self.retry_buffer).await;

        if result.is_ok() {
          #[cfg(feature = "replicas")]
          self.refresh_replica_routing().await?;

          // surface errors from the retry process, otherwise return the reconnection result
          match self.retry_buffer().await {
            WriteResult::Disconnected((_, _, error)) => {
              return Err(error);
            },
            WriteResult::Error((error, _)) => {
              return Err(error);
            },
            _ => {},
          }
        }
        result
      },
      _ => Ok(()),
    }
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
      self.replicas.drop_writer(server).await;
      self.replicas.remove_replica(server);
    }

    for (mut replica, primary) in new_replica_map.into_iter() {
      let should_use = if let Some(filter) = self.inner.connection.replica.filter.as_ref() {
        filter.filter(&primary, &replica).await
      } else {
        true
      };

      if should_use {
        replicas::map_replica_tls_names(&self.inner, &primary, &mut replica);

        self
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

  /// Attempt to replay all queued commands on the internal buffer without backpressure.
  pub async fn retry_buffer(&mut self) -> WriteResult {
    #[cfg(feature = "replicas")]
    {
      let commands = self.replicas.take_retry_buffer();
      self.retry_buffer.extend(commands);
    }

    while let Some(mut command) = self.retry_buffer.pop_front() {
      if client_utils::read_bool_atomic(&command.timed_out) {
        debug!(
          "{}: Ignore retrying timed out command: {}",
          self.inner.id,
          command.kind.to_str_debug()
        );
        continue;
      }

      if let Err(e) = command.decr_check_attempted() {
        command.respond_to_caller(Err(e));
        continue;
      }

      trace!(
        "{}: Retry `{}` ({}) command, attempts left: {}",
        self.inner.id,
        command.kind.to_str_debug(),
        command.debug_id(),
        command.attempts_remaining,
      );
      command.skip_backpressure = true;
      let result = if command.use_replica {
        self.write_replica(command, true).await
      } else {
        self.write(command, true).await
      };

      match result {
        WriteResult::Disconnected((server, command, error)) => {
          // disconnect and return the error to the caller
          if let Some(command) = command {
            self.retry_buffer.push_front(command);
          }

          debug!(
            "{}: Disconnect while retrying after write error: {:?}",
            &self.inner.id, error
          );
          self.connections.disconnect(&self.inner, server.as_ref()).await;
          return WriteResult::Disconnected((server, None, error));
        },
        WriteResult::NotFound(command) => {
          // disconnect all, re-queue the command for later, and return an error to the caller
          self.retry_buffer.push_front(command);

          warn!(
            "{}: Disconnect and re-sync cluster state after routing error while retrying commands.",
            self.inner.id
          );
          self.disconnect_all().await;
          return WriteResult::Disconnected((
            None,
            None,
            RedisError::new(RedisErrorKind::NotFound, "Failed to route command when retrying."),
          ));
        },
        WriteResult::Error((error, command)) => {
          // return the error to the caller
          if let Some(command) = command {
            self.retry_buffer.push_front(command);
          }

          error!("{}: Fatal error retrying command: {:?}", self.inner.id, error);
          return WriteResult::Error((error, None));
        },
        #[cfg(feature = "replicas")]
        WriteResult::Fallback(mut command) => {
          // try to fall back to a primary node or return an error to the caller
          if self.inner.connection.replica.primary_fallback {
            command.use_replica = false;
            self.retry_buffer.push_front(command);
            continue;
          } else {
            error!(
              "{}: Failed to send command to replica and client is not configured to allow fallback to a primary \
               node.",
              self.inner.id
            );
            command.respond_to_caller(Err(RedisError::new(
              RedisErrorKind::Unknown,
              "Could not send command to replica node.",
            )));
          }
        },
        _ => continue,
      };
    }

    WriteResult::SentAll
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

  /// Wait and read frames until there are no in-flight frames on replica connections.
  #[cfg(feature = "replicas")]
  pub async fn drain_replicas(&mut self) -> Result<(), RedisError> {
    let inner = self.inner.clone();
    try_join_all(self.replicas.writers.iter_mut().map(|(_, conn)| conn.drain(&inner))).await?;

    Ok(())
  }

  /// Wait and read frames until there are no in-flight frames on primary connections.
  pub async fn drain(&mut self) -> Result<(), RedisError> {
    let inner = self.inner.clone();
    _trace!(inner, "Draining all connections...");

    match self.connections {
      Connections::Clustered { ref mut writers, .. } => {
        try_join_all(writers.iter_mut().map(|(_, conn)| conn.drain(&inner))).await?;

        Ok(())
      },
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => match writer {
        Some(ref mut conn) => conn.drain(&inner).await,
        None => Ok(()),
      },
    }
  }

  pub async fn has_healthy_centralized_connection(&mut self) -> bool {
    match self.connections {
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer {
          writer.peek_reader_errors().await.is_none()
        } else {
          false
        }
      },
      _ => false,
    }
  }

  /// Try to read from all sockets concurrently, returning the first result that completes.
  pub async fn select_read(&mut self) -> Option<(Server, Result<Resp3Frame, RedisError>)> {
    match self.connections {
      Connections::Clustered { ref mut writers, .. } => {
        if writers.is_empty() {
          return None;
        }

        select_all(writers.iter_mut().map(|(_, conn)| {
          Box::pin(async {
            match next_frame!(&self.inner, conn) {
              Ok(Some(frame)) => Some((conn.server.clone(), Ok(frame))),
              Ok(None) => None,
              Err(e) => Some((conn.server.clone(), Err(e))),
            }
          })
        }))
        .await
        .0
      },
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => {
        if let Some(conn) = writer {
          match next_frame!(&self.inner, conn) {
            Ok(Some(frame)) => Some((conn.server.clone(), Ok(frame))),
            Ok(None) => None,
            Err(e) => Some((conn.server.clone(), Err(e))),
          }
        } else {
          None
        }
      },
    }
  }

  #[cfg(feature = "replicas")]
  pub fn is_replica(&self, server: &Server) -> bool {
    self.replicas.writers.contains_key(server)
  }

  #[cfg(not(feature = "replicas"))]
  pub fn is_replica(&self, _: &Server) -> bool {
    false
  }
}
