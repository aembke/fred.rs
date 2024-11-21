pub mod centralized;
pub mod clustered;
pub mod commands;
pub mod connections;
#[cfg(feature = "replicas")]
pub mod replicas;
pub mod responses;
pub mod sentinel;
pub mod types;
pub mod utils;

use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::RedisCommand,
    connection::{Counters, RedisConnection},
    types::Server,
  },
  router::{connections::Connections, utils::next_frame},
  runtime::RefCount,
  types::Resp3Frame,
  utils as client_utils,
};
use futures::future::{join_all, select_all};
use std::{collections::VecDeque, fmt, fmt::Formatter, future::pending, time::Duration};

#[cfg(feature = "replicas")]
use futures::future::{select, try_join, Either};
#[cfg(feature = "replicas")]
use std::collections::HashSet;
#[cfg(feature = "replicas")]
use tokio::pin;

#[cfg(feature = "transactions")]
pub mod transactions;
#[cfg(feature = "replicas")]
use replicas::Replicas;

macro_rules! read_primary_nodes {
  ($router:expr, $inner:expr) => {{
    match $router.connections {
      Connections::Clustered { ref mut writers, .. } => {
        if writers.is_empty() {
          // this is used in the context of select!, so we want to wait rather than break early.
          pending::<()>().await;
          return None;
        }

        select_all(writers.iter_mut().map(|(_, conn)| {
          Box::pin(async {
            match next_frame!($inner, conn) {
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
          match next_frame!($inner, conn) {
            Ok(Some(frame)) => Some((conn.server.clone(), Ok(frame))),
            Ok(None) => None,
            Err(e) => Some((conn.server.clone(), Err(e))),
          }
        } else {
          // this is used in the context of select!, so we want to wait rather than break early.
          pending::<()>().await;
          None
        }
      },
    }
  }};
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

  /// Find the primary node that owns the hash slot used by the command.
  pub fn cluster_owner(&self, command: &RedisCommand) -> Option<&Server> {
    match self.connections {
      Connections::Clustered { ref cache, .. } => command.cluster_hash().and_then(|slot| cache.get_server(slot)),
      _ => None,
    }
  }

  /// Find the connection that should receive the provided command.
  #[cfg(feature = "replicas")]
  pub fn route<'a>(&'a mut self, command: &RedisCommand) -> Option<&'a mut RedisConnection> {
    match command.cluster_node.as_ref() {
      Some(server) => {
        if command.use_replica {
          self
            .replicas
            .routing
            .next_replica(server)
            .and_then(|replica| self.replicas.writers.get_mut(replica))
        } else {
          self.connections.get_connection_mut(server)
        }
      },
      None => {
        if command.use_replica {
          self
            .cluster_owner(command)
            .and_then(|primary| self.replicas.routing.next_replica(primary))
            .and_then(|replica| self.replicas.writers.get_mut(replica))
        } else {
          match self.connections {
            Connections::Centralized { ref mut writer } => writer.as_mut(),
            Connections::Sentinel { ref mut writer } => writer.as_mut(),
            Connections::Clustered {
              ref mut writers,
              ref cache,
            } => command
              .cluster_hash()
              .and_then(|slot| cache.get_server(slot))
              .and_then(|server| writers.get_mut(server)),
          }
        }
      },
    }
  }

  /// Find the connection that should receive the provided command.
  #[cfg(not(feature = "replicas"))]
  pub fn route<'a>(&'a mut self, command: &RedisCommand) -> Option<&'a mut RedisConnection> {
    match command.cluster_node.as_ref() {
      Some(server) => self.connections.get_connection_mut(server),
      None => match self.connections {
        Connections::Centralized { ref mut writer, .. } => writer.as_mut(),
        Connections::Sentinel { ref mut writer, .. } => writer.as_mut(),
        Connections::Clustered {
          ref mut writers,
          ref cache,
        } => command
          .cluster_hash()
          .and_then(|slot| cache.get_server(slot))
          .and_then(|server| writers.get_mut(server))
          .or_else(|| writers.values_mut().next()),
      },
    }
  }

  #[cfg(feature = "replicas")]
  pub fn get_connection_mut<'a>(&mut self, server: &Server) -> Option<&mut RedisConnection> {
    self
      .connections
      .get_connection_mut(server)
      .or_else(|| self.replicas.writers.get_mut(server))
  }

  #[cfg(not(feature = "replicas"))]
  pub fn get_connection_mut<'a>(&mut self, server: &Server) -> Option<&mut RedisConnection> {
    self.connections.get_connection_mut(server)
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
          Box::pin(self.retry_buffer()).await?;
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
    let old_connections = self.replicas.active_connections().await;
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
  pub async fn retry_buffer(&mut self) -> Result<(), RedisError> {
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

      // TODO find connection, write to it, put back in buffer on failure
      unimplemented!()
    }

    Ok(())
  }

  /// Check each connection for pending frames that have not been flushed, and flush the connection if needed.
  #[cfg(feature = "replicas")]
  pub async fn flush(&mut self) -> Result<(), RedisError> {
    if let Err(e) = self.replicas.flush().await {
      warn!("{}: Error flushing replica connections: {:?}", self.inner.id, e);
    }
    self.connections.flush(&self.inner).await
  }

  #[cfg(not(feature = "replicas"))]
  pub async fn lush(&mut self) -> Result<(), RedisError> {
    self.connections.flush(&self.inner).await
  }

  /// Wait and read frames until there are no in-flight frames on primary connections.
  pub async fn drain_all(&mut self) -> Result<(), RedisError> {
    let inner = self.inner.clone();
    _trace!(inner, "Draining all connections...");

    let primary_ft = async {
      match self.connections {
        Connections::Clustered { ref mut writers, .. } => {
          // drain all connections even if one of them breaks out early with an error
          let _ = join_all(writers.iter_mut().map(|(_, conn)| conn.drain(&inner)))
            .await
            .into_iter()
            .collect::<Result<Vec<()>, RedisError>>()?;

          Ok(())
        },
        Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => match writer {
          Some(ref mut conn) => conn.drain(&inner).await,
          None => Ok(()),
        },
      }
    };

    #[cfg(feature = "replicas")]
    return try_join(primary_ft, self.replicas.drain(&inner)).await.map(|_| ());
    #[cfg(not(feature = "replicas"))]
    primary_ft.await
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
  #[cfg(feature = "replicas")]
  pub async fn select_read(
    &mut self,
    inner: &RefCount<RedisClientInner>,
  ) -> Option<(Server, Result<Resp3Frame, RedisError>)> {
    let primary_ft = async { read_primary_nodes!(self, inner) };
    pin!(primary_ft);
    let replica_ft = self.replicas.select_read(inner);
    pin!(replica_ft);

    match select(primary_ft, replica_ft).await {
      Either::Left((l, _)) => l,
      Either::Right((r, _)) => r,
    }
  }

  /// Try to read from all sockets concurrently, returning the first result that completes.
  #[cfg(not(feature = "replicas"))]
  pub async fn select_read(
    &mut self,
    inner: &RefCount<RedisClientInner>,
  ) -> Option<(Server, Result<Resp3Frame, RedisError>)> {
    read_primary_nodes!(self, inner)
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
