use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::RedisCommand,
    connection,
    connection::{Counters, RedisConnection},
    types::ClusterRouting,
  },
  router::{centralized, clustered, sentinel, WriteResult},
  runtime::RefCount,
  types::Server,
};
use futures::future::try_join_all;
use semver::Version;
use std::collections::{HashMap, VecDeque};

/// Connection maps for the supported deployment types.
pub enum Connections {
  Centralized {
    /// The connection to the primary server.
    writer: Option<RedisConnection>,
  },
  Clustered {
    /// The cached cluster routing table used for mapping keys to server IDs.
    cache:   ClusterRouting,
    /// A map of server IDs and connections.
    writers: HashMap<Server, RedisConnection>,
  },
  Sentinel {
    /// The connection to the primary server.
    writer: Option<RedisConnection>,
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
  pub async fn replica_map(
    &mut self,
    inner: &RefCount<RedisClientInner>,
  ) -> Result<HashMap<Server, Server>, RedisError> {
    Ok(match self {
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer {
          connection::discover_replicas(inner, writer)
            .await?
            .into_iter()
            .map(|replica| (replica, writer.server.clone()))
            .collect()
        } else {
          HashMap::new()
        }
      },
      Connections::Clustered { ref writers, .. } => {
        let mut out = HashMap::with_capacity(writers.len());

        for primary in writers.keys() {
          let replicas = inner
            .with_cluster_state(|state| Ok(state.replicas(primary)))
            .ok()
            .unwrap_or_default();

          for replica in replicas.into_iter() {
            out.insert(replica, primary.clone());
          }
        }
        out
      },
    })
  }

  /// Whether the connection map has a connection to the provided server`.
  pub async fn has_server_connection(&mut self, server: &Server) -> bool {
    match self {
      Connections::Centralized { ref mut writer } | Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.as_mut() {
          if writer.server == *server {
            writer.peek_reader_errors().await.is_none()
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
            return writer.peek_reader_errors().await.is_none();
          }
        }

        false
      },
    }
  }

  /// Get the connection writer half for the provided server.
  pub fn get_connection_mut(&mut self, server: &Server) -> Option<&mut RedisConnection> {
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
    inner: &RefCount<RedisClientInner>,
    buffer: &mut VecDeque<RedisCommand>,
  ) -> Result<(), RedisError> {
    let result = if inner.config.server.is_clustered() {
      Box::pin(clustered::initialize_connections(inner, self, buffer)).await
    } else if inner.config.server.is_centralized() || inner.config.server.is_unix_socket() {
      Box::pin(centralized::initialize_connection(inner, self, buffer)).await
    } else if inner.config.server.is_sentinel() {
      Box::pin(sentinel::initialize_connection(inner, self, buffer)).await
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
  pub async fn disconnect(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    server: Option<&Server>,
  ) -> VecDeque<RedisCommand> {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.close().await
        } else {
          VecDeque::new()
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        let mut out = VecDeque::new();

        if let Some(server) = server {
          if let Some(writer) = writers.remove(server) {
            _debug!(inner, "Disconnecting from {}", writer.server);
            let commands = writer.close().await;
            out.extend(commands);
          }
        }
        out.into_iter().collect()
      },
      Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.close().await
        } else {
          VecDeque::new()
        }
      },
    }
  }

  /// Disconnect and clear local state for all connections, returning all in-flight commands.
  pub async fn disconnect_all(&mut self, inner: &RefCount<RedisClientInner>) -> VecDeque<RedisCommand> {
    match self {
      Connections::Centralized { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.close().await
        } else {
          VecDeque::new()
        }
      },
      Connections::Clustered { ref mut writers, .. } => {
        let mut out = VecDeque::new();
        for (_, writer) in writers.drain() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          let commands = writer.close().await;
          out.extend(commands.into_iter());
        }
        out.into_iter().collect()
      },
      Connections::Sentinel { ref mut writer } => {
        if let Some(writer) = writer.take() {
          _debug!(inner, "Disconnecting from {}", writer.server);
          writer.close().await
        } else {
          VecDeque::new()
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
  pub async fn check_and_flush(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
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

  /// Send a command to all servers in a cluster.
  pub async fn write_all_cluster(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    command: RedisCommand,
  ) -> WriteResult {
    if let Connections::Clustered { ref mut writers, .. } = self {
      if let Err(error) = clustered::send_all_cluster_command(inner, writers, command).await {
        WriteResult::Disconnected((None, None, error))
      } else {
        WriteResult::SentAll
      }
    } else {
      WriteResult::Error((
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
  pub async fn add_connection(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    server: &Server,
  ) -> Result<(), RedisError> {
    if let Connections::Clustered { ref mut writers, .. } = self {
      let mut transport = connection::create(inner, server, None).await?;
      transport.setup(inner, None).await?;
      writers.insert(server.clone(), transport.into_pipelined(false));
      Ok(())
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected clustered configuration.",
      ))
    }
  }
}