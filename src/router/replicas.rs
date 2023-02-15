#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::types::{HostMapping, TlsHostMapping};
#[cfg(feature = "replicas")]
use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind, RouterCommand},
    connection,
    connection::{CommandBuffer, RedisWriter},
  },
  router::{centralized, clustered, utils, Written},
  types::Server,
};
#[cfg(feature = "replicas")]
use std::{
  collections::{HashMap, VecDeque},
  fmt,
  fmt::Formatter,
  sync::Arc,
};

/// An interface used to filter the list of available replica nodes.
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[async_trait]
pub trait ReplicaFilter: Send + Sync + 'static {
  /// Returns whether the replica node mapping can be used when routing commands to replicas.
  #[allow(unused_variables)]
  async fn filter(&self, primary: &Server, replica: &Server) -> bool {
    true
  }
}

/// Configuration options for replica node connections.
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone)]
pub struct ReplicaConfig {
  /// Whether the client should lazily connect to replica nodes.
  ///
  /// Default: `true`
  pub lazy_connections:           bool,
  /// An optional interface for filtering available replica nodes.
  ///
  /// Default: `None`
  pub filter:                     Option<Arc<dyn ReplicaFilter>>,
  /// Whether the client should ignore errors from replicas that occur when the max reconnection count is reached.
  ///
  /// Default: `true`
  pub ignore_reconnection_errors: bool,
  /// The number of times a command can fail with a replica connection error before being sent to a primary node.
  ///
  /// Default: `0` (unlimited)
  pub connection_error_count:     u32,
  /// Whether the client should use the associated primary node if no replica exists that can serve a command.
  ///
  /// Default: `true`
  pub primary_fallback:           bool,
}

#[cfg(feature = "replicas")]
impl fmt::Debug for ReplicaConfig {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("ReplicaConfig")
      .field("lazy_connections", &self.lazy_connections)
      .field("ignore_reconnection_errors", &self.ignore_reconnection_errors)
      .field("connection_error_count", &self.connection_error_count)
      .field("primary_fallback", &self.primary_fallback)
      .finish()
  }
}

#[cfg(feature = "replicas")]
impl PartialEq for ReplicaConfig {
  fn eq(&self, other: &Self) -> bool {
    self.lazy_connections == other.lazy_connections
      && self.ignore_reconnection_errors == other.ignore_reconnection_errors
      && self.connection_error_count == other.connection_error_count
      && self.primary_fallback == other.primary_fallback
  }
}

#[cfg(feature = "replicas")]
impl Eq for ReplicaConfig {}

#[cfg(feature = "replicas")]
impl Default for ReplicaConfig {
  fn default() -> Self {
    ReplicaConfig {
      lazy_connections:           true,
      filter:                     None,
      ignore_reconnection_errors: true,
      connection_error_count:     0,
      primary_fallback:           true,
    }
  }
}

/// A container for round-robin routing to replica servers.
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReplicaSet {
  /// A map of primary server IDs to a counter and set of replica server IDs.
  servers: HashMap<Server, (usize, Vec<Server>)>,
}

#[cfg(feature = "replicas")]
#[allow(dead_code)]
impl ReplicaSet {
  /// Create a new empty replica set.
  pub fn new() -> ReplicaSet {
    ReplicaSet {
      servers: HashMap::new(),
    }
  }

  /// Add a replica node to the routing table.
  pub fn add(&mut self, primary: Server, replica: Server) {
    let (_, replicas) = self.servers.entry(primary).or_insert((0, Vec::new()));

    if !replicas.contains(&replica) {
      replicas.push(replica);
    }
  }

  /// Remove a replica node mapping from the routing table.
  pub fn remove(&mut self, primary: &Server, replica: &Server) {
    if let Some((count, mut replicas)) = self.servers.remove(primary) {
      replicas = replicas.drain(..).filter(|node| node != replica).collect();

      if !replicas.is_empty() {
        self.servers.insert(primary.clone(), (count, replicas));
      }
    }
  }

  /// Read the server ID of the next replica that should receive a command.
  pub fn next_replica(&mut self, primary: &Server) -> Option<&Server> {
    self.servers.get_mut(primary).and_then(|(idx, replicas)| {
      *idx += 1;
      replicas.get(*idx % replicas.len())
    })
  }

  /// Read all the replicas associated with the provided primary node.
  pub fn replicas(&self, primary: &Server) -> Option<&Vec<Server>> {
    self.servers.get(primary).map(|(_, replicas)| replicas)
  }

  /// Return a map of replica nodes to primary nodes.
  pub fn to_map(&self) -> HashMap<Server, Server> {
    let mut out = HashMap::with_capacity(self.servers.len());
    for (primary, (_, replicas)) in self.servers.iter() {
      for replica in replicas.iter() {
        out.insert(replica.clone(), primary.clone());
      }
    }

    out
  }

  /// Read the set of all known replica nodes for all primary nodes.
  pub fn all_replicas(&self) -> Vec<Server> {
    let mut out = Vec::with_capacity(self.servers.len());
    for (_, (_, replicas)) in self.servers.iter() {
      for replica in replicas.iter() {
        out.push(replica.clone());
      }
    }

    out
  }

  /// Clear the routing table.
  pub fn clear(&mut self) {
    self.servers.clear();
  }
}

/// A struct for routing commands to replica nodes.
#[cfg(feature = "replicas")]
pub struct Replicas {
  writers: HashMap<Server, RedisWriter>,
  routing: ReplicaSet,
  buffer:  CommandBuffer,
}

#[cfg(feature = "replicas")]
#[allow(dead_code)]
impl Replicas {
  pub fn new() -> Replicas {
    Replicas {
      writers: HashMap::new(),
      routing: ReplicaSet::new(),
      buffer:  VecDeque::new(),
    }
  }

  pub fn add_to_retry_buffer(&mut self, command: RedisCommand) {
    self.buffer.push_back(command);
  }

  /// Retry the commands in the cached retry buffer by sending them to the router again.
  pub fn retry_buffer(&mut self, inner: &Arc<RedisClientInner>) {
    let retry_count = inner.config.replica.connection_error_count;
    for mut command in self.buffer.drain(..) {
      if retry_count > 0 && command.attempted >= retry_count {
        _trace!(
          inner,
          "Switch {} ({}) to fall back to primary after retry.",
          command.kind.to_str_debug(),
          command.debug_id()
        );
        command.attempted = 0;
        command.use_replica = false;
      }

      if let Err(e) = interfaces::send_to_router(inner, RouterCommand::Command(command)) {
        _error!(inner, "Error sending replica command to router: {:?}", e);
      }
    }
  }

  /// Sync the connection map in place based on the cached routing table.
  pub async fn sync_connections(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    for (_, writer) in self.writers.drain() {
      let commands = writer.graceful_close().await;
      self.buffer.extend(commands);
    }

    for (replica, primary) in self.routing.to_map() {
      let _ = self.add_connection(inner, primary, replica, true).await?;
    }

    Ok(())
  }

  /// Drop all connections and clear the cached routing table.
  pub async fn clear_connections(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    self.routing.clear();
    self.sync_connections(inner).await
  }

  /// Connect to the replica and add it to the cached routing table.
  pub async fn add_connection(
    &mut self,
    inner: &Arc<RedisClientInner>,
    primary: Server,
    replica: Server,
    force: bool,
  ) -> Result<(), RedisError> {
    _debug!(
      inner,
      "Adding replica connection {} (replica) -> {} (primary)",
      replica,
      primary
    );

    if !inner.config.replica.lazy_connections || force {
      let mut transport = connection::create(
        inner,
        replica.host.as_str().to_owned(),
        replica.port,
        None,
        replica.tls_server_name.as_ref(),
      )
      .await?;
      let _ = transport.setup(inner, None).await?;

      let (_, writer) = if inner.config.server.is_clustered() {
        connection::split_and_initialize(inner, transport, true, clustered::spawn_reader_task)?
      } else {
        connection::split_and_initialize(inner, transport, true, centralized::spawn_reader_task)?
      };

      self.writers.insert(replica.clone(), writer);
    }

    self.routing.add(primary, replica);
    Ok(())
  }

  /// Close the replica connection and optionally remove the replica from the routing table.
  pub async fn remove_connection(
    &mut self,
    inner: &Arc<RedisClientInner>,
    primary: &Server,
    replica: &Server,
    keep_routable: bool,
  ) -> Result<(), RedisError> {
    _debug!(
      inner,
      "Removing replica connection {} (replica) -> {} (primary)",
      replica,
      primary
    );
    if let Some(writer) = self.writers.remove(replica) {
      let commands = writer.graceful_close().await;
      self.buffer.extend(commands);
    }

    if !keep_routable {
      self.routing.remove(primary, replica);
    }
    Ok(())
  }

  /// Check and flush all the sockets managed by the replica routing state.
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    for (_, writer) in self.writers.iter_mut() {
      let _ = writer.flush().await?;
    }

    Ok(())
  }

  /// Whether a working connection exists to any replica for the provided primary node.
  pub fn has_replica_connection(&self, primary: &Server) -> bool {
    if let Some(replicas) = self.routing.replicas(primary) {
      for replica in replicas.iter() {
        if self.has_connection(replica) {
          return true;
        }
      }
    }

    false
  }

  /// Whether a connection exists to the provided replica node.
  pub fn has_connection(&self, replica: &Server) -> bool {
    self.writers.get(replica).map(|w| w.is_working()).unwrap_or(false)
  }

  /// Return a map of `replica` -> `primary` server identifiers.
  pub fn routing_table(&self) -> HashMap<Server, Server> {
    self.routing.to_map()
  }

  /// Check if the provided connection has any known replica nodes, and if so add them to the cached routing table.
  pub async fn check_replicas(
    &mut self,
    inner: &Arc<RedisClientInner>,
    primary: &mut RedisWriter,
  ) -> Result<(), RedisError> {
    let command = RedisCommand::new(RedisCommandKind::Info, vec!["replication".into()]);
    let frame = connection::request_response(inner, primary, command, None)
      .await?
      .as_str()
      .map(|s| s.to_owned())
      .ok_or(RedisError::new(
        RedisErrorKind::Replica,
        "Failed to read replication info.",
      ))?;

    for replica in parse_info_replication(frame) {
      self.routing.add(primary.server.clone(), replica);
    }
    Ok(())
  }

  /// Send a command to one of the replicas associated with the provided primary server.
  pub async fn write_command(
    &mut self,
    inner: &Arc<RedisClientInner>,
    primary: &Server,
    mut command: RedisCommand,
    force_flush: bool,
  ) -> Result<Written, (RedisError, RedisCommand)> {
    let replica = match self.routing.next_replica(primary) {
      Some(replica) => replica.clone(),
      None => {
        // these errors indicate we do not know of any replica node associated with the primary node

        return if inner.config.replica.primary_fallback {
          // FIXME this is ugly and leaks implementation details to the caller
          Err((
            RedisError::new(RedisErrorKind::Replica, "Missing replica node."),
            command,
          ))
        } else {
          command.respond_to_caller(Err(RedisError::new(RedisErrorKind::Replica, "Missing replica node.")));
          Ok(Written::Ignore)
        };
      },
    };
    let writer = match self.writers.get_mut(&replica) {
      Some(writer) => writer,
      None => {
        // these errors indicate that we know a replica node _should_ exist, but we are not connected or cannot
        // connect to it. in this case we want to hide the error, trigger a reconnect, and retry the command later.

        if inner.config.replica.lazy_connections {
          _debug!(inner, "Lazily adding {} replica connection", replica);
          if let Err(e) = self.add_connection(inner, primary.clone(), replica.clone(), true).await {
            return Err((e, command));
          }

          match self.writers.get_mut(&replica) {
            Some(writer) => writer,
            None => {
              return Err((
                RedisError::new(RedisErrorKind::Replica, "Missing replica node connection."),
                command,
              ))
            },
          }
        } else {
          return Err((
            RedisError::new(RedisErrorKind::Replica, "Missing replica node connection."),
            command,
          ));
        }
      },
    };
    let (frame, should_flush) = match utils::prepare_command(inner, &writer.counters, &mut command) {
      Ok((frame, should_flush)) => (frame, should_flush || force_flush),
      Err(e) => {
        _warn!(inner, "Frame encoding error for {}", command.kind.to_str_debug());
        // do not retry commands that trigger frame encoding errors
        command.respond_to_caller(Err(e));
        return Ok(Written::Ignore);
      },
    };

    let blocks_connection = command.blocks_connection();
    // always flush the socket in this case
    _debug!(
      inner,
      "Sending {} ({}) to replica {}",
      command.kind.to_str_debug(),
      command.debug_id(),
      replica
    );
    writer.push_command(command);
    if let Err(e) = writer.write_frame(frame, should_flush).await {
      let command = match writer.pop_recent_command() {
        Some(cmd) => cmd,
        None => {
          _error!(inner, "Failed to take recent command off queue after write failure.");
          return Ok(Written::Ignore);
        },
      };

      _debug!(inner, "Error sending command {}: {:?}", command.kind.to_str_debug(), e);
      Err((e, command))
    } else {
      if blocks_connection {
        inner.backchannel.write().await.set_blocked(&writer.server);
      }

      Ok(Written::Sent((writer.server.clone(), true)))
    }
  }
}

/// Parse the `INFO replication` response for replica node server identifiers.
#[cfg(feature = "replicas")]
pub fn parse_info_replication(frame: String) -> Vec<Server> {
  let mut replicas = Vec::new();
  for line in frame.lines() {
    if line.trim().starts_with("slave") {
      let values = match line.split(":").last() {
        Some(values) => values,
        None => continue,
      };

      let parts: Vec<&str> = values.split(",").collect();
      if parts.len() < 2 {
        continue;
      }

      let (mut host, mut port) = (None, None);
      for kv in parts.into_iter() {
        let parts: Vec<&str> = kv.split("=").collect();
        if parts.len() != 2 {
          continue;
        }

        if parts[0] == "ip" {
          host = Some(parts[1].to_owned());
        } else if parts[0] == "port" {
          port = parts[1].parse::<u16>().ok();
        }
      }

      if let Some(host) = host {
        if let Some(port) = port {
          replicas.push(Server::new(host, port));
        }
      }
    }
  }

  replicas
}

#[cfg(all(feature = "replicas", any(feature = "enable-native-tls", feature = "enable-rustls")))]
pub fn map_replica_tls_names(inner: &Arc<RedisClientInner>, primary: &Server, replica: &mut Server) {
  let policy = match inner.config.tls {
    Some(ref config) => &config.hostnames,
    None => {
      _trace!(inner, "Skip modifying TLS hostname for replicas.");
      return;
    },
  };
  if *policy == TlsHostMapping::None {
    _trace!(inner, "Skip modifying TLS hostnames for replicas.");
    return;
  }

  replica.set_tls_server_name(policy, primary.host.as_str());
}

#[cfg(all(
  feature = "replicas",
  not(any(feature = "enable-native-tls", feature = "enable-rustls"))
))]
pub fn map_replica_tls_names(_: &Arc<RedisClientInner>, _: &Server, _: &mut Server) {}
