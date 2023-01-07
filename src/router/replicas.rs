use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    connection,
    connection::{CommandBuffer, RedisWriter},
  },
  router::{centralized, clustered, utils, Written},
  types::{ReconnectPolicy, Server, SlotRange},
  utils as client_utils,
};
use std::{
  collections::{HashMap, VecDeque},
  net::IpAddr,
  str::FromStr,
  sync::Arc,
};

use crate::types::ScanType::Hash;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::types::{HostMapping, TlsHostMapping};

/// An interface used to filter the list of available replica nodes.
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[async_trait]
pub trait ReplicaFilter: Send + Sync + 'static {
  /// Returns whether the replica node mapping can be used when routing commands to replicas.
  async fn filter(&self, primary: &Server, replica: &Server) -> bool {
    true
  }
}

/// Configuration options for replica node connections.
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaConfig {
  /// Whether the client should lazily connect to replica nodes.
  ///
  /// Default: `false`
  pub lazy_connections:           bool,
  /// An optional interface for filtering available replica nodes.
  ///
  /// Default: `None`
  pub filter:                     Option<Arc<dyn ReplicaFilter>>,
  /// An optional secondary reconnection policy for replica nodes.
  ///
  /// Default: `None`
  pub policy:                     Option<ReconnectPolicy>,
  /// Whether the client should ignore errors that occur when the max reconnection count is reached.
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
impl Default for ReplicaConfig {
  fn default() -> Self {
    ReplicaConfig {
      lazy_connections:           false,
      filter:                     None,
      policy:                     None,
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
    let should_remove = if let Some((_, mut replicas)) = self.servers.get_mut(primary) {
      replicas = replicas.drain(..).filter(|node| node != replica).collect();
      replicas.is_empty()
    } else {
      false
    };

    if should_remove {
      self.servers.remove(primary);
    }
  }

  /// Read the server ID of the next replica that should receive a command.
  pub fn next_replica(&mut self, primary: &Server) -> Option<&Server> {
    self.servers.get_mut(primary).and_then(|(idx, replicas)| {
      *idx += 1;
      replicas.get(idx % replicas.len())
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
}

/// A struct for routing commands to replica nodes.
#[cfg(feature = "replicas")]
pub struct Replicas {
  writers: HashMap<Server, RedisWriter>,
  routing: ReplicaSet,
  buffer:  CommandBuffer,
}

#[cfg(feature = "replicas")]
impl Replicas {
  pub fn new() -> Replicas {
    Replicas {
      writers: HashMap::new(),
      routing: ReplicaSet::new(),
      buffer:  VecDeque::new(),
    }
  }

  /// Drain the cached command retry buffer.
  pub fn take_buffer(&mut self) -> CommandBuffer {
    self.buffer.drain(..).collect()
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

  /// Add a replica connection mapping to the routing table and connection map.
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
      let commands = writer.graceful_close().await?;
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

  /// Check if the provided connection has any known replica nodes, and if so add them to the cached routing table.
  pub async fn check_replicas(
    &mut self,
    inner: &Arc<RedisClientInner>,
    primary: &mut RedisWriter,
  ) -> Result<(), RedisError> {
    // TODO need request_response on writer that uses the resp_tx
    let command = RedisCommand::new(RedisCommandKind::Info, vec!["replication".into()]);
    let frame = primary.request_response(inner, command).await?;
    let replicas = parse_info_replication(&primary.server, frame);
  }

  /// Send a command to one of the replicas associated with the provided primary server.
  pub async fn write_command(
    &mut self,
    inner: &Arc<RedisClientInner>,
    primary: &Server,
    mut command: RedisCommand,
  ) -> Result<Written, (RedisError, RedisCommand)> {
    let replica = match self.routing.next_replica(primary) {
      Some(replica) => replica.clone(),
      None => {
        return Err((
          RedisError::new(RedisErrorKind::Replica, "Missing replica node."),
          command,
        ))
      },
    };
    let writer = match self.writers.get_mut(&replica) {
      Some(writer) => writer,
      None => {
        return Err((
          RedisError::new(RedisErrorKind::Replica, "Missing replica node connection."),
          command,
        ))
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
    writer.push_command(command);
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

    unimplemented!()
  }
}

/// Parse the `INFO replication` response for replica node server identifiers.
fn parse_info_replication(primary: &Server, frame: String) -> HashMap<Server, Server> {
  let mut replicas = HashMap::new();
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

        if &parts[0] == "ip" {
          host = Some(parts[1].to_owned());
        } else if &parts[0] == "port" {
          port = parts[1].parse::<u16>().ok();
        }
      }

      if let Some(host) = host {
        if let Some(port) = port {
          replicas.insert(Server::new(host, port), primary.clone());
        }
      }
    }
  }

  replicas
}

#[cfg(all(feature = "replicas", any(feature = "enable-native-tls", feature = "enable-rustls")))]
pub fn map_replica_tls_names(
  inner: &Arc<RedisClientInner>,
  replicas: &mut HashMap<Server, Server>,
  default_host: &str,
) {
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

  let mut out = HashMap::with_capacity(replicas.len());
  for (mut replica, primary) in replicas.drain() {
    replica.set_tls_server_name(policy, default_host);
    out.insert(replica, primary);
  }
  *replicas = out;
}
