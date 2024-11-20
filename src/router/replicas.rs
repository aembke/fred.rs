#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::types::TlsHostMapping;
use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{command::RedisCommand, connection, connection::RedisConnection},
  router::{utils, utils::next_frame, WriteResult},
  runtime::RefCount,
  types::{Resp3Frame, Server},
  utils as client_utils,
};
use futures::future::{join_all, select_all};
use std::{
  collections::{HashMap, VecDeque},
  fmt,
  fmt::Formatter,
  future::pending,
};

/// An interface used to filter the list of available replica nodes.
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
  pub filter:                     Option<RefCount<dyn ReplicaFilter>>,
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

impl PartialEq for ReplicaConfig {
  fn eq(&self, other: &Self) -> bool {
    self.lazy_connections == other.lazy_connections
      && self.ignore_reconnection_errors == other.ignore_reconnection_errors
      && self.connection_error_count == other.connection_error_count
      && self.primary_fallback == other.primary_fallback
  }
}

impl Eq for ReplicaConfig {}

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

/// A container for round-robin routing among replica nodes.
// This implementation optimizes for next() at the cost of add() and remove()
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
pub struct ReplicaRouter {
  counter: usize,
  servers: Vec<Server>,
}

impl ReplicaRouter {
  /// Read the server that should receive the next command.
  pub fn next(&mut self) -> Option<&Server> {
    self.counter = (self.counter + 1) % self.servers.len();
    self.servers.get(self.counter)
  }

  /// Conditionally add the server to the replica set.
  pub fn add(&mut self, server: Server) {
    if !self.servers.contains(&server) {
      self.servers.push(server);
    }
  }

  /// Remove the server from the replica set.
  pub fn remove(&mut self, server: &Server) {
    self.servers = self.servers.drain(..).filter(|_server| server != _server).collect();
  }

  /// The size of the replica set.
  pub fn len(&self) -> usize {
    self.servers.len()
  }

  /// Iterate over the replica set.
  pub fn iter(&self) -> impl Iterator<Item = &Server> {
    self.servers.iter()
  }
}

/// A container for round-robin routing to replica servers.
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReplicaSet {
  /// A map of primary server IDs to a counter and set of replica server IDs.
  servers: HashMap<Server, ReplicaRouter>,
}

impl ReplicaSet {
  /// Create a new empty replica set.
  pub fn new() -> ReplicaSet {
    ReplicaSet {
      servers: HashMap::new(),
    }
  }

  /// Add a replica node to the routing table.
  pub fn add(&mut self, primary: Server, replica: Server) {
    self.servers.entry(primary).or_default().add(replica);
  }

  /// Remove a replica node mapping from the routing table.
  pub fn remove(&mut self, primary: &Server, replica: &Server) {
    let should_remove = if let Some(router) = self.servers.get_mut(primary) {
      router.remove(replica);
      router.len() == 0
    } else {
      false
    };

    if should_remove {
      self.servers.remove(primary);
    }
  }

  /// Remove the replica from all routing sets.
  pub fn remove_replica(&mut self, replica: &Server) {
    self.servers = self
      .servers
      .drain()
      .filter_map(|(primary, mut routing)| {
        routing.remove(replica);

        if routing.len() > 0 {
          Some((primary, routing))
        } else {
          None
        }
      })
      .collect();
  }

  /// Read the server ID of the next replica that should receive a command.
  pub fn next_replica(&mut self, primary: &Server) -> Option<&Server> {
    self.servers.get_mut(primary).and_then(|router| router.next())
  }

  /// Read all the replicas associated with the provided primary node.
  pub fn replicas(&self, primary: &Server) -> impl Iterator<Item = &Server> {
    self
      .servers
      .get(primary)
      .map(|router| router.iter())
      .into_iter()
      .flatten()
  }

  /// Return a map of replica nodes to primary nodes.
  pub fn to_map(&self) -> HashMap<Server, Server> {
    let mut out = HashMap::with_capacity(self.servers.len());
    for (primary, replicas) in self.servers.iter() {
      for replica in replicas.iter() {
        out.insert(replica.clone(), primary.clone());
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
  pub(crate) writers: HashMap<Server, RedisConnection>,
  routing:            ReplicaSet,
  buffer:             VecDeque<RedisCommand>,
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

  /// Sync the connection map in place based on the cached routing table.
  pub async fn sync_connections(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    for (_, writer) in self.writers.drain() {
      let commands = writer.close().await;
      self.buffer.extend(commands);
    }

    for (replica, primary) in self.routing.to_map() {
      self.add_connection(inner, primary, replica, false).await?;
    }

    Ok(())
  }

  /// Drop all connections and clear the cached routing table.
  pub async fn clear_connections(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    self.routing.clear();
    self.sync_connections(inner).await
  }

  /// Clear the cached routing table without dropping connections.
  pub fn clear_routing(&mut self) {
    self.routing.clear();
  }

  /// Connect to the replica and add it to the cached routing table.
  pub async fn add_connection(
    &mut self,
    inner: &RefCount<RedisClientInner>,
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

    if !inner.connection.replica.lazy_connections || force {
      let mut transport = connection::create(inner, &replica, None).await?;
      transport.setup(inner, None).await?;

      if inner.config.server.is_clustered() {
        transport.readonly(inner, None).await?;
      };
      self.writers.insert(replica.clone(), transport.into_pipelined(true));
    }

    self.routing.add(primary, replica);
    Ok(())
  }

  /// Drop the socket associated with the provided server.
  pub async fn drop_writer(&mut self, replica: &Server) {
    if let Some(writer) = self.writers.remove(replica) {
      let commands = writer.close().await;
      self.buffer.extend(commands);
    }
  }

  /// Remove the replica from the routing table.
  pub fn remove_replica(&mut self, replica: &Server) {
    self.routing.remove_replica(replica);
  }

  /// Close the replica connection and optionally remove the replica from the routing table.
  pub async fn remove_connection(
    &mut self,
    inner: &RefCount<RedisClientInner>,
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
    self.drop_writer(replica).await;

    if !keep_routable {
      self.routing.remove(primary, replica);
    }
    Ok(())
  }

  /// Check and flush all the sockets managed by the replica routing state.
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    for (_, writer) in self.writers.iter_mut() {
      writer.flush().await?;
    }

    Ok(())
  }

  /// Whether a working connection exists to any replica for the provided primary node.
  pub async fn has_replica_connection(&mut self, primary: &Server) -> bool {
    for replica in self.routing.replicas(primary) {
      if let Some(replica) = self.writers.get_mut(replica) {
        if replica.peek_reader_errors().await.is_some() {
          continue;
        } else {
          return true;
        }
      } else {
        continue;
      }
    }

    false
  }

  /// Return a map of `replica` -> `primary` server identifiers.
  pub fn routing_table(&self) -> HashMap<Server, Server> {
    self.routing.to_map()
  }

  /// Check the active connections and drop any without a working reader task.
  pub async fn drop_broken_connections(&mut self) {
    let mut new_writers = HashMap::with_capacity(self.writers.len());
    for (server, mut writer) in self.writers.drain() {
      if writer.peek_reader_errors().await.is_some() {
        self.buffer.extend(writer.close().await);
        self.routing.remove_replica(&server);
      } else {
        new_writers.insert(server, writer);
      }
    }

    self.writers = new_writers;
  }

  /// Read the set of all active connections.
  pub async fn active_connections(&mut self) -> Vec<Server> {
    join_all(self.writers.iter_mut().map(|(server, conn)| async move {
      if conn.peek_reader_errors().await.is_some() {
        None
      } else {
        Some(server.clone())
      }
    }))
    .await
    .into_iter()
    .flatten()
    .collect()
  }

  /// Send a command to one of the replicas associated with the provided primary server.
  pub async fn write(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    primary: &Server,
    mut command: RedisCommand,
    force_flush: bool,
  ) -> WriteResult {
    let replica = match command.cluster_node {
      Some(ref server) => server.clone(),
      None => match self.routing.next_replica(primary) {
        Some(replica) => replica.clone(),
        None => {
          // we do not know of any replica node associated with the primary node
          return if inner.connection.replica.primary_fallback {
            WriteResult::Fallback(command)
          } else {
            command.respond_to_caller(Err(RedisError::new(RedisErrorKind::Replica, "Missing replica node.")));
            WriteResult::Ignore
          };
        },
      },
    };

    _trace!(
      inner,
      "Found replica {} (primary: {}) for {} ({})",
      replica,
      primary,
      command.kind.to_str_debug(),
      command.debug_id()
    );

    let writer = match self.writers.get_mut(&replica) {
      Some(writer) => writer,
      None => {
        // these errors indicate that we know a replica node should exist, but we are not connected or cannot
        // connect to it. in this case we want to hide the error, trigger a reconnect, and retry the command later.
        if inner.connection.replica.lazy_connections {
          _debug!(inner, "Lazily adding {} replica connection", replica);
          if let Err(e) = self.add_connection(inner, primary.clone(), replica.clone(), true).await {
            // we tried connecting once but failed.
            self.routing.remove_replica(&replica);
            // since we didn't get to actually send the command
            command.attempts_remaining += 1;
            return WriteResult::Disconnected((Some(replica.clone()), Some(command), e));
          }

          match self.writers.get_mut(&replica) {
            Some(writer) => writer,
            None => {
              self.routing.remove_replica(&replica);
              // the connection should be here if self.add_connection succeeded
              return WriteResult::Disconnected((
                Some(replica.clone()),
                Some(command),
                RedisError::new(RedisErrorKind::Replica, "Missing connection."),
              ));
            },
          }
        } else {
          // we don't have a connection to the replica, and we're not configured to lazily create new ones
          return WriteResult::NotFound(command);
        }
      },
    };
    let (frame, should_flush) = match utils::prepare_command(inner, &writer.counters, &mut command) {
      Ok((frame, should_flush)) => (frame, should_flush || force_flush),
      Err(e) => {
        _warn!(inner, "Frame encoding error for {}", command.kind.to_str_debug());
        // do not retry commands that trigger frame encoding errors
        command.respond_to_caller(Err(e));
        return WriteResult::Ignore;
      },
    };

    let blocks_connection = command.blocks_connection();
    _debug!(
      inner,
      "Sending {} ({}) to replica {}",
      command.kind.to_str_debug(),
      command.debug_id(),
      replica
    );
    command.write_attempts += 1;

    // TODO is this still necessary in the write path?
    // if let Some(error) = writer.peek_reader_errors().await {
    // _debug!(
    // inner,
    // "Error sending replica command {}: {:?}",
    // command.kind.to_str_debug(),
    // error
    // );
    // self.routing.remove_replica(&writer.server);
    // return WriteResult::Disconnected((Some(writer.server.clone()), Some(command), error));
    // }

    let check_unresponsive = !command.kind.is_pubsub() && inner.has_unresponsive_duration();
    writer.push_command(command);
    let write_result = writer.write(frame, should_flush, false, check_unresponsive).await;

    if let Err(err) = write_result {
      self.routing.remove_replica(&writer.server);
      WriteResult::Disconnected((Some(writer.server.clone()), None, err))
    } else {
      if blocks_connection {
        inner.backchannel.write().await.set_blocked(&writer.server);
      }
      WriteResult::Sent((writer.server.clone(), should_flush))
    }
  }

  /// Take the commands stored for retry later.
  pub fn take_retry_buffer(&mut self) -> VecDeque<RedisCommand> {
    self.buffer.drain(..).collect()
  }

  pub async fn drain(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    // let inner = inner.clone();
    let _ = join_all(self.writers.iter_mut().map(|(_, conn)| conn.drain(inner)))
      .await
      .into_iter()
      .collect::<Result<Vec<()>, RedisError>>()?;

    Ok(())
  }

  /// Try to read from all sockets concurrently, returning the first result that completes.
  pub async fn select_read(
    &mut self,
    inner: &RefCount<RedisClientInner>,
  ) -> Option<(Server, Result<Resp3Frame, RedisError>)> {
    if self.writers.is_empty() {
      // this is used in the context of select!, so we want to wait rather than break early.
      pending::<()>().await;
      return None;
    }

    select_all(self.writers.iter_mut().map(|(_, conn)| {
      Box::pin(async {
        match next_frame!(inner, conn) {
          Ok(Some(frame)) => Some((conn.server.clone(), Ok(frame))),
          Ok(None) => None,
          Err(e) => Some((conn.server.clone(), Err(e))),
        }
      })
    }))
    .await
    .0
  }
}

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
pub fn map_replica_tls_names(inner: &RefCount<RedisClientInner>, primary: &Server, replica: &mut Server) {
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

  replica.set_tls_server_name(policy, &primary.host);
}

#[cfg(not(any(feature = "enable-native-tls", feature = "enable-rustls")))]
pub fn map_replica_tls_names(_: &RefCount<RedisClientInner>, _: &Server, _: &mut Server) {}
