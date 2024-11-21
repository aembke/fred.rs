use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{command::RedisCommand, connection, connection::ExclusiveConnection, types::Server},
  router::connections::Connections,
  runtime::{AsyncRwLock, RefCount},
  utils,
};
use parking_lot::Mutex;
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::collections::HashMap;

/// Check if an existing connection can be used to the provided `server`, otherwise create a new one.
///
/// Returns whether a new connection was created.
async fn check_and_create_transport(
  backchannel: &mut Backchannel,
  inner: &RefCount<RedisClientInner>,
  server: &Server,
) -> Result<bool, RedisError> {
  if let Some(ref mut transport) = backchannel.transport {
    if &transport.server == server && transport.ping(inner).await.is_ok() {
      _debug!(inner, "Using existing backchannel connection to {}", server);
      return Ok(false);
    }
  }
  backchannel.transport = None;

  let mut transport = connection::create(inner, server, None).await?;
  transport.setup(inner, None).await?;
  backchannel.transport = Some(transport);

  Ok(true)
}

/// A struct wrapping a separate connection to the server or cluster for client or cluster management commands.
#[derive(Default)]
pub struct Backchannel {
  /// A connection to any of the servers.
  pub transport:      AsyncRwLock<Option<ExclusiveConnection>>,
  /// An identifier for the blocked connection, if any.
  pub blocked:        Mutex<Option<Server>>,
  /// A map of server IDs to connection IDs, as managed by the router.
  pub connection_ids: Mutex<HashMap<Server, i64>>,
}

impl Backchannel {
  /// Check if the current server matches the provided server, and disconnect.
  // TODO does this need to disconnect whenever the caller manually changes the RESP protocol mode?
  pub async fn check_and_disconnect(&mut self, inner: &RefCount<RedisClientInner>, server: Option<&Server>) {
    let should_close = self
      .current_server()
      .map(|current| server.map(|server| *server == current).unwrap_or(true))
      .unwrap_or(false);

    if should_close {
      if let Some(ref mut transport) = self.transport {
        let _ = transport.disconnect(inner).await;
      }
      self.transport = None;
    }
  }

  /// Clear all local state that depends on the associated `Router` instance.
  pub async fn clear_router_state(&mut self, inner: &RefCount<RedisClientInner>) {
    self.connection_ids.clear();
    self.blocked = None;

    if let Some(ref mut transport) = self.transport {
      let _ = transport.disconnect(inner).await;
    }
    self.transport = None;
  }

  /// Set the connection IDs from the router.
  pub fn update_connection_ids(&mut self, connections: &Connections) {
    self.connection_ids = connections.connection_ids();
  }

  /// Remove the provided server from the connection ID map.
  pub fn remove_connection_id(&mut self, server: &Server) {
    self.connection_ids.get(server);
  }

  /// Read the connection ID for the provided server.
  pub fn connection_id(&self, server: &Server) -> Option<i64> {
    self.connection_ids.get(server).cloned()
  }

  /// Set the blocked flag to the provided server.
  pub fn set_blocked(&mut self, server: &Server) {
    self.blocked = Some(server.clone());
  }

  /// Remove the blocked flag.
  pub fn set_unblocked(&mut self) {
    self.blocked = None;
  }

  /// Remove the blocked flag only if the server matches the blocked server.
  pub fn check_and_set_unblocked(&mut self, server: &Server) {
    let should_remove = self.blocked.as_ref().map(|blocked| blocked == server).unwrap_or(false);
    if should_remove {
      self.set_unblocked();
    }
  }

  /// Whether the client is blocked on a command.
  pub fn is_blocked(&self) -> bool {
    self.blocked.is_some()
  }

  /// Whether an open connection exists to the blocked server.
  pub fn has_blocked_transport(&self) -> bool {
    match self.blocked {
      Some(ref server) => match self.transport {
        Some(ref transport) => &transport.server == server,
        None => false,
      },
      None => false,
    }
  }

  /// Return the server ID of the blocked client connection, if found.
  pub fn blocked_server(&self) -> Option<Server> {
    self.blocked.clone()
  }

  /// Return the server ID of the existing backchannel connection, if found.
  pub fn current_server(&self) -> Option<Server> {
    self.transport.as_ref().map(|t| t.server.clone())
  }

  /// Return a server ID, with the following preferences:
  ///
  /// 1. The server ID of the existing connection, if any.
  /// 2. The blocked server ID, if any.
  /// 3. A random server ID from the router's connection map.
  pub fn any_server(&self) -> Option<Server> {
    self
      .current_server()
      .or(self.blocked_server())
      .or(self.connection_ids.keys().next().cloned())
  }

  /// Whether the existing connection is to the currently blocked server.
  pub fn current_server_is_blocked(&self) -> bool {
    self
      .current_server()
      .and_then(|server| self.blocked_server().map(|blocked| server == blocked))
      .unwrap_or(false)
  }

  /// Send the provided command to the provided server, creating a new connection if needed.
  ///
  /// If a new connection is created this function also sets it on `self` before returning.
  pub async fn request_response(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    server: &Server,
    command: RedisCommand,
  ) -> Result<Resp3Frame, RedisError> {
    let _ = check_and_create_transport(self, inner, server).await?;

    if let Some(ref mut transport) = self.transport {
      _debug!(
        inner,
        "Sending {} ({}) on backchannel to {}",
        command.kind.to_str_debug(),
        command.debug_id(),
        server
      );

      utils::timeout(
        transport.request_response(command, inner.is_resp3()),
        inner.connection_timeout(),
      )
      .await
    } else {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Failed to create backchannel connection.",
      ))
    }
  }

  /// Find the server identifier that should receive the provided command.
  ///
  /// Servers are chosen with the following preference order:
  ///
  /// * If `use_blocked` is true and a connection is blocked then that server will be used.
  /// * If the client is clustered and the command uses a hashing policy that specifies a specific server then that
  ///   will be used.
  /// * If a backchannel connection already exists then that will be used.
  /// * Failing all of the above a random server will be used.
  pub fn find_server(
    &self,
    inner: &RefCount<RedisClientInner>,
    command: &RedisCommand,
    use_blocked: bool,
  ) -> Result<Server, RedisError> {
    if use_blocked {
      if let Some(server) = self.blocked.as_ref() {
        Ok(server.clone())
      } else {
        // should this be more relaxed?
        Err(RedisError::new(RedisErrorKind::Unknown, "No connections are blocked."))
      }
    } else if inner.config.server.is_clustered() {
      if command.kind.use_random_cluster_node() {
        self
          .any_server()
          .ok_or_else(|| RedisError::new(RedisErrorKind::Unknown, "Failed to find backchannel server."))
      } else {
        inner.with_cluster_state(|state| {
          let slot = match command.cluster_hash() {
            Some(slot) => slot,
            None => {
              return Err(RedisError::new(
                RedisErrorKind::Cluster,
                "Failed to find cluster hash slot.",
              ))
            },
          };
          state
            .get_server(slot)
            .cloned()
            .ok_or_else(|| RedisError::new(RedisErrorKind::Cluster, "Failed to find cluster owner."))
        })
      }
    } else {
      self
        .any_server()
        .ok_or_else(|| RedisError::new(RedisErrorKind::Unknown, "Failed to find backchannel server."))
    }
  }
}
