use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{ConnectionIDs, Connections};
use crate::protocol::command::RedisCommand;
use crate::protocol::command::RedisCommandKind;
use crate::protocol::connection;
use crate::protocol::connection::{FramedTcp, FramedTls, RedisTransport};
use crate::protocol::types::ProtocolFrame;
use crate::protocol::utils as protocol_utils;
use crate::types::{Resolve, TlsConnector};
use arcstr::ArcStr;
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::TlsStream as NativeTlsStream;
#[cfg(feature = "enable-rustls")]
use tokio_rustls::TlsStream as RustlsStream;

/// A struct wrapping a separate connection to the server or cluster for client or cluster management commands.
#[derive(Default)]
pub struct Backchannel {
  /// A connection to any of the servers.
  pub transport: Option<RedisTransport>,
  /// An identifier for the blocked connection, if any.
  pub blocked: Option<ArcStr>,
  /// A map of server IDs to connection IDs, as managed by the multiplexer.
  pub connection_ids: HashMap<ArcStr, i64>,
}

impl Backchannel {
  /// Check if the current server matches the provided server, and disconnect.
  pub async fn check_and_disconnect(&mut self, inner: &Arc<RedisClientInner>, server: Option<&ArcStr>) {
    let should_close = self
      .current_server()
      .map(|current| server.map(|server| server == current).unwrap_or(true))
      .unwrap_or(false);

    if should_close {
      if let Some(ref mut transport) = self.transport {
        let _ = transport.disconnect(inner).await;
      }
      self.transport = None;
    }
  }

  /// Set the connection IDs from the multiplexer.
  pub fn update_connection_ids(&mut self, connections: &Connections) {
    self.connection_ids = connections.connection_ids();
  }

  /// Read the connection ID for the provided server.
  pub fn connection_id(&self, server: &ArcStr) -> Option<i64> {
    self.connection_ids.get(server).cloned()
  }

  /// Set the blocked flag to the provided server.
  pub fn set_blocked(&mut self, server: &ArcStr) {
    self.blocked = Some(server.clone());
  }

  /// Remove the blocked flag.
  pub fn set_unblocked(&mut self) {
    self.blocked = None;
  }

  /// Remove the blocked flag only if the server matches the blocked server.
  pub fn check_and_set_unblocked(&mut self, server: &ArcStr) {
    let should_remove = self.blocked.as_ref().map(|blocked| blocked == server).unwrap_or(false);
    if should_remove {
      self.set_unblocked();
    }
  }

  /// Whether or not the client is blocked on a command.
  pub fn is_blocked(&self) -> bool {
    self.blocked.is_some()
  }

  /// Whether or not an open connection exists to the blocked server.
  pub fn has_blocked_transport(&self) -> bool {
    match self.blocked {
      Some(ref server) => match self.transport {
        Some(ref transport) => transport.server() == server,
        None => false,
      },
      None => false,
    }
  }

  /// Return the server ID of the blocked client connection, if found.
  pub fn blocked_server(&self) -> Option<ArcStr> {
    self.blocked.clone()
  }

  /// Return the server ID of the existing backchannel connection, if found.
  pub fn current_server(&self) -> Option<ArcStr> {
    self.transport.map(|t| t.server().clone())
  }

  /// Return a server ID, with the following preferences:
  ///
  /// 1. The server ID of the existing connection, if any.
  /// 2. The blocked server ID, if any.
  /// 3. A random server ID from the multiplexer's connection map.
  pub fn any_server(&self) -> Option<ArcStr> {
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

  /// Check if an existing connection can be used to the provided `server`, otherwise create a new one.
  ///
  /// Returns whether a new connection was created.
  pub async fn check_and_create_transport(
    &mut self,
    inner: &Arc<RedisClientInner>,
    server: &ArcStr,
  ) -> Result<bool, RedisError> {
    if let Some(ref mut transport) = self.transport {
      if transport.server() == server {
        if transport.ping(inner).await.is_ok() {
          _debug!(inner, "Using existing backchannel connection to {}", server);
          return Ok(false);
        }
      }
    }
    self.transport = None;

    let (host, port) = protocol_utils::server_to_parts(server)?;
    let transport = connection::create(inner, host.to_owned(), port, None).await?;
    self.transport = Some(transport);

    Ok(true)
  }

  /// Send the provided command to the provided server, creating a new connection if needed.
  ///
  /// If a new connection is created this function also sets it on `self` before returning.
  pub async fn request_response(
    &mut self,
    inner: &Arc<RedisClientInner>,
    server: &ArcStr,
    command: RedisCommand,
  ) -> Result<Resp3Frame, RedisError> {
    let _ = self.check_and_create_transport(inner, server).await?;

    if let Some(ref mut transport) = self.transport {
      _debug!(inner, "Sending {} on backchannel to {}", command.to_debug_str(), server);
      transport.request_response(command, inner.is_resp3()).await
    } else {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Failed to create backchannel connection.",
      ))
    }
  }
}
