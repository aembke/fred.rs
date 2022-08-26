use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{ConnectionIDs, Connections};
use crate::protocol::command::RedisCommand;
use crate::protocol::command::RedisCommandKind;
use crate::protocol::connection;
use crate::protocol::connection::{FramedTcp, FramedTls, RedisTransport};
use crate::protocol::types::ProtocolFrame;
use crate::protocol::utils as protocol_utils;
use crate::types::Resolve;
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

/// Wrapper type to hide generic connection type parameters.
pub enum BackchannelTransport {
  Tcp(RedisTransport<TcpStream>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(RedisTransport<NativeTlsStream<TcpStream>>),
  #[cfg(feature = "enable-rustls")]
  Rustls(RedisTransport<RustlsStream<TcpStream>>),
}

impl BackchannelTransport {
  pub async fn setup(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    match self {
      BackchannelTransport::Tcp(t) => t.setup(inner).await,
      #[cfg(feature = "enable-native-tls")]
      BackchannelTransport::NativeTls(t) => t.setup(inner).await,
      #[cfg(feature = "enable-rustls")]
      BackchannelTransport::Rustls(t) => t.setup(inner).await,
    }
  }

  pub async fn request_response(&mut self, cmd: RedisCommand, is_resp3: bool) -> Result<Resp3Frame, RedisError> {
    match self {
      BackchannelTransport::Tcp(t) => t.request_response(cmd, is_resp3).await.map(|f| f.into_resp3()),
      #[cfg(feature = "enable-native-tls")]
      BackchannelTransport::NativeTls(t) => t.request_response(cmd, is_resp3).await.map(|f| f.into_resp3()),
      #[cfg(feature = "enable-rustls")]
      BackchannelTransport::Rustls(t) => t.request_response(cmd, is_resp3).await.map(|f| f.into_resp3()),
    }
  }

  pub async fn ping(&mut self, is_resp3: bool) -> Result<(), RedisError> {
    let cmd = RedisCommand::new(RedisCommandKind::Ping, vec![], None);
    self.request_response(cmd, is_resp3).await.map(|_| ())
  }

  pub fn server(&self) -> &ArcStr {
    match self {
      BackchannelTransport::Tcp(t) => &t.server,
      #[cfg(feature = "enable-native-tls")]
      BackchannelTransport::NativeTls(t) => &t.server,
      #[cfg(feature = "enable-rustls")]
      BackchannelTransport::Rustls(t) => &t.server,
    }
  }
}

/// A struct wrapping a separate connection to the server or cluster for client or cluster management commands.
#[derive(Default)]
pub struct Backchannel {
  /// A connection to any of the servers.
  pub transport: Option<BackchannelTransport>,
  /// An identifier for the blocked connection, if any.
  pub blocked: Option<ArcStr>,
  /// A map of server IDs to connection IDs, as managed by the multiplexer.
  pub connection_ids: HashMap<ArcStr, i64>,
}

impl Backchannel {
  /// Set the connection IDs from the multiplexer.
  pub fn update_connection_ids<T>(&mut self, connections: Connections<T>)
  where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
  {
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
  /// 1. The blocked server ID, if any.
  /// 2. The server ID of the existing connection, if any.
  /// 3. A random server ID from the multiplexer's connection map.
  pub fn any_server(&self) -> Option<ArcStr> {
    self
      .blocked_server()
      .or(self.current_server())
      .or(self.connection_ids.keys().next().cloned())
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
        if transport.ping(inner.is_resp3()).await.is_ok() {
          _debug!(inner, "Using existing backchannel connection to {}", server);
          return Ok(false);
        }
      }
    }
    self.transport = None;

    let (host, port) = protocol_utils::server_to_parts(server)?;
    if inner.config.uses_native_tls() {
      _debug!(inner, "Creating backchannel native-tls connection to {}:{}", host, port);
      let mut transport = RedisTransport::new_native_tls(inner, host.to_owned(), port).await?;
      let _ = transport.setup(inner).await?;
      self.transport = Some(BackchannelTransport::NativeTls(transport));
    } else if inner.config.uses_rustls() {
      _debug!(inner, "Creating backchannel rustls connection to {}:{}", host, port);
      let mut transport = RedisTransport::new_rustls(inner, host.to_owned(), port).await?;
      let _ = transport.setup(inner).await?;
      self.transport = Some(BackchannelTransport::Rustls(transport));
    } else {
      _debug!(inner, "Creating backchannel TCP connection to {}:{}", host, port);
      let mut transport = RedisTransport::new_tcp(inner, host.to_owned(), port).await?;
      let _ = transport.setup(inner).await?;
      self.transport = Some(BackchannelTransport::Tcp(transport));
    }

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
