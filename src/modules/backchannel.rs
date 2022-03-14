use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::ConnectionIDs;
use crate::protocol::connection;
use crate::protocol::connection::{FramedTcp, FramedTls, RedisTransport};
use crate::protocol::types::{ProtocolFrame, RedisCommand};
use crate::protocol::utils as protocol_utils;
use crate::types::Resolve;
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::sync::Arc;

async fn create_transport(
  inner: &Arc<RedisClientInner>,
  host: &str,
  port: u16,
  tls: bool,
) -> Result<RedisTransport, RedisError> {
  let addr = inner.resolver.resolve(host.to_owned(), port).await?;

  let transport = if tls {
    let transport = connection::create_authenticated_connection_tls(&addr, host, inner).await?;
    RedisTransport::Tls(transport)
  } else {
    let transport = connection::create_authenticated_connection(&addr, inner).await?;
    RedisTransport::Tcp(transport)
  };

  Ok(transport)
}

fn map_tcp_response(
  result: Result<(ProtocolFrame, FramedTcp), (RedisError, FramedTcp)>,
) -> Result<(Resp3Frame, RedisTransport), (RedisError, RedisTransport)> {
  result
    .map(|(f, t)| (f.into_resp3(), RedisTransport::Tcp(t)))
    .map_err(|(e, t)| (e, RedisTransport::Tcp(t)))
}

fn map_tls_response(
  result: Result<(ProtocolFrame, FramedTls), (RedisError, FramedTls)>,
) -> Result<(Resp3Frame, RedisTransport), (RedisError, RedisTransport)> {
  result
    .map(|(f, t)| (f.into_resp3(), RedisTransport::Tls(t)))
    .map_err(|(e, t)| (e, RedisTransport::Tls(t)))
}

/// A struct that allows for a backchannel to the server(s) even when the connections are blocked.
#[derive(Default)]
pub struct Backchannel {
  /// A connection to any of the servers, with the associated server name.
  pub transport: Option<(RedisTransport, Arc<String>)>,
  /// The server (host/port) that is blocked, if any.
  pub blocked: Option<Arc<String>>,
  /// A shared mapping of server IDs to connection IDs.
  pub connection_ids: Option<ConnectionIDs>,
}

impl Backchannel {
  /// Set the connection IDs from the multiplexer.
  pub fn set_connection_ids(&mut self, connection_ids: ConnectionIDs) {
    self.connection_ids = Some(connection_ids);
  }

  /// Read the connection ID for the provided server.
  pub fn connection_id(&self, server: &Arc<String>) -> Option<i64> {
    self
      .connection_ids
      .as_ref()
      .and_then(|connection_ids| match connection_ids {
        ConnectionIDs::Centralized(ref inner) => inner.read().clone(),
        ConnectionIDs::Clustered(ref inner) => inner.read().get(server).map(|i| *i),
      })
  }

  /// Set the blocked flag to the provided server.
  pub fn set_blocked(&mut self, server: Arc<String>) {
    self.blocked = Some(server);
  }

  /// Remove the blocked flag.
  pub fn set_unblocked(&mut self) {
    self.blocked = None;
  }

  /// Whether or not the client is blocked on a command.
  pub fn is_blocked(&self) -> bool {
    self.blocked.is_some()
  }

  /// Whether or not an open transport exists to the blocked server.
  pub fn has_blocked_transport(&self) -> bool {
    match self.blocked {
      Some(ref server) => match self.transport {
        Some((_, ref _server)) => server == _server,
        None => false,
      },
      None => false,
    }
  }

  /// Take the current transport or create a new one, returning the new transport, the server name, and whether a new connection was needed.
  pub async fn take_or_create_transport(
    &mut self,
    inner: &Arc<RedisClientInner>,
    host: &str,
    port: u16,
    uses_tls: bool,
    use_blocked: bool,
  ) -> Result<(RedisTransport, Option<Arc<String>>, bool), RedisError> {
    if self.has_blocked_transport() && use_blocked {
      if let Some((transport, server)) = self.transport.take() {
        Ok((transport, Some(server), false))
      } else {
        _debug!(inner, "Creating backchannel to {}:{}", host, port);
        let transport = create_transport(inner, host, port, uses_tls).await?;
        Ok((transport, None, true))
      }
    } else {
      let _ = self.transport.take();
      _debug!(inner, "Creating backchannel to {}:{}", host, port);

      let transport = create_transport(inner, host, port, uses_tls).await?;
      Ok((transport, None, true))
    }
  }

  /// Send the provided command to the server at `host:port`.
  ///
  /// If an existing transport to the provided server is found this function will try to use it, but will automatically retry once if the connection is dead.
  /// If a new transport has to be created this function will create it, use it, and set it on `self` if the command succeeds.
  pub async fn request_response(
    &mut self,
    inner: &Arc<RedisClientInner>,
    server: &Arc<String>,
    command: RedisCommand,
    use_blocked: bool,
  ) -> Result<Resp3Frame, RedisError> {
    let is_resp3 = inner.is_resp3();
    let uses_tls = inner.config.read().uses_tls();
    let (host, port) = protocol_utils::server_to_parts(server)?;

    let (transport, _server, try_once) = self
      .take_or_create_transport(inner, host, port, uses_tls, use_blocked)
      .await?;
    let server = _server.unwrap_or(server.clone());
    let result = match transport {
      RedisTransport::Tcp(transport) => {
        map_tcp_response(connection::request_response_safe(transport, &command, is_resp3).await)
      }
      RedisTransport::Tls(transport) => {
        map_tls_response(connection::request_response_safe(transport, &command, is_resp3).await)
      }
    };

    match result {
      Ok((frame, transport)) => {
        _debug!(inner, "Created backchannel to {}", server);
        self.transport = Some((transport, server));
        Ok(frame)
      }
      Err((e, _)) => {
        if try_once {
          _warn!(inner, "Failed to create backchannel to {}", server);
          Err(e)
        } else {
          // need to avoid async recursion
          let (transport, _, _) = self
            .take_or_create_transport(inner, host, port, uses_tls, use_blocked)
            .await?;
          let result = match transport {
            RedisTransport::Tcp(transport) => {
              map_tcp_response(connection::request_response_safe(transport, &command, is_resp3).await)
            }
            RedisTransport::Tls(transport) => {
              map_tls_response(connection::request_response_safe(transport, &command, is_resp3).await)
            }
          };

          match result {
            Ok((frame, transport)) => {
              self.transport = Some((transport, server));
              Ok(frame)
            }
            Err((e, _)) => Err(e),
          }
        }
      }
    }
  }
}
