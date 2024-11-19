use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    codec::RedisCodec,
    command::{RedisCommand, RedisCommandKind},
    types::{ProtocolFrame, Server},
    utils as protocol_utils,
  },
  router::{centralized, clustered},
  runtime::{AtomicUsize, RefCount},
  types::InfoKind,
  utils as client_utils,
  utils,
};
use bytes_utils::Str;
use futures::{
  sink::SinkExt,
  stream::{Peekable, StreamExt},
  Sink,
  Stream,
};
use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, Resp3Frame as _Resp3Frame, RespVersion};
use semver::Version;
use std::{
  collections::VecDeque,
  fmt,
  net::SocketAddr,
  pin::Pin,
  str,
  task::{Context, Poll},
  time::{Duration, Instant},
};
use tokio_util::codec::Framed;

#[cfg(not(feature = "glommio"))]
use socket2::SockRef;

#[cfg(feature = "glommio")]
use glommio::net::TcpStream as BaseTcpStream;
#[cfg(feature = "glommio")]
pub type TcpStream = crate::runtime::glommio::io_compat::TokioIO<BaseTcpStream>;

#[cfg(not(feature = "glommio"))]
use tokio::net::TcpStream;
#[cfg(not(feature = "glommio"))]
use tokio::net::TcpStream as BaseTcpStream;

#[cfg(feature = "unix-sockets")]
use crate::prelude::ServerConfig;
#[cfg(any(
  feature = "enable-native-tls",
  feature = "enable-rustls",
  feature = "enable-rustls-ring"
))]
use crate::protocol::tls::TlsConnector;
use crate::router::responses;
#[cfg(feature = "replicas")]
use crate::types::RedisValue;
#[cfg(feature = "unix-sockets")]
use std::path::Path;
#[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
use std::{convert::TryInto, ops::Deref};
#[cfg(feature = "unix-sockets")]
use tokio::net::UnixStream;
#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::TlsStream as NativeTlsStream;
#[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
use tokio_rustls::client::TlsStream as RustlsStream;

/// The contents of a simplestring OK response.
pub const OK: &str = "OK";
/// The timeout duration used when dropping the split sink and waiting on the split stream to close.
pub const CONNECTION_CLOSE_TIMEOUT_MS: u64 = 5_000;
pub const INITIAL_BUFFER_SIZE: usize = 64;

/// Connect to each socket addr and return the first successful connection.
async fn tcp_connect_any(
  inner: &RefCount<RedisClientInner>,
  server: &Server,
  addrs: &Vec<SocketAddr>,
) -> Result<(TcpStream, SocketAddr), RedisError> {
  let mut last_error: Option<RedisError> = None;

  for addr in addrs.iter() {
    _debug!(
      inner,
      "Creating TCP connection to {} at {}:{}",
      server.host,
      addr.ip(),
      addr.port()
    );
    let socket = match BaseTcpStream::connect(addr).await {
      Ok(socket) => socket,
      Err(e) => {
        _debug!(inner, "Error connecting to {}: {:?}", addr, e);
        last_error = Some(e.into());
        continue;
      },
    };
    if let Some(val) = inner.connection.tcp.nodelay {
      socket.set_nodelay(val)?;
    }
    if let Some(_dur) = inner.connection.tcp.linger {
      #[cfg(not(feature = "glommio"))]
      socket.set_linger(Some(_dur))?;
      #[cfg(feature = "glommio")]
      _warn!(inner, "TCP Linger is not yet supported with Glommio features.");
    }
    if let Some(ttl) = inner.connection.tcp.ttl {
      socket.set_ttl(ttl)?;
    }
    if let Some(ref _keepalive) = inner.connection.tcp.keepalive {
      #[cfg(not(feature = "glommio"))]
      SockRef::from(&socket).set_tcp_keepalive(_keepalive)?;
      #[cfg(feature = "glommio")]
      _warn!(inner, "TCP keepalive is not yet supported with Glommio features.");
    }

    #[cfg(feature = "glommio")]
    let socket = crate::runtime::glommio::io_compat::TokioIO(socket);
    return Ok((socket, *addr));
  }

  _trace!(inner, "Failed to connect to any of {:?}.", addrs);
  Err(last_error.unwrap_or(RedisError::new(RedisErrorKind::IO, "Failed to connect.")))
}

pub enum ConnectionKind {
  Tcp(Peekable<Framed<TcpStream, RedisCodec>>),
  #[cfg(feature = "unix-sockets")]
  Unix(Peekable<Framed<UnixStream, RedisCodec>>),
  #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
  Rustls(Peekable<Framed<RustlsStream<TcpStream>, RedisCodec>>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(Peekable<Framed<NativeTlsStream<TcpStream>, RedisCodec>>),
}

impl Stream for ConnectionKind {
  type Item = Result<ProtocolFrame, RedisError>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self.get_mut() {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_next(cx),
    }
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    match self {
      ConnectionKind::Tcp(ref conn) => conn.size_hint(),
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref conn) => conn.size_hint(),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref conn) => conn.size_hint(),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref conn) => conn.size_hint(),
    }
  }
}

impl Sink<ProtocolFrame> for ConnectionKind {
  type Error = RedisError;

  fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self.get_mut() {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_ready(cx),
    }
  }

  fn start_send(self: Pin<&mut Self>, item: ProtocolFrame) -> Result<(), Self::Error> {
    match self.get_mut() {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).start_send(item),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self.get_mut() {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self.get_mut() {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
    }
  }
}

/// Atomic counters stored with connection state.
#[derive(Clone, Debug)]
pub struct Counters {
  pub cmd_buffer_len: RefCount<AtomicUsize>,
  pub in_flight:      RefCount<AtomicUsize>,
  pub feed_count:     RefCount<AtomicUsize>,
}

impl Counters {
  pub fn new(cmd_buffer_len: &RefCount<AtomicUsize>) -> Self {
    Counters {
      cmd_buffer_len: cmd_buffer_len.clone(),
      in_flight:      RefCount::new(AtomicUsize::new(0)),
      feed_count:     RefCount::new(AtomicUsize::new(0)),
    }
  }

  /// Flush the sink if the max feed count is reached or no commands are queued following the current command.
  pub fn should_send(&self, inner: &RefCount<RedisClientInner>) -> bool {
    client_utils::read_atomic(&self.feed_count) as u64 > inner.max_feed_count()
      || client_utils::read_atomic(&self.cmd_buffer_len) == 0
  }

  pub fn incr_feed_count(&self) -> usize {
    client_utils::incr_atomic(&self.feed_count)
  }

  pub fn incr_in_flight(&self) -> usize {
    client_utils::incr_atomic(&self.in_flight)
  }

  pub fn decr_in_flight(&self) -> usize {
    client_utils::decr_atomic(&self.in_flight)
  }

  pub fn reset_feed_count(&self) {
    client_utils::set_atomic(&self.feed_count, 0);
  }

  pub fn reset_in_flight(&self) {
    client_utils::set_atomic(&self.in_flight, 0);
  }
}

/// A connection to Redis that is not auto-pipelined and cannot be shared across client tasks.
pub struct ExclusiveConnection {
  /// An identifier for the connection, usually `<host>|<ip>:<port>`.
  pub server:       Server,
  /// The parsed `SocketAddr` for the connection.
  pub addr:         Option<SocketAddr>,
  /// The hostname used to initialize the connection.
  pub default_host: Str,
  /// The network connection.
  pub transport:    ConnectionKind,
  /// The connection/client ID from the CLIENT ID command.
  pub id:           Option<i64>,
  /// The server version.
  pub version:      Option<Version>,
  /// Counters for the connection state.
  pub counters:     Counters,
}

impl ExclusiveConnection {
  pub async fn new_tcp(
    inner: &RefCount<RedisClientInner>,
    server: &Server,
  ) -> Result<ExclusiveConnection, RedisError> {
    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let default_host = server.host.clone();
    let codec = RedisCodec::new(inner, server);
    let addrs = inner
      .get_resolver()
      .await
      .resolve(server.host.clone(), server.port)
      .await?;
    let (socket, addr) = tcp_connect_any(inner, server, &addrs).await?;
    let transport = ConnectionKind::Tcp(Framed::new(socket, codec).peekable());

    Ok(ExclusiveConnection {
      server: server.clone(),
      addr: Some(addr),
      default_host,
      counters,
      id,
      version,
      transport,
    })
  }

  #[cfg(feature = "unix-sockets")]
  pub async fn new_unix(inner: &RefCount<RedisClientInner>, path: &Path) -> Result<ExclusiveConnection, RedisError> {
    _debug!(inner, "Connecting via unix socket to {}", utils::path_to_string(path));
    let server = Server::new(utils::path_to_string(path), 0);
    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let default_host = server.host.clone();
    let codec = RedisCodec::new(inner, &server);
    let socket = UnixStream::connect(path).await?;
    let transport = ConnectionKind::Unix(Framed::new(socket, codec).peekable());

    Ok(ExclusiveConnection {
      addr: None,
      server,
      default_host,
      counters,
      id,
      version,
      transport,
    })
  }

  #[cfg(feature = "enable-native-tls")]
  #[allow(unreachable_patterns)]
  pub async fn new_native_tls(
    inner: &RefCount<RedisClientInner>,
    server: &Server,
  ) -> Result<ExclusiveConnection, RedisError> {
    let connector = match inner.config.tls {
      Some(ref config) => match config.connector {
        TlsConnector::Native(ref connector) => connector.clone(),
        _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
      },
      None => return ExclusiveConnection::new_tcp(inner, server).await,
    };

    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let tls_server_name = server.tls_server_name.as_ref().cloned().unwrap_or(server.host.clone());

    let default_host = server.host.clone();
    let codec = RedisCodec::new(inner, server);
    let addrs = inner
      .get_resolver()
      .await
      .resolve(server.host.clone(), server.port)
      .await?;
    let (socket, addr) = tcp_connect_any(inner, server, &addrs).await?;

    _debug!(inner, "native-tls handshake with server name/host: {}", tls_server_name);
    let socket = connector.clone().connect(&tls_server_name, socket).await?;
    let transport = ConnectionKind::NativeTls(Framed::new(socket, codec).peekable());

    Ok(ExclusiveConnection {
      server: server.clone(),
      addr: Some(addr),
      default_host,
      counters,
      id,
      version,
      transport,
    })
  }

  #[cfg(not(feature = "enable-native-tls"))]
  pub async fn new_native_tls(
    inner: &RefCount<RedisClientInner>,
    server: &Server,
  ) -> Result<ExclusiveConnection, RedisError> {
    ExclusiveConnection::new_tcp(inner, server).await
  }

  #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
  #[allow(unreachable_patterns)]
  pub async fn new_rustls(
    inner: &RefCount<RedisClientInner>,
    server: &Server,
  ) -> Result<ExclusiveConnection, RedisError> {
    use rustls::pki_types::ServerName;

    let connector = match inner.config.tls {
      Some(ref config) => match config.connector {
        TlsConnector::Rustls(ref connector) => connector.clone(),
        _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
      },
      None => return ExclusiveConnection::new_tcp(inner, server).await,
    };

    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let tls_server_name = server.tls_server_name.as_ref().cloned().unwrap_or(server.host.clone());

    let default_host = server.host.clone();
    let codec = RedisCodec::new(inner, server);
    let addrs = inner
      .get_resolver()
      .await
      .resolve(server.host.clone(), server.port)
      .await?;
    let (socket, addr) = tcp_connect_any(inner, server, &addrs).await?;
    let server_name: ServerName = tls_server_name.deref().try_into()?;

    _debug!(inner, "rustls handshake with server name/host: {:?}", tls_server_name);
    let socket = connector.clone().connect(server_name.to_owned(), socket).await?;
    let transport = ConnectionKind::Rustls(Framed::new(socket, codec).peekable());

    Ok(ExclusiveConnection {
      server: server.clone(),
      addr: Some(addr),
      counters,
      default_host,
      id,
      version,
      transport,
    })
  }

  #[cfg(not(any(feature = "enable-rustls", feature = "enable-rustls-ring")))]
  pub async fn new_rustls(
    inner: &RefCount<RedisClientInner>,
    server: &Server,
  ) -> Result<ExclusiveConnection, RedisError> {
    ExclusiveConnection::new_tcp(inner, server).await
  }

  /// Send a command to the server.
  pub async fn request_response(&mut self, cmd: RedisCommand, is_resp3: bool) -> Result<Resp3Frame, RedisError> {
    let frame = cmd.to_frame(is_resp3)?;
    self.transport.send(frame).await?;

    match self.transport.next().await {
      Some(result) => result.map(|f| f.into_resp3()),
      None => Ok(Resp3Frame::Null),
    }
  }

  /// Set the client name with `CLIENT SETNAME`.
  pub async fn set_client_name(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    _debug!(inner, "Setting client name.");
    let name = &inner.id;
    let command = RedisCommand::new(RedisCommandKind::ClientSetname, vec![name.clone().into()]);
    let response = self.request_response(command, inner.is_resp3()).await?;

    if protocol_utils::is_ok(&response) {
      debug!("{}: Successfully set Redis client name.", name);
      Ok(())
    } else {
      error!("{} Failed to set client name with error {:?}", name, response);
      Err(RedisError::new(RedisErrorKind::Protocol, "Failed to set client name."))
    }
  }

  /// Read and cache the server version.
  pub async fn cache_server_version(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    let command = RedisCommand::new(RedisCommandKind::Info, vec![InfoKind::Server.to_str().into()]);
    let result = self.request_response(command, inner.is_resp3()).await?;
    let result = match result {
      Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.to_vec())?,
      Resp3Frame::BlobString { data, .. } | Resp3Frame::VerbatimString { data, .. } => {
        String::from_utf8(data.to_vec())?
      },
      Resp3Frame::SimpleError { data, .. } => {
        _warn!(inner, "Failed to read server version: {:?}", data);
        return Ok(());
      },
      Resp3Frame::BlobError { data, .. } => {
        let parsed = String::from_utf8_lossy(&data);
        _warn!(inner, "Failed to read server version: {:?}", parsed);
        return Ok(());
      },
      _ => {
        _warn!(inner, "Invalid INFO response: {:?}", result.kind());
        return Ok(());
      },
    };

    self.version = result.lines().find_map(|line| {
      let parts: Vec<&str> = line.split(':').collect();
      if parts.len() < 2 {
        return None;
      }

      if parts[0] == "redis_version" {
        Version::parse(parts[1]).ok()
      } else {
        None
      }
    });

    _debug!(inner, "Read server version {:?}", self.version);
    Ok(())
  }

  /// Authenticate via AUTH, then set the client name.
  pub async fn authenticate(
    &mut self,
    name: &str,
    username: Option<String>,
    password: Option<String>,
    is_resp3: bool,
  ) -> Result<(), RedisError> {
    if let Some(password) = password {
      let args = if let Some(username) = username {
        vec![username.into(), password.into()]
      } else {
        vec![password.into()]
      };
      let command = RedisCommand::new(RedisCommandKind::Auth, args);

      debug!("{}: Authenticating Redis client...", name);
      let frame = self.request_response(command, is_resp3).await?;

      if !protocol_utils::is_ok(&frame) {
        let error = protocol_utils::frame_into_string(frame)?;
        return Err(protocol_utils::pretty_error(&error));
      }
    } else {
      trace!("{}: Skip authentication without credentials.", name);
    }

    Ok(())
  }

  /// Authenticate via HELLO in RESP3 mode or AUTH in RESP2 mode, then set the client name.
  pub async fn switch_protocols_and_authenticate(
    &mut self,
    inner: &RefCount<RedisClientInner>,
  ) -> Result<(), RedisError> {
    // reset the protocol version to the one specified by the config when we create new connections
    inner.reset_protocol_version();
    let (username, password) = inner.read_credentials(&self.server).await?;

    if inner.is_resp3() {
      _debug!(inner, "Switching to RESP3 protocol with HELLO...");
      let args = if let Some(password) = password {
        if let Some(username) = username {
          vec![username.into(), password.into()]
        } else {
          vec!["default".into(), password.into()]
        }
      } else {
        vec![]
      };

      let cmd = RedisCommand::new(RedisCommandKind::_Hello(RespVersion::RESP3), args);
      let response = self.request_response(cmd, true).await?;
      let response = protocol_utils::frame_to_results(response)?;
      inner.switch_protocol_versions(RespVersion::RESP3);
      _trace!(inner, "Recv HELLO response {:?}", response);

      Ok(())
    } else {
      self.authenticate(&inner.id, username, password, false).await
    }
  }

  /// Read and cache the connection ID.
  pub async fn cache_connection_id(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    let command = (RedisCommandKind::ClientID, vec![]).into();
    let result = self.request_response(command, inner.is_resp3()).await;
    _debug!(inner, "Read client ID: {:?}", result);
    self.id = match result {
      Ok(Resp3Frame::Number { data, .. }) => Some(data),
      _ => None,
    };

    Ok(())
  }

  /// Send `PING` to the server.
  pub async fn ping(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    let command = RedisCommandKind::Ping.into();
    let response = self.request_response(command, inner.is_resp3()).await?;

    if let Some(e) = protocol_utils::frame_to_error(&response) {
      Err(e)
    } else {
      Ok(())
    }
  }

  /// Send `QUIT` and close the connection.
  pub async fn disconnect(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    if let Err(e) = self.transport.close().await {
      _warn!(inner, "Error closing connection to {}: {:?}", self.server, e);
    }
    Ok(())
  }

  /// Select the database provided in the `RedisConfig`.
  pub async fn select_database(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    if inner.config.server.is_clustered() {
      return Ok(());
    }

    let db = match inner.config.database {
      Some(db) => db,
      None => return Ok(()),
    };

    _trace!(inner, "Selecting database {} after connecting.", db);
    let command = RedisCommand::new(RedisCommandKind::Select, vec![(db as i64).into()]);
    let response = self.request_response(command, inner.is_resp3()).await?;

    if let Some(error) = protocol_utils::frame_to_error(&response) {
      Err(error)
    } else {
      Ok(())
    }
  }

  /// Check the `cluster_state` via `CLUSTER INFO`.
  ///
  /// Returns an error if the state is not `ok`.
  pub async fn check_cluster_state(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    if !inner.config.server.is_clustered() {
      return Ok(());
    }

    _trace!(inner, "Checking cluster info for {}", self.server);
    let command = RedisCommand::new(RedisCommandKind::ClusterInfo, vec![]);
    let response = self.request_response(command, inner.is_resp3()).await?;
    let response: String = protocol_utils::frame_to_results(response)?.convert()?;

    for line in response.lines() {
      let parts: Vec<&str> = line.split(':').collect();
      if parts.len() == 2 && parts[0] == "cluster_state" && parts[1] == "ok" {
        return Ok(());
      }
    }

    Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid or missing cluster state.",
    ))
  }

  /// Authenticate, set the protocol version, set the client name, select the provided database, cache the
  /// connection ID and server version, and check the cluster state (if applicable).
  pub async fn setup(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    timeout: Option<Duration>,
  ) -> Result<(), RedisError> {
    let timeout = timeout.unwrap_or(inner.internal_command_timeout());
    let has_credentials = inner.config.password.is_some() || inner.config.version == RespVersion::RESP3;
    #[cfg(feature = "credential-provider")]
    let has_credentials = has_credentials || inner.config.credential_provider.is_some();

    utils::timeout(
      async {
        if has_credentials {
          self.switch_protocols_and_authenticate(inner).await?;
        } else {
          self.ping(inner).await?;
        }
        self.select_database(inner).await?;
        if inner.connection.auto_client_setname {
          self.set_client_name(inner).await?;
        }
        self.cache_connection_id(inner).await?;
        self.cache_server_version(inner).await?;
        if !inner.connection.disable_cluster_health_check {
          self.check_cluster_state(inner).await?;
        }

        Ok::<_, RedisError>(())
      },
      timeout,
    )
    .await
  }

  /// Send `READONLY` to the server.
  #[cfg(feature = "replicas")]
  pub async fn readonly(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    timeout: Option<Duration>,
  ) -> Result<(), RedisError> {
    if !inner.config.server.is_clustered() {
      return Ok(());
    }
    let timeout = timeout.unwrap_or(inner.internal_command_timeout());

    utils::timeout(
      async {
        _debug!(inner, "Sending READONLY to {}", self.server);
        let command = RedisCommand::new(RedisCommandKind::Readonly, vec![]);
        let response = self.request_response(command, inner.is_resp3()).await?;
        let _ = protocol_utils::frame_to_results(response)?;

        Ok::<_, RedisError>(())
      },
      timeout,
    )
    .await
  }

  /// Send the `ROLE` command to the server.
  #[cfg(feature = "replicas")]
  pub async fn role(
    &mut self,
    inner: &RefCount<RedisClientInner>,
    timeout: Option<Duration>,
  ) -> Result<RedisValue, RedisError> {
    let timeout = timeout.unwrap_or(inner.internal_command_timeout());
    let command = RedisCommand::new(RedisCommandKind::Role, vec![]);

    utils::timeout(
      async {
        self
          .request_response(command, inner.is_resp3())
          .await
          .and_then(protocol_utils::frame_to_results)
      },
      timeout,
    )
    .await
  }

  /// Discover connected replicas via the ROLE command.
  #[cfg(feature = "replicas")]
  pub async fn discover_replicas(&mut self, inner: &RefCount<RedisClientInner>) -> Result<Vec<Server>, RedisError> {
    self
      .role(inner, None)
      .await
      .and_then(protocol_utils::parse_master_role_replicas)
  }

  /// Discover connected replicas via the ROLE command.
  #[cfg(not(feature = "replicas"))]
  pub async fn discover_replicas(&mut self, _: &RefCount<RedisClientInner>) -> Result<Vec<Server>, RedisError> {
    Ok(Vec::new())
  }

  /// Convert the connection into one that can be shared and pipelined across tasks.
  pub fn into_pipelined(self, _replica: bool) -> RedisConnection {
    let buffer = VecDeque::with_capacity(INITIAL_BUFFER_SIZE);
    let (server, addr, default_host) = (self.server, self.addr, self.default_host);
    let (id, version, counters) = (self.id, self.version, self.counters);

    RedisConnection {
      server,
      default_host,
      addr,
      buffer,
      version,
      counters,
      id,
      last_write: None,
      transport: self.transport,
      blocked: false,
      #[cfg(feature = "replicas")]
      replica: _replica,
    }
  }
}

/// A connection to Redis that can be shared and pipelined across tasks.
///
/// Once a connection becomes usable by clients we can no longer use the request-response logic on `RedisTransport`
/// since caller tasks may have in-flight frames already on the wire.
pub struct RedisConnection {
  pub server:       Server,
  pub transport:    ConnectionKind,
  pub default_host: Str,
  pub addr:         Option<SocketAddr>,
  pub buffer:       VecDeque<RedisCommand>,
  pub version:      Option<Version>,
  pub id:           Option<i64>,
  pub counters:     Counters,
  pub last_write:   Option<Instant>,
  pub blocked:      bool,
  #[cfg(feature = "replicas")]
  pub replica:      bool,
}

impl fmt::Debug for RedisConnection {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RedisConnection")
      .field("server", &self.server)
      .field("id", &self.id)
      .field("default_host", &self.default_host)
      .field("version", &self.version)
      .finish()
  }
}

impl RedisConnection {
  /// Check if the reader half is healthy, returning any errors.
  pub async fn peek_reader_errors(&mut self) -> Option<RedisError> {
    let result = std::future::poll_fn(|cx| match self.transport {
      ConnectionKind::Tcp(ref mut t) => match Pin::new(t).poll_peek(cx) {
        Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.clone()))),
        _ => Poll::Ready(None::<Result<(), RedisError>>),
      },
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(ref mut t) => match Pin::new(t).poll_peek(cx) {
        Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.clone()))),
        _ => Poll::Ready(None),
      },
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(ref mut t) => match Pin::new(t).poll_peek(cx) {
        Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.clone()))),
        _ => Poll::Ready(None),
      },
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut t) => match Pin::new(t).poll_peek(cx) {
        Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.clone()))),
        _ => Poll::Ready(None),
      },
    });

    if let Some(Err(e)) = result.await {
      Some(e)
    } else {
      None
    }
  }

  /// Write a frame to the socket.
  ///
  /// The caller is responsible for pushing frames into the in-flight buffer.
  #[inline(always)]
  pub async fn write<F: Into<ProtocolFrame>>(
    &mut self,
    frame: F,
    flush: bool,
    no_incr: bool,
    check_unresponsive: bool,
  ) -> Result<(), RedisError> {
    if check_unresponsive {
      self.last_write = Some(Instant::now());
    }

    let result = if flush {
      self.counters.reset_feed_count();
      self.transport.send(frame.into()).await
    } else {
      self.counters.incr_feed_count();
      self.transport.feed(frame.into()).await
    };

    if result.is_ok() & !no_incr {
      self.counters.incr_in_flight();
    }
    result
  }

  /// Put a command at the back of the in-flight command buffer.
  pub fn push_command(&mut self, mut cmd: RedisCommand) {
    if cmd.has_no_responses() {
      cmd.respond_to_caller(Ok(Resp3Frame::Null));
    } else {
      if cmd.blocks_connection() {
        self.blocked = true;
      }
      self.buffer.push_back(cmd);
    }
  }

  /// Read the next frame from the reader half in a cancellation-safe way.
  ///
  /// The caller is responsible for decrementing any in-flight counters.
  #[inline(always)]
  pub async fn read(&mut self) -> Result<Option<Resp3Frame>, RedisError> {
    match self.transport.next().await {
      Some(result) => result.map(|f| Some(f.into_resp3())),
      None => Ok(None),
    }
  }

  /// Read frames until the in-flight buffer is empty.
  pub async fn drain(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    let is_clustered = inner.config.server.is_clustered();
    while !self.buffer.is_empty() {
      let frame = match self.read_skip_pubsub(&inner).await? {
        Some(f) => f,
        None => return Ok(()),
      };

      // TODO check for special errors, pubsub, etc
      // need to merge this with router.drain and reader logic
      if is_clustered {
        clustered::process_response_frame(&inner, self, frame).await?;
      } else {
        centralized::process_response_frame(&inner, self, frame).await?;
      }
    }

    Ok(())
  }

  /// Read frames until the in-flight buffer is empty, dropping any non-pubsub frames.
  pub async fn skip_results(&mut self, inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
    while !self.buffer.is_empty() {
      match self.read_skip_pubsub(&inner).await? {
        Some(f) => f,
        None => return Ok(()),
      };
    }

    Ok(())
  }

  /// Read frames until detecting a non-pubsub frame.
  ///
  /// This function is not cancellation-safe.
  pub async fn read_skip_pubsub(
    &mut self,
    inner: &RefCount<RedisClientInner>,
  ) -> Result<Option<Resp3Frame>, RedisError> {
    loop {
      let frame = match self.read().await? {
        Some(f) => f,
        None => return Ok(None),
      };

      if let Some(frame) = responses::check_pubsub_message(inner, &self.server, frame) {
        return Ok(Some(frame));
      } else {
        continue;
      }
    }
  }

  /// Flush the sink and reset the feed counter.
  pub async fn flush(&mut self) -> Result<(), RedisError> {
    trace!("Flushing socket to {}", self.server);
    self.transport.flush().await?;
    self.counters.reset_feed_count();
    Ok(())
  }

  /// Close the connection.
  ///
  /// Returns the in-flight commands that had not received a response.
  pub async fn close(mut self) -> VecDeque<RedisCommand> {
    let _ = utils::timeout(
      self.transport.close(),
      Duration::from_millis(CONNECTION_CLOSE_TIMEOUT_MS),
    )
    .await;
    self.buffer
  }
}

/// Send a command and wait on the response.
///
/// The connection's in-flight command queue must be empty or drained before calling this.
pub async fn request_response(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
  command: RedisCommand,
  timeout: Option<Duration>,
) -> Result<Resp3Frame, RedisError> {
  let timeout_dur = timeout
    .or(command.timeout_dur)
    .unwrap_or_else(|| inner.default_command_timeout());

  _trace!(
    inner,
    "Sending {} ({}) to {}",
    command.kind.to_str_debug(),
    command.debug_id(),
    conn.server
  );
  let frame = protocol_utils::encode_frame(inner, &command)?;

  let check_unresponsive = !command.kind.is_pubsub() && inner.has_unresponsive_duration();
  let ft = async {
    conn.write(frame, true, true, check_unresponsive).await?;
    match conn.read_skip_pubsub(inner).await {
      Ok(Some(f)) => Ok(f),
      Ok(None) => Err(RedisError::new(RedisErrorKind::Unknown, "Missing response.")),
      Err(e) => Err(e),
    }
  };
  if timeout_dur.is_zero() {
    ft.await
  } else {
    utils::timeout(ft, timeout_dur).await
  }
}

#[cfg(feature = "replicas")]
pub async fn discover_replicas(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
) -> Result<Vec<Server>, RedisError> {
  utils::timeout(conn.drain(inner), inner.internal_command_timeout()).await?;

  let command = RedisCommand::new(RedisCommandKind::Role, vec![]);
  let role = request_response(inner, conn, command, None)
    .await
    .and_then(protocol_utils::frame_to_results)?;

  protocol_utils::parse_master_role_replicas(role)
}

/// Create a connection to the specified `host` and `port` with the provided timeout, in ms.
///
/// The returned connection will not be initialized.
pub async fn create(
  inner: &RefCount<RedisClientInner>,
  server: &Server,
  timeout: Option<Duration>,
) -> Result<ExclusiveConnection, RedisError> {
  let timeout = timeout.unwrap_or(inner.connection_timeout());

  _trace!(
    inner,
    "Checking connection type. Native-tls: {}, Rustls: {}",
    inner.config.uses_native_tls(),
    inner.config.uses_rustls(),
  );
  if inner.config.uses_native_tls() {
    utils::timeout(ExclusiveConnection::new_native_tls(inner, server), timeout).await
  } else if inner.config.uses_rustls() {
    utils::timeout(ExclusiveConnection::new_rustls(inner, server), timeout).await
  } else {
    match inner.config.server {
      #[cfg(feature = "unix-sockets")]
      ServerConfig::Unix { ref path } => utils::timeout(ExclusiveConnection::new_unix(inner, path), timeout).await,
      _ => utils::timeout(ExclusiveConnection::new_tcp(inner, server), timeout).await,
    }
  }
}
