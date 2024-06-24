use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    codec::RedisCodec,
    command::{RedisCommand, RedisCommandKind, RouterResponse},
    types::{ProtocolFrame, Server},
    utils as protocol_utils,
  },
  types::InfoKind,
  utils as client_utils,
  utils,
};
use bytes_utils::Str;
use crossbeam_queue::SegQueue;
use futures::{
  sink::SinkExt,
  stream::{SplitSink, SplitStream, StreamExt},
  Sink,
  Stream,
};
use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, Resp3Frame as _Resp3Frame, RespVersion};
use semver::Version;
use socket2::SockRef;
use std::{
  fmt,
  net::SocketAddr,
  pin::Pin,
  str,
  sync::{atomic::AtomicUsize, Arc},
  task::{Context, Poll},
  time::Duration,
};
use tokio::{net::TcpStream, task::JoinHandle};
use tokio_util::codec::Framed;

#[cfg(feature = "unix-sockets")]
use crate::prelude::ServerConfig;
#[cfg(any(
  feature = "enable-native-tls",
  feature = "enable-rustls",
  feature = "enable-rustls-ring"
))]
use crate::protocol::tls::TlsConnector;
#[cfg(feature = "replicas")]
use crate::{
  protocol::{connection, responders::ResponseKind},
  types::RedisValue,
};
#[cfg(feature = "unix-sockets")]
use std::path::Path;
use std::sync::atomic::AtomicBool;
#[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
use std::{convert::TryInto, ops::Deref};
#[cfg(feature = "unix-sockets")]
use tokio::net::UnixStream;
#[cfg(feature = "replicas")]
use tokio::sync::oneshot::channel as oneshot_channel;
#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::TlsStream as NativeTlsStream;
#[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
use tokio_rustls::client::TlsStream as RustlsStream;

/// The contents of a simplestring OK response.
pub const OK: &str = "OK";
/// The timeout duration used when dropping the split sink and waiting on the split stream to close.
pub const CONNECTION_CLOSE_TIMEOUT_MS: u64 = 5_000;

pub type CommandBuffer = Vec<RedisCommand>;

/// A shared buffer across tasks.
#[derive(Clone, Debug)]
pub struct SharedBuffer {
  inner:   Arc<SegQueue<RedisCommand>>,
  blocked: Arc<AtomicBool>,
}

impl SharedBuffer {
  pub fn new() -> Self {
    SharedBuffer {
      inner:   Arc::new(SegQueue::new()),
      blocked: Arc::new(AtomicBool::new(false)),
    }
  }

  pub fn push(&self, cmd: RedisCommand) {
    self.inner.push(cmd);
  }

  pub fn pop(&self) -> Option<RedisCommand> {
    self.inner.pop()
  }

  pub fn len(&self) -> usize {
    self.inner.len()
  }

  pub fn set_blocked(&self) {
    utils::set_bool_atomic(&self.blocked, true);
  }

  pub fn set_unblocked(&self) {
    utils::set_bool_atomic(&self.blocked, false);
  }

  pub fn is_blocked(&self) -> bool {
    utils::read_bool_atomic(&self.blocked)
  }

  pub fn drain(&self) -> Vec<RedisCommand> {
    utils::set_bool_atomic(&self.blocked, false);
    let mut out = Vec::with_capacity(self.inner.len());
    while let Some(cmd) = self.inner.pop() {
      out.push(cmd);
    }
    out
  }
}

pub type SplitRedisSink<T> = SplitSink<Framed<T, RedisCodec>, ProtocolFrame>;
pub type SplitRedisStream<T> = SplitStream<Framed<T, RedisCodec>>;

/// Connect to each socket addr and return the first successful connection.
async fn tcp_connect_any(
  inner: &Arc<RedisClientInner>,
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
    let socket = match TcpStream::connect(addr).await {
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
    if let Some(dur) = inner.connection.tcp.linger {
      socket.set_linger(Some(dur))?;
    }
    if let Some(ttl) = inner.connection.tcp.ttl {
      socket.set_ttl(ttl)?;
    }
    if let Some(ref keepalive) = inner.connection.tcp.keepalive {
      SockRef::from(&socket).set_tcp_keepalive(keepalive)?;
    }

    return Ok((socket, *addr));
  }

  _trace!(inner, "Failed to connect to any of {:?}.", addrs);
  Err(last_error.unwrap_or(RedisError::new(RedisErrorKind::IO, "Failed to connect.")))
}

pub enum ConnectionKind {
  Tcp(Framed<TcpStream, RedisCodec>),
  #[cfg(feature = "unix-sockets")]
  Unix(Framed<UnixStream, RedisCodec>),
  #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
  Rustls(Framed<RustlsStream<TcpStream>, RedisCodec>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(Framed<NativeTlsStream<TcpStream>, RedisCodec>),
}

impl ConnectionKind {
  /// Split the connection.
  pub fn split(self) -> (SplitSinkKind, SplitStreamKind) {
    match self {
      ConnectionKind::Tcp(conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::Tcp(sink), SplitStreamKind::Tcp(stream))
      },
      #[cfg(feature = "unix-sockets")]
      ConnectionKind::Unix(conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::Unix(sink), SplitStreamKind::Unix(stream))
      },
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      ConnectionKind::Rustls(conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::Rustls(sink), SplitStreamKind::Rustls(stream))
      },
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::NativeTls(sink), SplitStreamKind::NativeTls(stream))
      },
    }
  }
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

pub enum SplitStreamKind {
  Tcp(SplitRedisStream<TcpStream>),
  #[cfg(feature = "unix-sockets")]
  Unix(SplitRedisStream<UnixStream>),
  #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
  Rustls(SplitRedisStream<RustlsStream<TcpStream>>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(SplitRedisStream<NativeTlsStream<TcpStream>>),
}

impl Stream for SplitStreamKind {
  type Item = Result<ProtocolFrame, RedisError>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self.get_mut() {
      SplitStreamKind::Tcp(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "unix-sockets")]
      SplitStreamKind::Unix(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      SplitStreamKind::Rustls(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "enable-native-tls")]
      SplitStreamKind::NativeTls(ref mut conn) => Pin::new(conn).poll_next(cx),
    }
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    match self {
      SplitStreamKind::Tcp(ref conn) => conn.size_hint(),
      #[cfg(feature = "unix-sockets")]
      SplitStreamKind::Unix(ref conn) => conn.size_hint(),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      SplitStreamKind::Rustls(ref conn) => conn.size_hint(),
      #[cfg(feature = "enable-native-tls")]
      SplitStreamKind::NativeTls(ref conn) => conn.size_hint(),
    }
  }
}

pub enum SplitSinkKind {
  Tcp(SplitRedisSink<TcpStream>),
  #[cfg(feature = "unix-sockets")]
  Unix(SplitRedisSink<UnixStream>),
  #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
  Rustls(SplitRedisSink<RustlsStream<TcpStream>>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(SplitRedisSink<NativeTlsStream<TcpStream>>),
}

impl Sink<ProtocolFrame> for SplitSinkKind {
  type Error = RedisError;

  fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self.get_mut() {
      SplitSinkKind::Tcp(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "unix-sockets")]
      SplitSinkKind::Unix(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      SplitSinkKind::Rustls(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "enable-native-tls")]
      SplitSinkKind::NativeTls(ref mut conn) => Pin::new(conn).poll_ready(cx),
    }
  }

  fn start_send(self: Pin<&mut Self>, item: ProtocolFrame) -> Result<(), Self::Error> {
    match self.get_mut() {
      SplitSinkKind::Tcp(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "unix-sockets")]
      SplitSinkKind::Unix(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      SplitSinkKind::Rustls(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "enable-native-tls")]
      SplitSinkKind::NativeTls(ref mut conn) => Pin::new(conn).start_send(item),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self.get_mut() {
      SplitSinkKind::Tcp(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
      #[cfg(feature = "unix-sockets")]
      SplitSinkKind::Unix(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      SplitSinkKind::Rustls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
      #[cfg(feature = "enable-native-tls")]
      SplitSinkKind::NativeTls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self.get_mut() {
      SplitSinkKind::Tcp(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
      #[cfg(feature = "unix-sockets")]
      SplitSinkKind::Unix(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
      #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
      SplitSinkKind::Rustls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
      #[cfg(feature = "enable-native-tls")]
      SplitSinkKind::NativeTls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e),
    }
  }
}

/// Atomic counters stored with connection state.
#[derive(Clone, Debug)]
pub struct Counters {
  pub cmd_buffer_len: Arc<AtomicUsize>,
  pub in_flight:      Arc<AtomicUsize>,
  pub feed_count:     Arc<AtomicUsize>,
}

impl Counters {
  pub fn new(cmd_buffer_len: &Arc<AtomicUsize>) -> Self {
    Counters {
      cmd_buffer_len: cmd_buffer_len.clone(),
      in_flight:      Arc::new(AtomicUsize::new(0)),
      feed_count:     Arc::new(AtomicUsize::new(0)),
    }
  }

  /// Flush the sink if the max feed count is reached or no commands are queued following the current command.
  pub fn should_send(&self, inner: &Arc<RedisClientInner>) -> bool {
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

pub struct RedisTransport {
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

impl RedisTransport {
  pub async fn new_tcp(inner: &Arc<RedisClientInner>, server: &Server) -> Result<RedisTransport, RedisError> {
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
    let transport = ConnectionKind::Tcp(Framed::new(socket, codec));

    Ok(RedisTransport {
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
  pub async fn new_unix(inner: &Arc<RedisClientInner>, path: &Path) -> Result<RedisTransport, RedisError> {
    _debug!(inner, "Connecting via unix socket to {}", utils::path_to_string(path));
    let server = Server::new(utils::path_to_string(path), 0);
    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let default_host = server.host.clone();
    let codec = RedisCodec::new(inner, &server);
    let socket = UnixStream::connect(path).await?;
    let transport = ConnectionKind::Unix(Framed::new(socket, codec));

    Ok(RedisTransport {
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
  pub async fn new_native_tls(inner: &Arc<RedisClientInner>, server: &Server) -> Result<RedisTransport, RedisError> {
    let connector = match inner.config.tls {
      Some(ref config) => match config.connector {
        TlsConnector::Native(ref connector) => connector.clone(),
        _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
      },
      None => return RedisTransport::new_tcp(inner, server).await,
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
    let transport = ConnectionKind::NativeTls(Framed::new(socket, codec));

    Ok(RedisTransport {
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
  pub async fn new_native_tls(inner: &Arc<RedisClientInner>, server: &Server) -> Result<RedisTransport, RedisError> {
    RedisTransport::new_tcp(inner, server).await
  }

  #[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
  #[allow(unreachable_patterns)]
  pub async fn new_rustls(inner: &Arc<RedisClientInner>, server: &Server) -> Result<RedisTransport, RedisError> {
    use rustls::pki_types::ServerName;

    let connector = match inner.config.tls {
      Some(ref config) => match config.connector {
        TlsConnector::Rustls(ref connector) => connector.clone(),
        _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
      },
      None => return RedisTransport::new_tcp(inner, server).await,
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
    let transport = ConnectionKind::Rustls(Framed::new(socket, codec));

    Ok(RedisTransport {
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
  pub async fn new_rustls(inner: &Arc<RedisClientInner>, server: &Server) -> Result<RedisTransport, RedisError> {
    RedisTransport::new_tcp(inner, server).await
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
  pub async fn set_client_name(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
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
  pub async fn cache_server_version(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
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
  pub async fn switch_protocols_and_authenticate(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    // reset the protocol version to the one specified by the config when we create new connections
    inner.reset_protocol_version();
    let username = inner.config.username.clone();
    let password = inner.config.password.clone();

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
  pub async fn cache_connection_id(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
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
  pub async fn ping(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let command = RedisCommandKind::Ping.into();
    let response = self.request_response(command, inner.is_resp3()).await?;

    if let Some(e) = protocol_utils::frame_to_error(&response) {
      Err(e)
    } else {
      Ok(())
    }
  }

  /// Send `QUIT` and close the connection.
  pub async fn disconnect(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    if let Err(e) = self.transport.close().await {
      _warn!(inner, "Error closing connection to {}: {:?}", self.server, e);
    }
    Ok(())
  }

  /// Select the database provided in the `RedisConfig`.
  pub async fn select_database(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    if inner.config.server.is_clustered() {
      return Ok(());
    }

    let db = match inner.config.database {
      Some(db) => db,
      None => return Ok(()),
    };

    _trace!(inner, "Selecting database {} after connecting.", db);
    let command = RedisCommand::new(RedisCommandKind::Select, vec![db.into()]);
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
  pub async fn check_cluster_state(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
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
  pub async fn setup(&mut self, inner: &Arc<RedisClientInner>, timeout: Option<Duration>) -> Result<(), RedisError> {
    let timeout = timeout.unwrap_or(inner.internal_command_timeout());

    utils::timeout(
      async {
        if inner.config.password.is_some() || inner.config.version == RespVersion::RESP3 {
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
    inner: &Arc<RedisClientInner>,
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
    inner: &Arc<RedisClientInner>,
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
  pub async fn discover_replicas(&mut self, inner: &Arc<RedisClientInner>) -> Result<Vec<Server>, RedisError> {
    self
      .role(inner, None)
      .await
      .and_then(protocol_utils::parse_master_role_replicas)
  }

  /// Discover connected replicas via the ROLE command.
  #[cfg(not(feature = "replicas"))]
  pub async fn discover_replicas(&mut self, _: &Arc<RedisClientInner>) -> Result<Vec<Server>, RedisError> {
    Ok(Vec::new())
  }

  /// Split the transport into reader/writer halves.
  pub fn split(self) -> (RedisWriter, RedisReader) {
    let buffer = SharedBuffer::new();
    let (server, addr, default_host) = (self.server, self.addr, self.default_host);
    let (sink, stream) = self.transport.split();
    let (id, version, counters) = (self.id, self.version, self.counters);

    let writer = RedisWriter {
      sink,
      id,
      version,
      default_host,
      counters: counters.clone(),
      server: server.clone(),
      addr,
      buffer: buffer.clone(),
      reader: None,
    };
    let reader = RedisReader {
      stream: Some(stream),
      task: None,
      server,
      buffer,
      counters,
    };
    (writer, reader)
  }
}

pub struct RedisReader {
  pub stream:   Option<SplitStreamKind>,
  pub server:   Server,
  pub buffer:   SharedBuffer,
  pub counters: Counters,
  pub task:     Option<JoinHandle<Result<(), RedisError>>>,
}

impl RedisReader {
  pub async fn wait(&mut self) -> Result<(), RedisError> {
    if let Some(ref mut task) = self.task {
      task.await?
    } else {
      Ok(())
    }
  }

  pub fn is_connected(&self) -> bool {
    self.task.is_some() || self.stream.is_some()
  }

  pub fn is_running(&self) -> bool {
    self.task.is_some()
  }

  pub fn stop(&mut self, abort: bool) {
    if abort && self.task.is_some() {
      self.task.take().unwrap().abort();
    } else {
      self.task = None;
    }
    self.stream = None;
  }
}

pub struct RedisWriter {
  pub sink:         SplitSinkKind,
  pub server:       Server,
  pub default_host: Str,
  pub addr:         Option<SocketAddr>,
  pub buffer:       SharedBuffer,
  pub version:      Option<Version>,
  pub id:           Option<i64>,
  pub counters:     Counters,
  pub reader:       Option<RedisReader>,
}

impl fmt::Debug for RedisWriter {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Connection")
      .field("server", &self.server)
      .field("id", &self.id)
      .field("default_host", &self.default_host)
      .field("version", &self.version)
      .finish()
  }
}

impl RedisWriter {
  /// Flush the sink and reset the feed counter.
  pub async fn flush(&mut self) -> Result<(), RedisError> {
    trace!("Flushing socket to {}", self.server);
    self.sink.flush().await?;
    trace!("Flushed socket to {}", self.server);
    self.counters.reset_feed_count();
    Ok(())
  }

  #[cfg(feature = "replicas")]
  pub async fn discover_replicas(&mut self, inner: &Arc<RedisClientInner>) -> Result<Vec<Server>, RedisError> {
    let command = RedisCommand::new(RedisCommandKind::Role, vec![]);
    let role = connection::request_response(inner, self, command, None)
      .await
      .and_then(protocol_utils::frame_to_results)?;

    protocol_utils::parse_master_role_replicas(role)
  }

  /// Check if the reader task is still running or awaiting frames.
  pub fn is_working(&self) -> bool {
    self
      .reader
      .as_ref()
      .and_then(|reader| reader.task.as_ref())
      .map(|task| !task.is_finished())
      .unwrap_or(false)
  }

  /// Send a command to the server without waiting on the response.
  pub async fn write_frame(
    &mut self,
    frame: ProtocolFrame,
    should_flush: bool,
    no_incr: bool,
  ) -> Result<(), RedisError> {
    if should_flush {
      trace!("Writing and flushing {}", self.server);
      if let Err(e) = self.sink.send(frame).await {
        // the more useful error appears on the reader half but we'll log this just in case
        debug!("{}: Error sending frame to socket: {:?}", self.server, e);
        return Err(e);
      }
      self.counters.reset_feed_count();
    } else {
      trace!("Writing without flushing {}", self.server);
      if let Err(e) = self.sink.feed(frame).await {
        // the more useful error appears on the reader half but we'll log this just in case
        debug!("{}: Error feeding frame to socket: {:?}", self.server, e);
        return Err(e);
      }
      self.counters.incr_feed_count();
    };
    if !no_incr {
      self.counters.incr_in_flight();
    }

    Ok(())
  }

  /// Put a command at the back of the command queue.
  pub fn push_command(&self, inner: &Arc<RedisClientInner>, mut cmd: RedisCommand) {
    if cmd.has_no_responses() {
      _trace!(
        inner,
        "Skip adding `{}` command to response buffer (no expected responses).",
        cmd.kind.to_str_debug()
      );

      cmd.respond_to_router(inner, RouterResponse::Continue);
      cmd.respond_to_caller(Ok(Resp3Frame::Null));
      return;
    }

    if cmd.blocks_connection() {
      self.buffer.set_blocked();
    }
    self.buffer.push(cmd);
  }

  /// Force close the connection.
  ///
  /// Returns the in-flight commands that had not received a response.
  pub fn force_close(self, abort_reader: bool) -> CommandBuffer {
    if abort_reader && self.reader.is_some() {
      self.reader.unwrap().stop(true);
    }
    self.buffer.drain()
  }

  /// Gracefully close the connection and wait for the reader task to finish.
  ///
  /// Returns the in-flight commands that had not received a response.
  pub async fn graceful_close(mut self) -> CommandBuffer {
    let _ = utils::timeout(
      async {
        let _ = self.sink.close().await;
        if let Some(mut reader) = self.reader {
          let _ = reader.wait().await;
        }

        Ok::<_, RedisError>(())
      },
      Duration::from_millis(CONNECTION_CLOSE_TIMEOUT_MS),
    )
    .await;

    self.buffer.drain()
  }
}

/// Create a connection to the specified `host` and `port` with the provided timeout, in ms.
///
/// The returned connection will not be initialized.
pub async fn create(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  timeout: Option<Duration>,
) -> Result<RedisTransport, RedisError> {
  let timeout = timeout.unwrap_or(inner.connection_timeout());

  _trace!(
    inner,
    "Checking connection type. Native-tls: {}, Rustls: {}",
    inner.config.uses_native_tls(),
    inner.config.uses_rustls(),
  );
  if inner.config.uses_native_tls() {
    utils::timeout(RedisTransport::new_native_tls(inner, server), timeout).await
  } else if inner.config.uses_rustls() {
    utils::timeout(RedisTransport::new_rustls(inner, server), timeout).await
  } else {
    match inner.config.server {
      #[cfg(feature = "unix-sockets")]
      ServerConfig::Unix { ref path } => utils::timeout(RedisTransport::new_unix(inner, path), timeout).await,
      _ => utils::timeout(RedisTransport::new_tcp(inner, server), timeout).await,
    }
  }
}

/// Split a connection, spawn a reader task, and link the reader and writer halves.
pub fn split<F>(
  inner: &Arc<RedisClientInner>,
  transport: RedisTransport,
  is_replica: bool,
  func: F,
) -> Result<(Server, RedisWriter), RedisError>
where
  F: FnOnce(
    &Arc<RedisClientInner>,
    SplitStreamKind,
    &Server,
    &SharedBuffer,
    &Counters,
    bool,
  ) -> JoinHandle<Result<(), RedisError>>,
{
  let server = transport.server.clone();
  let (mut writer, mut reader) = transport.split();
  let reader_stream = match reader.stream.take() {
    Some(stream) => stream,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Missing clustered connection reader stream.",
      ))
    },
  };
  reader.task = Some(func(
    inner,
    reader_stream,
    &writer.server,
    &writer.buffer,
    &writer.counters,
    is_replica,
  ));
  writer.reader = Some(reader);

  Ok((server, writer))
}

/// Send a command to the server and wait for a response.
#[cfg(feature = "replicas")]
pub async fn request_response(
  inner: &Arc<RedisClientInner>,
  writer: &mut RedisWriter,
  mut command: RedisCommand,
  timeout: Option<Duration>,
) -> Result<Resp3Frame, RedisError> {
  let (tx, rx) = oneshot_channel();
  command.response = ResponseKind::Respond(Some(tx));
  let timeout_dur = timeout
    .or(command.timeout_dur)
    .unwrap_or_else(|| inner.default_command_timeout());

  _trace!(
    inner,
    "Sending {} ({}) to {}",
    command.kind.to_str_debug(),
    command.debug_id(),
    writer.server
  );
  let frame = protocol_utils::encode_frame(inner, &command)?;

  if !writer.is_working() {
    return Err(RedisError::new(RedisErrorKind::IO, "Connection closed."));
  }

  writer.push_command(inner, command);
  writer.write_frame(frame, true, false).await?;
  utils::timeout(async { rx.await? }, timeout_dur).await
}
