use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::protocol::codec::RedisCodec;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::responders;
use crate::protocol::types::ClusterKeyCache;
use crate::protocol::types::ProtocolFrame;
use crate::protocol::utils as protocol_utils;
use crate::protocol::utils::{frame_into_string, pretty_error};
use crate::types::{ClientState, InfoKind, Resolve};
use crate::{utils as client_utils, utils};
use arcstr::ArcStr;
use futures::lock::BiLock;
use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use futures::{Sink, Stream, TryStream};
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp3::types::{Frame as Resp3Frame, RespVersion};
use semver::Version;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use std::{fmt, mem, str};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::protocol::tls::{self, TlsConnector};
#[cfg(feature = "monitor")]
use crate::types::ServerConfig;
#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::{TlsConnector as NativeTlsConnector, TlsStream as NativeTlsStream};
#[cfg(feature = "enable-rustls")]
use tokio_rustls::{rustls::ServerName, TlsConnector as RustlsConnector, TlsStream as RustlsStream};

/// The contents of a simplestring OK response.
pub const OK: &'static str = "OK";
/// The default timeout when establishing new connections.
pub const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 60_0000;

pub type CommandBuffer = VecDeque<RedisCommand>;
pub type SharedBuffer = Arc<BiLock<CommandBuffer>>;

pub type SplitRedisSink<T> = SplitSink<Framed<T, RedisCodec>, ProtocolFrame>;
pub type SplitRedisStream<T> = SplitStream<Framed<T, RedisCodec>>;

pub enum ConnectionKind {
  Tcp(Framed<TcpStream, RedisCodec>),
  #[cfg(feature = "enable-rustls")]
  Rustls(Framed<RustlsStream<TcpStream>, RedisCodec>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(Framed<NativeTlsStream<TcpStream>, RedisCodec>),
}

impl ConnectionKind {
  /// Split the connection.
  pub fn split(self) -> (SplitSinkKind, SplitStreamKind) {
    match self {
      ConnectionKind::Tcp(mut conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::Tcp(sink), SplitStreamKind::Tcp(stream))
      },
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(mut conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::Rustls(sink), SplitStreamKind::Rustls(stream))
      },
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(mut conn) => {
        let (sink, stream) = conn.split();
        (SplitSinkKind::NativeTls(sink), SplitStreamKind::NativeTls(stream))
      },
    }
  }
}

impl Stream for ConnectionKind {
  type Item = Result<ProtocolFrame, RedisError>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_next(cx),
    }
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).size_hint(),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).size_hint(),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).size_hint(),
    }
  }
}

impl Sink<ProtocolFrame> for ConnectionKind {
  type Error = RedisError;

  fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_ready(cx),
    }
  }

  fn start_send(self: Pin<&mut Self>, item: ProtocolFrame) -> Result<(), Self::Error> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).start_send(item),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e.into()),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e.into()),
    }
  }
}

pub enum SplitStreamKind {
  Tcp(SplitRedisStream<TcpStream>),
  #[cfg(feature = "enable-rustls")]
  Rustls(SplitRedisStream<RustlsStream<TcpStream>>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(SplitRedisStream<NativeTlsStream<TcpStream>>),
}

impl Stream for SplitStreamKind {
  type Item = Result<ProtocolFrame, RedisError>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_next(cx),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_next(cx),
    }
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).size_hint(),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).size_hint(),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).size_hint(),
    }
  }
}

pub enum SplitSinkKind {
  Tcp(SplitRedisSink<TcpStream>),
  #[cfg(feature = "enable-rustls")]
  Rustls(SplitRedisSink<RustlsStream<TcpStream>>),
  #[cfg(feature = "enable-native-tls")]
  NativeTls(SplitRedisSink<NativeTlsStream<TcpStream>>),
}

impl Sink<ProtocolFrame> for SplitSinkKind {
  type Error = RedisError;

  fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_ready(cx),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_ready(cx),
    }
  }

  fn start_send(self: Pin<&mut Self>, item: ProtocolFrame) -> Result<(), Self::Error> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).start_send(item),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).start_send(item),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_flush(cx).map_err(|e| e.into()),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    match self {
      ConnectionKind::Tcp(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-rustls")]
      ConnectionKind::Rustls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e.into()),
      #[cfg(feature = "enable-native-tls")]
      ConnectionKind::NativeTls(ref mut conn) => Pin::new(conn).poll_close(cx).map_err(|e| e.into()),
    }
  }
}

/// Atomic counters stored with connection state.
#[derive(Clone, Debug)]
pub struct Counters {
  pub cmd_buffer_len: Arc<AtomicUsize>,
  pub in_flight: Arc<AtomicUsize>,
  pub feed_count: Arc<AtomicUsize>,
}

impl Counters {
  pub fn new(cmd_buffer_len: &Arc<AtomicUsize>) -> Self {
    Counters {
      cmd_buffer_len: cmd_buffer_len.clone(),
      in_flight: Arc::new(AtomicUsize::new(0)),
      feed_count: Arc::new(AtomicUsize::new(0)),
    }
  }

  /// Flush the sink if the max feed count is reached or no commands are queued following the current command.
  pub fn should_send(&self, inner: &Arc<RedisClientInner>) -> bool {
    client_utils::read_atomic(&self.feed_count) > inner.max_feed_count()
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
  pub server: ArcStr,
  /// The parsed `SocketAddr` for the connection.
  pub addr: SocketAddr,
  /// The hostname used to initialize the connection.
  pub default_host: ArcStr,
  /// The network connection.
  pub transport: ConnectionKind,
  /// The connection/client ID from the CLIENT ID command.
  pub id: Option<i64>,
  /// The server version.
  pub version: Option<Version>,
  /// Counters for the connection state.
  pub counters: Counters,
}

impl RedisTransport {
  pub async fn new_tcp(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self, RedisError> {
    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let server = ArcStr::from(format!("{}:{}", host, port));
    let default_host = ArcStr::from(host.clone());
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    _debug!(
      inner,
      "Creating TCP connection to {} at {}:{}",
      server,
      addr.ip(),
      addr.port()
    );
    let socket = TcpStream::connect(addr).await?;
    let transport = ConnectionKind::Tcp(Framed::new(socket, codec));

    Ok(RedisTransport {
      server,
      default_host,
      counters,
      addr,
      id,
      version,
      transport,
    })
  }

  #[cfg(feature = "enable-native-tls")]
  pub async fn new_native_tls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self, RedisError> {
    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let server = ArcStr::from(format!("{}:{}", host, port));
    let default_host = ArcStr::from(host.clone());
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    _debug!(
      inner,
      "Creating `native-tls` connection to {} at {}:{}",
      server,
      addr.ip(),
      addr.port()
    );
    let socket = TcpStream::connect(addr).await?;

    match inner.config.tls {
      Some(TlsConnector::Native(ref connector)) => {
        let socket = connector.clone().connect(&host, socket).await?;
        let transport = ConnectionKind::NativeTls(Framed::new(socket, codec));

        Ok(RedisTransport {
          server,
          default_host,
          counters,
          addr,
          id,
          version,
          transport,
        })
      },
      _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
    };

    Ok(framed)
  }

  #[cfg(not(feature = "enable-native-tls"))]
  pub async fn new_native_tls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self, RedisError> {
    RedisTransport::new_tcp(inner, host, port).await
  }

  #[cfg(feature = "enable-rustls")]
  pub async fn new_rustls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self, RedisError> {
    let counters = Counters::new(&inner.counters.cmd_buffer_len);
    let (id, version) = (None, None);
    let server = ArcStr::from(format!("{}:{}", host, port));
    let default_host = ArcStr::from(host.clone());
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    _debug!(
      inner,
      "Creating `rustls` connection to {} at {}:{}",
      server,
      addr.ip(),
      addr.port()
    );
    let socket = TcpStream::connect(addr).await?;

    match inner.config.tls {
      Some(TlsConnector::Rustls(ref connector)) => {
        let server_name: ServerName = host.as_str().try_into()?;
        let socket = connector.clone().connect(server_name, socket).await?;
        let transport = ConnectionKind::Rustls(Framed::new(socket, codec));

        Ok(RedisTransport {
          server,
          counters,
          default_host,
          addr,
          id,
          version,
          transport,
        })
      },
      _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
    };

    Ok(framed)
  }

  #[cfg(not(feature = "enable-rustls"))]
  pub async fn new_rustls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self, RedisError> {
    RedisTransport::new_tcp(inner, host, port).await
  }

  /// Send a command to the server.
  pub async fn request_response(&mut self, cmd: RedisCommand, is_resp3: bool) -> Result<Resp3Frame, RedisError> {
    let frame = cmd.to_frame(is_resp3)?;
    let _ = self.transport.send(frame).await?;
    let response = self.transport.next().await;

    Ok(match response {
      Some(result) => result?.into_resp3(),
      None => protocol_utils::null_frame(is_resp3),
    })
  }

  /// Set the client name with CLIENT SETNAME.
  #[cfg(not(feature = "no-client-setname"))]
  pub async fn set_client_name(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    _debug!(inner, "Setting client name.");
    let command = RedisCommand::new(RedisCommandKind::ClientSetname, vec![inner.id.as_str().into()]);
    let response = self.request_response(command, inner.is_resp3()).await?;

    if protocol_utils::is_ok(&response) {
      debug!("{}: Successfully set Redis client name.", name);
      Ok(())
    } else {
      error!("{} Failed to set client name with error {:?}", name, response);
      Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Failed to set client name.",
      ))
    }
  }

  #[cfg(feature = "no-client-setname")]
  pub async fn set_client_name(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    _debug!(inner, "Skip setting client name.");
    Ok(())
  }

  /// Read and cache the server version.
  pub async fn cache_server_version(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let command = RedisCommand::new(RedisCommandKind::Info, vec![InfoKind::Server.to_str().into()]);
    let result = self.request_response(command, inner.is_resp3()).await?;
    let result = match result {
      Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())?,
      Resp3Frame::SimpleError { data, .. } => {
        _warn!(inner, "Failed to read server version: {:?}", data);
        return Ok(());
      },
      Resp3Frame::BlobError { data, .. } => {
        let parsed = String::from_utf8_lossy(&data);
        _warn!("Failed to read server version: {:?}", parsed);
        return Ok(());
      },
      _ => {
        warn!(inner, "Invalid INFO response.");
        return Ok(());
      },
    };

    self.version = result.lines().find_map(|line| {
      let parts: Vec<&str> = line.split(":").collect();
      if parts.len() < 2 {
        return None;
      }

      if parts[0] == "redis_version" {
        Version::parse(&parts[1]).ok()
      } else {
        None
      }
    });

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
        let error = match protocol_utils::frame_into_string(frame) {
          Ok(error) => error,
          Err(_) => {
            return Err(RedisError::new(
              RedisErrorKind::Auth,
              "Invalid auth response. Expected string.",
            ));
          },
        };
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

      let cmd = RedisCommand::new(RedisCommandKind::Hello(RespVersion::RESP3), args);
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
    let result = self.request_response(command, inner.is_resp3()).await?;
    _debug!(inner, "Read client ID: {:?}", result);
    self.id = match result {
      Resp3Frame::Number { data, .. } => Some(data),
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
    let command: RedisCommand = RedisCommandKind::Quit.into();
    let _ = self.request_response(command, inner.is_resp3()).await?;
    let _ = self.transport.close().await;

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
    let (result, transport) = self.request_response(command, inner.is_resp3()).await?;

    if let Some(error) = protocol_utils::frame_to_error(&result) {
      Err(error)
    } else {
      Ok(transport)
    }
  }

  /// Authenticate, set the protocol version, set the client name, select the provided database, and cache the connection ID and server version.
  pub async fn setup(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let _ = self.switch_protocols_and_authenticate(inner).await?;
    let _ = self.set_client_name(inner).await?;
    let _ = self.select_database(inner).await?;
    let _ = self.cache_connection_id(inner).await?;
    let _ = self.cache_server_version(inner).await?;

    Ok(())
  }

  /// Split the transport into reader/writer halves.
  pub fn split(self, inner: &Arc<RedisClientInner>) -> (RedisWriter, RedisReader) {
    let len = protocol_utils::initial_buffer_size(inner);
    let buffer = Arc::new(Mutex::new(VecDeque::with_capacity(len)));
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
      addr: addr.clone(),
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
  pub stream: Option<SplitStreamKind>,
  pub server: ArcStr,
  pub buffer: SharedBuffer,
  pub counters: Counters,
  pub task: Option<JoinHandle<Result<(), RedisError>>>,
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
  pub sink: SplitSinkKind,
  pub server: ArcStr,
  pub default_host: ArcStr,
  pub addr: SocketAddr,
  pub buffer: SharedBuffer,
  pub version: Option<Version>,
  pub id: Option<i64>,
  pub counters: Counters,
  pub reader: Option<RedisReader>,
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
    let _ = self.sink.flush().await?;
    self.counters.reset_feed_count();
    Ok(())
  }

  /// Conditionally flush the sink based on the feed count.
  pub async fn check_and_flush(&mut self) -> Result<(), RedisError> {
    if utils::read_atomic(&self.counters.feed_count) > 0 {
      let _ = self.flush().await?;
    }
    Ok(())
  }

  /// Send a command to the server without waiting on the response.
  pub async fn write_frame(&mut self, frame: ProtocolFrame, should_flush: bool) -> Result<(), RedisError> {
    if should_flush {
      _trace!(inner, "Sending command and flushing the sink.");
      let _ = self.sink.send(frame).await?;
      self.counters.reset_feed_count();
    } else {
      _trace!(inner, "Sending command without flushing the sink.");
      let _ = self.sink.feed(frame).await?;
      self.counters.incr_feed_count();
    };
    self.counters.incr_in_flight();

    Ok(())
  }

  /// Put a command at the back of the command queue.
  pub async fn push_command(&self, cmd: RedisCommand) {
    self.buffer.lock().await.push_back(cmd);
  }

  /// Force close the connection.
  ///
  /// Returns the in-flight commands that had not received a response.
  pub fn force_close(self, abort_reader: bool) -> CommandBuffer {
    if abort_reader && self.reader.is_some() {
      self.reader.unwrap().stop(true);
    }
    self.buffer.lock().drain(..).collect()
  }

  /// Gracefully close the connection and wait for the reader task to finish.
  ///
  /// Returns the in-flight commands that had not received a response.
  pub async fn graceful_close(mut self) -> CommandBuffer {
    self.sink.close().await;
    if let Some(mut reader) = self.reader {
      reader.wait().await
    }
    self.buffer.lock().drain(..).collect()
  }
}

/// Create a connection to the specified `host` and `port` with the provided timeout, in ms.
///
/// The returned connection will not be initialized.
pub async fn create(
  inner: &Arc<RedisClientInner>,
  host: String,
  port: u16,
  timeout_ms: Option<u64>,
) -> Result<RedisTransport, RedisError> {
  let timeout = timeout_ms.unwrap_or(DEFAULT_CONNECTION_TIMEOUT_MS);

  let transport_ft = if inner.config.uses_native_tls() {
    RedisTransport::new_native_tls(inner, host, port)
  } else if inner.config.uses_rustls() {
    RedisTransport::new_rustls(inner, host, port)
  } else {
    RedisTransport::new_tcp(inner, host, port)
  };

  utils::apply_timeout(transport_ft, timeout).await
}

/// Split a connection, spawn a reader task, and link the reader and writer halves.
pub fn split_and_initialize<F>(
  inner: &Arc<RedisClientInner>,
  transport: RedisTransport,
  func: F,
) -> Result<(ArcStr, RedisWriter), RedisError>
where
  F: FnOnce(
    &Arc<RedisClientInner>,
    SplitStreamKind,
    &ArcStr,
    &SharedBuffer,
    &Counters,
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
  ));
  writer.reader = Some(reader);

  Ok((server, writer))
}
