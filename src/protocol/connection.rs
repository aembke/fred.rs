use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::protocol::codec::RedisCodec;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::types::ClusterKeyCache;
use crate::protocol::types::ProtocolFrame;
use crate::protocol::utils as protocol_utils;
use crate::protocol::utils::{frame_into_string, pretty_error};
use crate::types::{ClientState, InfoKind, Resolve};
use crate::utils as client_utils;
use arcstr::ArcStr;
use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use futures::{Sink, Stream};
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp3::types::{Frame as Resp3Frame, RespVersion};
use semver::Version;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use std::{fmt, mem, str};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
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

pub type CommandBuffer = VecDeque<RedisCommand>;
pub type SharedBuffer = Arc<Mutex<CommandBuffer>>;
pub type SplitRedisSink<T> = SplitSink<Framed<T, RedisCodec>, ProtocolFrame>;
pub type SplitRedisStream<T> = SplitStream<Framed<T, RedisCodec>>;

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
    client_utils::read_atomic(&self.feed_count) > inner.perf_config.max_feed_count()
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

pub struct RedisTransport<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  /// An identifier for the connection, usually `<host>|<ip>:<port>`.
  pub server: ArcStr,
  /// The parsed `SocketAddr` for the connection.
  pub addr: SocketAddr,
  /// The hostname used to initialize the connection.
  pub default_host: ArcStr,
  /// The TCP/TLS connection.
  pub transport: Framed<T, RedisCodec>,
  /// A shared buffer of commands used for pipelining.
  pub buffer: CommandBuffer,
  /// The connection/client ID from the CLIENT ID command.
  pub id: Option<i64>,
  /// The server version.
  pub version: Option<Version>,
  /// Counters for the connection state.
  pub counters: Counters,
}

impl<T> RedisTransport<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  pub async fn new_tcp(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self<T>, RedisError> {
    let buffer_size = protocol_utils::initial_buffer_size(inner);
    let counters = Counters::new(&inner.cmd_buffer_len);
    let (id, version, buffer) = (None, None, VecDeque::with_capacity(buffer_size));
    let server = ArcStr::from(format!("{}:{}", host, port));
    let default_host = ArcStr::from(host.clone());
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    let socket = TcpStream::connect(addr).await?;
    let transport = Framed::new(socket, codec);

    Ok(RedisTransport {
      server,
      default_host,
      counters,
      addr,
      buffer,
      id,
      version,
      transport,
    })
  }

  #[cfg(feature = "enable-native-tls")]
  pub async fn new_native_tls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self<T>, RedisError> {
    let buffer_size = protocol_utils::initial_buffer_size(inner);
    let counters = Counters::new(&inner.cmd_buffer_len);
    let (id, version, buffer) = (None, None, VecDeque::with_capacity(buffer_size));
    let server = ArcStr::from(format!("{}:{}", host, port));
    let default_host = ArcStr::from(host.clone());
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    let socket = TcpStream::connect(addr).await?;

    match inner.config.tls {
      Some(TlsConnector::Native(ref connector)) => {
        let socket = connector.clone().connect(&host, socket).await?;
        let transport = Framed::new(socket, codec);

        Ok(RedisTransport {
          server,
          default_host,
          counters,
          addr,
          buffer,
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
  pub async fn new_native_tls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self<T>, RedisError> {
    RedisTransport::new_tcp(inner, host, port).await
  }

  #[cfg(feature = "enable-rustls")]
  pub async fn new_rustls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self<T>, RedisError> {
    let buffer_size = protocol_utils::initial_buffer_size(inner);
    let counters = Counters::new(&inner.cmd_buffer_len);
    let (id, version, buffer) = (None, None, VecDeque::with_capacity(buffer_size));
    let server = ArcStr::from(format!("{}:{}", host, port));
    let default_host = ArcStr::from(host.clone());
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    let socket = TcpStream::connect(addr).await?;

    match inner.config.tls {
      Some(TlsConnector::Rustls(ref connector)) => {
        let server_name: ServerName = host.as_str().try_into()?;
        let socket = connector.clone().connect(server_name, socket).await?;
        let transport = Framed::new(socket, codec);

        Ok(RedisTransport {
          server,
          counters,
          default_host,
          addr,
          buffer,
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
  pub async fn new_rustls(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self<T>, RedisError> {
    RedisTransport::new_tcp(inner, host, port).await
  }

  /// Send a command to the server.
  pub async fn request_response(&mut self, cmd: RedisCommand, is_resp3: bool) -> Result<ProtocolFrame, RedisError> {
    let frame = cmd.to_frame(is_resp3)?;
    let _ = self.transport.send(frame).await?;
    let response = self.transport.next().await;

    Ok(match response {
      Some(result) => result?,
      None => protocol_utils::null_frame(is_resp3),
    })
  }

  /// Set the client name with CLIENT SETNAME.
  #[cfg(not(feature = "no-client-setname"))]
  pub async fn set_client_name(&mut self, name: &str, is_resp3: bool) -> Result<(), RedisError> {
    debug!("{}: Changing client name to {}", name, name);
    let command = RedisCommand::new(RedisCommandKind::ClientSetname, vec![name.into()], None);
    let response = self.request_response(command, is_resp3).await?;

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
  pub async fn set_client_name(&mut self, name: &str, _: bool) -> Result<(), RedisError> {
    debug!("{}: Skip setting client name.", name);
    Ok(())
  }

  /// Read and cache the server version.
  pub async fn cache_server_version(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let command = RedisCommand::new(RedisCommandKind::Info, vec![InfoKind::Server.to_str().into()], None);
    let result = self.request_response(command, inner.is_resp3()).await?;
    let result = match result.into_resp3() {
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
      let command = RedisCommand::new(RedisCommandKind::Auth, args, None);

      debug!("{}: Authenticating Redis client...", name);
      let frame = self.request_response(command, is_resp3).await?;

      if !protocol_utils::is_ok(&frame) {
        let error = match protocol_utils::frame_into_string(frame.into_resp3()) {
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
    }

    let _ = self.set_client_name(name, is_resp3).await?;
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

      let cmd = RedisCommand::new(RedisCommandKind::Hello(RespVersion::RESP3), args, None);
      let response = self.request_response(cmd, true).await?;
      let response = protocol_utils::frame_to_results(response.into_resp3())?;
      _debug!(inner, "Recv HELLO response {:?}", response);

      self.set_client_name(&inner.id, true).await
    } else {
      self.authenticate(&inner.id, username, password, false).await
    }
  }

  /// Read and cache the connection ID.
  pub async fn cache_connection_id(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let command = RedisCommand::new(RedisCommandKind::ClientID, vec![], None);
    let result = self.request_response(command, inner.is_resp3()).await?;
    _debug!(inner, "Read client ID: {:?}", result);
    self.id = match result.into_resp3() {
      Resp3Frame::Number { data, .. } => Some(data),
      _ => None,
    };

    Ok(())
  }

  /// Select the database provided in the `RedisConfig`.
  pub async fn select_database(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let db = match inner.config.database {
      Some(db) => db,
      None => return Ok(()),
    };

    _trace!(inner, "Selecting database {} after connecting.", db);
    let command = RedisCommand::new(RedisCommandKind::Select, vec![db.into()], None);
    let (result, transport) = self.request_response(command, inner.is_resp3()).await?;
    let response = result.into_resp3();

    if let Some(error) = protocol_utils::frame_to_error(&response) {
      Err(error)
    } else {
      Ok(transport)
    }
  }

  /// Authenticate, set the protocol version, set the client name, select the provided database, and cache the connection ID and server version.
  pub async fn setup(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let _ = self.switch_protocols_and_authenticate(inner).await?;
    let _ = self.select_database(inner).await?;
    let _ = self.cache_connection_id(inner).await?;
    let _ = self.cache_server_version(inner).await?;

    Ok(())
  }

  /// Split the transport into reader/writer halves.
  pub fn split(self) -> (RedisWriter<T>, RedisReader<T>) {
    let buffer = Arc::new(Mutex::new(self.buffer));
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
    };
    let reader = RedisReader {
      stream,
      server,
      buffer,
      counters,
    };
    (writer, reader)
  }
}

pub struct RedisReader<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  stream: SplitRedisStream<T>,
  server: ArcStr,
  buffer: SharedBuffer,
  counters: Counters,
}

impl<T> RedisReader<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  pub fn into_inner(self) -> (SplitSt, ArcStr, SharedBuffer, Counters) {
    (self.stream, self.server, self.buffer, self.counters)
  }
}

pub struct RedisWriter<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  pub sink: SplitRedisSink<T>,
  pub server: ArcStr,
  pub default_host: ArcStr,
  pub addr: SocketAddr,
  pub buffer: SharedBuffer,
  pub version: Option<Version>,
  pub id: Option<i64>,
  pub counters: Counters,
}

impl<T> fmt::Debug for RedisWriter<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Connection").field("server", &self.server).finish()
  }
}

impl<T> RedisWriter<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  /// Send a command to the server without waiting on the response.
  pub async fn write_command(&mut self, frame: ProtocolFrame, should_flush: bool) -> Result<(), RedisError> {
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

  pub fn push_command(&self, cmd: RedisCommand) {
    self.buffer.lock().push_back(cmd.into());
  }

  pub fn close(self) -> CommandBuffer {
    self.buffer.lock().drain(..).collect()
  }
}
