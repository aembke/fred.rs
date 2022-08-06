use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::Counters;
use crate::protocol::codec::RedisCodec;
use crate::protocol::types::ProtocolFrame;
use crate::protocol::types::{ClusterKeyCache, RedisCommand, RedisCommandKind};
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
use std::sync::Arc;
use std::task::{Context, Poll};
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

pub struct RedisTransport<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  server: ArcStr,
  addr: SocketAddr,
  transport: Framed<T, RedisCodec>,
  buffer: CommandBuffer,
  id: Option<i64>,
}

impl<T> RedisTransport<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  pub async fn new_tcp<R>(resolver: R, host: String, port: u16) -> Result<Self<T>, RedisError>
  where
    R: Resolve,
  {
    let (id, buffer) = (None, VecDeque::new());
    let server = ArcStr::from(format!("{}:{}", host, port));
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    let socket = TcpStream::connect(addr).await?;
    let transport = Framed::new(socket, codec);

    Ok(RedisTransport {
      server,
      addr,
      buffer,
      id,
      transport,
    })
  }

  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  pub async fn new_tls<R>(inner: &Arc<RedisClientInner>, host: String, port: u16) -> Result<Self<T>, RedisError>
  where
    R: Resolve,
  {
    let (id, buffer) = (None, VecDeque::with_capacity(256));
    let server = ArcStr::from(format!("{}:{}", host, port));
    let codec = RedisCodec::new(inner, &server);
    let addr = inner.resolver.resolve(host, port).await?;
    let socket = TcpStream::connect(addr).await?;

    // TODO double check this works with both FF's enabled
    match inner.config.tls {
      #[cfg(feature = "enable-native-tls")]
      Some(TlsConnector::Native(ref connector)) => {
        let socket = connector.clone().connect(&host, socket).await?;
        let transport = Framed::new(socket, codec);

        Ok(RedisTransport {
          server,
          addr,
          buffer,
          id,
          transport,
        })
      },
      #[cfg(feature = "enable-rustls")]
      Some(TlsConnector::Rustls(ref connector)) => {
        let server_name: ServerName = host.as_str().try_into()?;
        let socket = connector.clone().connect(server_name, socket).await?;
        let transport = Framed::new(socket, codec);

        Ok(RedisTransport {
          server,
          addr,
          buffer,
          id,
          transport,
        })
      },
      // TODO should this fall back on TCP?
      _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
    };

    Ok(framed)
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

  /// Authenticate, set the protocol version, set the client name, select the provided database, and cache the connection ID.
  pub async fn setup(&mut self, inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
    let _ = self.switch_protocols_and_authenticate(inner).await?;
    let _ = self.select_database(inner).await?;
    let _ = self.cache_connection_id(inner).await?;

    Ok(())
  }

  /// Split the transport into reader/writer halves.
  pub fn split(self) -> (RedisWriter<T>, RedisReader<T>) {
    let buffer = Arc::new(Mutex::new(self.buffer));
    let (server, addr) = (self.server, self.addr);
    let (sink, stream) = self.transport.split();

    let writer = RedisWriter {
      sink,
      server: server.clone(),
      addr: addr.clone(),
      buffer: buffer.clone(),
    };
    let reader = RedisReader { stream, server, buffer };
    (writer, reader)
  }
}

pub struct RedisReader<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  stream: SplitRedisStream<T>,
  server: ArcStr,
  buffer: SharedBuffer,
}

impl<T> RedisReader<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  pub fn into_inner(self) -> (SplitSt, ArcStr, SharedBuffer) {
    (self.stream, self.server, self.buffer)
  }
}

pub struct RedisWriter<T: AsyncRead + AsyncWrite + Unpin + 'static> {
  sink: SplitRedisSink<T>,
  server: ArcStr,
  addr: SocketAddr,
  buffer: SharedBuffer,
}

impl<T> fmt::Debug for RedisWriter<T>
where
  T: AsyncRead + AsyncWrite + Unpic + 'static,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Connection").field("server", &self.server).finish()
  }
}

impl<T> RedisWriter<T>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  // TODO write, etc

  pub async fn write_command(&mut self, cmd: RedisCommand, is_resp3: bool) -> Result<(), RedisError> {
    unimplemented!()
  }

  pub fn close(self) -> CommandBuffer {
    self.buffer.lock().drain(..).collect()
  }
}

// TODO refactor the cluster logic to another file
async fn read_cluster_state(
  inner: &Arc<RedisClientInner>,
  host: String,
  port: u16,
  uses_tls: bool,
) -> Option<ClusterKeyCache> {
  let command = RedisCommand::new(RedisCommandKind::ClusterNodes, vec![], None);

  let addr = match inner.resolver.resolve(host.clone(), port).await {
    Ok(addr) => addr,
    Err(e) => {
      _debug!(inner, "Resolver error: {:?}", e);
      return None;
    },
  };

  let response = if uses_tls {
    let connection = match create_authenticated_connection_tls(&addr, &host, &inner).await {
      Ok(connection) => connection,
      Err(e) => {
        _debug!(inner, "Error creating tls connection to {}:{} => {:?}", host, port, e);
        return None;
      },
    };

    match request_response(connection, &command, inner.is_resp3()).await {
      Ok((frame, _)) => frame.into_resp3(),
      Err(e) => {
        _trace!(inner, "Failed to read cluster state from {}:{} => {:?}", host, port, e);
        return None;
      },
    }
  } else {
    let connection = match create_authenticated_connection(&addr, &inner).await {
      Ok(connection) => connection,
      Err(e) => {
        _debug!(inner, "Error creating connection to {}:{} => {:?}", host, port, e);
        return None;
      },
    };

    match request_response(connection, &command, inner.is_resp3()).await {
      Ok((frame, _)) => frame.into_resp3(),
      Err(e) => {
        _trace!(inner, "Failed to read cluster state from {}:{} => {:?}", host, port, e);
        return None;
      },
    }
  };

  if response.is_error() {
    _trace!(
      inner,
      "Protocol error reading cluster state from {}:{} => {:?}",
      host,
      port,
      response
    );
    return None;
  }
  let cluster_state = match response.as_str() {
    Some(response) => response,
    None => return None,
  };

  _trace!(inner, "Cluster state:\n {}", cluster_state);
  if let Ok(cache) = ClusterKeyCache::new(Some(cluster_state)) {
    return Some(cache);
  }

  None
}

#[allow(dead_code)]
pub async fn read_server_version<T>(
  inner: &Arc<RedisClientInner>,
  transport: Framed<T, RedisCodec>,
) -> Result<(Version, Framed<T, RedisCodec>), RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let command = RedisCommand::new(RedisCommandKind::Info, vec![InfoKind::Server.to_str().into()], None);
  let (result, transport) = request_response(transport, &command, inner.is_resp3()).await?;
  let result = match result.into_resp3() {
    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())?,
    Resp3Frame::SimpleError { data, .. } => return Err(pretty_error(&data)),
    Resp3Frame::BlobError { data, .. } => {
      let parsed = String::from_utf8_lossy(&data);
      return Err(pretty_error(&parsed));
    },
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid server info response.",
      ))
    },
  };

  let version = result.lines().find_map(|line| {
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

  if let Some(version) = version {
    _debug!(inner, "Server version: {}", version);
    Ok((version, transport))
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Failed to read redis server version.",
    ))
  }
}

// TODO
#[allow(dead_code)]
pub async fn read_redis_version(inner: &Arc<RedisClientInner>) -> Result<Version, RedisError> {
  let uses_tls = protocol_utils::uses_tls(inner);

  if client_utils::is_clustered(&inner.config) {
    let known_nodes = protocol_utils::read_clustered_hosts(&inner.config)?;

    for (host, port) in known_nodes.into_iter() {
      let addr = match inner.resolver.resolve(host.clone(), port).await {
        Ok(addr) => addr,
        Err(e) => {
          _debug!(inner, "Resolver error: {:?}", e);
          continue;
        },
      };

      if uses_tls {
        let transport = match create_authenticated_connection_tls(&addr, &host, inner).await {
          Ok(t) => t,
          Err(e) => {
            _warn!(inner, "Error creating connection to {}: {:?}", host, e);
            continue;
          },
        };
        let version = match read_server_version(inner, transport).await {
          Ok((v, _)) => v,
          Err(e) => {
            _warn!(inner, "Error reading server version from {}: {:?}", host, e);
            continue;
          },
        };

        return Ok(version);
      } else {
        let transport = match create_authenticated_connection(&addr, inner).await {
          Ok(t) => t,
          Err(e) => {
            _warn!(inner, "Error creating connection to {}: {:?}", host, e);
            continue;
          },
        };
        let version = match read_server_version(inner, transport).await {
          Ok((v, _)) => v,
          Err(e) => {
            _warn!(inner, "Error reading server version from {}: {:?}", host, e);
            continue;
          },
        };

        return Ok(version);
      }
    }

    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Failed to read server version from any cluster node.",
    ))
  } else {
    let addr = protocol_utils::read_centralized_addr(&inner).await?;

    if uses_tls {
      let domain = protocol_utils::read_centralized_domain(&inner.config)?;
      let transport = create_authenticated_connection_tls(&addr, &domain, inner).await?;
      let (version, _) = read_server_version(inner, transport).await?;
      Ok(version)
    } else {
      let transport = create_authenticated_connection(&addr, inner).await?;
      let (version, _) = read_server_version(inner, transport).await?;
      Ok(version)
    }
  }
}

pub async fn read_cluster_nodes(inner: &Arc<RedisClientInner>) -> Result<ClusterKeyCache, RedisError> {
  let known_nodes = protocol_utils::read_clustered_hosts(&inner.config)?;
  let uses_tls = protocol_utils::uses_tls(inner);

  for (host, port) in known_nodes.into_iter() {
    _debug!(inner, "Attempting to read cluster state from {}:{}", host, port);

    if let Some(cache) = read_cluster_state(inner, host, port, uses_tls).await {
      return Ok(cache);
    }
  }

  Err(RedisError::new(
    RedisErrorKind::Unknown,
    "Could not read cluster state from any known node in the cluster.",
  ))
}

pub async fn write_command(
  inner: &Arc<RedisClientInner>,
  sink: &mut RedisSink,
  counters: &Counters,
  frame: ProtocolFrame,
  should_flush: bool,
) -> Result<(), RedisError> {
  if should_flush {
    _trace!(inner, "Sending command and flushing the sink.");

    match sink {
      RedisSink::Tcp(ref mut inner) => inner.send(frame).await?,
      RedisSink::Tls(ref mut inner) => inner.send(frame).await?,
    };
    counters.reset_feed_count();
  } else {
    _trace!(inner, "Sending command without flushing the sink.");

    match sink {
      RedisSink::Tcp(ref mut inner) => inner.feed(frame).await?,
      RedisSink::Tls(ref mut inner) => inner.feed(frame).await?,
    };
    counters.incr_feed_count();
  };
  counters.incr_in_flight();

  Ok(())
}
