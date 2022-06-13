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
use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp3::types::{Frame as Resp3Frame, RespVersion};
use semver::Version;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[cfg(feature = "enable-tls")]
use crate::protocol::tls;
#[cfg(feature = "monitor")]
use crate::types::ServerConfig;
#[cfg(feature = "enable-tls")]
use tokio_native_tls::TlsStream;

/// The contents of a simplestring OK response.
pub const OK: &'static str = "OK";

pub type FramedTcp = Framed<TcpStream, RedisCodec>;
#[cfg(feature = "enable-tls")]
pub type FramedTls = Framed<TlsStream<TcpStream>, RedisCodec>;
#[cfg(not(feature = "enable-tls"))]
pub type FramedTls = FramedTcp;

pub type TcpRedisReader = SplitStream<FramedTcp>;
pub type TcpRedisWriter = SplitSink<FramedTcp, ProtocolFrame>;

pub type TlsRedisReader = SplitStream<FramedTls>;
pub type TlsRedisWriter = SplitSink<FramedTls, ProtocolFrame>;

pub enum RedisStream {
  Tls(TlsRedisReader),
  Tcp(TcpRedisReader),
}

pub enum RedisSink {
  Tls(TlsRedisWriter),
  Tcp(TcpRedisWriter),
}

impl RedisSink {
  pub async fn flush(&mut self) -> Result<(), RedisError> {
    match self {
      RedisSink::Tls(ref mut inner) => inner.flush().await,
      RedisSink::Tcp(ref mut inner) => inner.flush().await,
    }
  }
}

pub enum RedisTransport {
  Tls(FramedTls),
  Tcp(FramedTcp),
}

pub fn null_frame(is_resp3: bool) -> ProtocolFrame {
  if is_resp3 {
    ProtocolFrame::Resp2(Resp2Frame::Null)
  } else {
    ProtocolFrame::Resp3(Resp3Frame::Null)
  }
}

#[cfg(not(feature = "no-client-setname"))]
pub fn is_ok(frame: &ProtocolFrame) -> bool {
  match frame {
    ProtocolFrame::Resp3(ref frame) => match frame {
      Resp3Frame::SimpleString { ref data, .. } => data == OK,
      _ => false,
    },
    ProtocolFrame::Resp2(ref frame) => match frame {
      Resp2Frame::SimpleString(ref data) => data == OK,
      _ => false,
    },
  }
}

pub fn split_transport(transport: RedisTransport) -> (RedisSink, RedisStream) {
  match transport {
    RedisTransport::Tcp(framed) => {
      let (sink, stream) = framed.split();
      (RedisSink::Tcp(sink), RedisStream::Tcp(stream))
    },
    RedisTransport::Tls(framed) => {
      let (sink, stream) = framed.split();
      (RedisSink::Tls(sink), RedisStream::Tls(stream))
    },
  }
}

pub async fn request_response<T>(
  mut transport: Framed<T, RedisCodec>,
  request: &RedisCommand,
  is_resp3: bool,
) -> Result<(ProtocolFrame, Framed<T, RedisCodec>), RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let frame = request.to_frame(is_resp3)?;
  let _ = transport.send(frame).await?;
  let (response, transport) = transport.into_future().await;

  let response = match response {
    Some(result) => result?,
    None => null_frame(is_resp3),
  };
  Ok((response, transport))
}

pub async fn request_response_safe<T>(
  mut transport: Framed<T, RedisCodec>,
  request: &RedisCommand,
  is_resp3: bool,
) -> Result<(ProtocolFrame, Framed<T, RedisCodec>), (RedisError, Framed<T, RedisCodec>)>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let frame = match request.to_frame(is_resp3) {
    Ok(frame) => frame,
    Err(e) => return Err((e, transport)),
  };
  if let Err(e) = transport.send(frame).await {
    return Err((e, transport));
  };
  let (response, transport) = transport.into_future().await;

  let response = match response {
    Some(result) => match result {
      Ok(frame) => frame,
      Err(e) => return Err((e, transport)),
    },
    None => null_frame(is_resp3),
  };

  Ok((response, transport))
}

pub async fn transport_request_response(
  transport: RedisTransport,
  request: &RedisCommand,
  is_resp3: bool,
) -> Result<(ProtocolFrame, RedisTransport), RedisError> {
  match transport {
    RedisTransport::Tcp(transport) => {
      let (frame, transport) = match request_response_safe(transport, request, is_resp3).await {
        Ok(result) => result,
        Err((e, _)) => return Err(e),
      };
      Ok((frame, RedisTransport::Tcp(transport)))
    },
    RedisTransport::Tls(transport) => {
      let (frame, transport) = match request_response_safe(transport, request, is_resp3).await {
        Ok(result) => result,
        Err((e, _)) => return Err(e),
      };
      Ok((frame, RedisTransport::Tls(transport)))
    },
  }
}

#[cfg(not(feature = "no-client-setname"))]
pub async fn set_client_name<T>(
  transport: Framed<T, RedisCodec>,
  name: &str,
  is_resp3: bool,
) -> Result<Framed<T, RedisCodec>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  debug!("{}: Changing client name to {}", name, name);
  let command = RedisCommand::new(RedisCommandKind::ClientSetname, vec![name.into()], None);
  let (response, transport) = request_response(transport, &command, is_resp3).await?;

  if is_ok(&response) {
    debug!("{}: Successfully set Redis client name.", name);
    Ok(transport)
  } else {
    error!("{} Failed to set client name with error {:?}", name, response);
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Failed to set client name.",
    ))
  }
}

#[cfg(feature = "no-client-setname")]
pub async fn set_client_name<T>(
  transport: Framed<T, RedisCodec>,
  name: &str,
  _: bool,
) -> Result<Framed<T, RedisCodec>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  debug!("{}: Skip setting client name.", name);
  Ok(transport)
}

pub async fn authenticate<T>(
  transport: Framed<T, RedisCodec>,
  name: &str,
  username: Option<String>,
  password: Option<String>,
  is_resp3: bool,
) -> Result<Framed<T, RedisCodec>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let transport = if let Some(password) = password {
    let args = if let Some(username) = username {
      vec![username.into(), password.into()]
    } else {
      vec![password.into()]
    };
    let command = RedisCommand::new(RedisCommandKind::Auth, args, None);

    debug!("{}: Authenticating Redis client...", name);
    let (response, transport) = request_response(transport, &command, is_resp3).await?;

    match frame_into_string(response.into_resp3()) {
      Ok(inner) => {
        if inner == OK {
          transport
        } else {
          return Err(RedisError::new(RedisErrorKind::Auth, inner));
        }
      },
      Err(_) => {
        return Err(RedisError::new(
          RedisErrorKind::Auth,
          "Invalid auth response. Expected string.",
        ))
      },
    }
  } else {
    transport
  };

  set_client_name(transport, name, is_resp3).await
}

pub async fn switch_protocols<T>(
  inner: &Arc<RedisClientInner>,
  transport: Framed<T, RedisCodec>,
) -> Result<Framed<T, RedisCodec>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  // reset the protocol version to the one specified by the config when we create new connections
  inner.reset_protocol_version();
  // this is only used when initializing connections, and if the caller has not specified RESP3 then we can skip this
  if !inner.is_resp3() {
    return Ok(transport);
  }

  _debug!(inner, "Switching to RESP3 protocol with HELLO...");
  let cmd = RedisCommand::new(RedisCommandKind::Hello(RespVersion::RESP3), vec![], None);
  let (response, transport) = request_response(transport, &cmd, true).await?;
  let response = protocol_utils::frame_to_results(response.into_resp3())?;

  _debug!(inner, "Recv HELLO response {:?}", response);
  Ok(transport)
}

pub async fn read_client_id<T>(
  inner: &Arc<RedisClientInner>,
  transport: Framed<T, RedisCodec>,
) -> Result<(Option<i64>, Framed<T, RedisCodec>), (RedisError, Framed<T, RedisCodec>)>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let command = RedisCommand::new(RedisCommandKind::ClientID, vec![], None);
  let (result, transport) = request_response_safe(transport, &command, inner.is_resp3()).await?;
  _debug!(inner, "Read client ID: {:?}", result);
  let id = match result.into_resp3() {
    Resp3Frame::Number { data, .. } => Some(data),
    _ => None,
  };

  Ok((id, transport))
}

pub async fn select_database<T>(
  inner: &Arc<RedisClientInner>,
  transport: Framed<T, RedisCodec>,
) -> Result<Framed<T, RedisCodec>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let db = match inner.config.read().database.clone() {
    Some(db) => db,
    None => return Ok(transport),
  };

  _trace!(inner, "Selecting database {} after connecting.", db);
  let command = RedisCommand::new(RedisCommandKind::Select, vec![db.into()], None);
  let (result, transport) = request_response(transport, &command, inner.is_resp3()).await?;
  let response = result.into_resp3();

  if let Some(error) = protocol_utils::frame_to_error(&response) {
    Err(error)
  } else {
    Ok(transport)
  }
}

#[cfg(feature = "enable-tls")]
pub async fn create_authenticated_connection_tls(
  addr: &SocketAddr,
  domain: &str,
  inner: &Arc<RedisClientInner>,
) -> Result<FramedTls, RedisError> {
  let server = format!("{}:{}", addr.ip().to_string(), addr.port());
  let codec = RedisCodec::new(inner, server);
  let client_name = inner.client_name();
  let password = inner.config.read().password.clone();
  let username = inner.config.read().username.clone();

  let socket = TcpStream::connect(addr).await?;
  let tls_stream = tls::create_tls_connector(&inner.config)?;
  let socket = tls_stream.connect(domain, socket).await?;
  let framed = switch_protocols(inner, Framed::new(socket, codec)).await?;
  let framed = authenticate(framed, &client_name, username, password, inner.is_resp3()).await?;
  let framed = select_database(inner, framed).await?;

  client_utils::set_client_state(&inner.state, ClientState::Connected);
  Ok(framed)
}

#[cfg(not(feature = "enable-tls"))]
pub(crate) async fn create_authenticated_connection_tls(
  addr: &SocketAddr,
  _domain: &str,
  inner: &Arc<RedisClientInner>,
) -> Result<FramedTls, RedisError> {
  create_authenticated_connection(addr, inner).await
}

pub async fn create_authenticated_connection(
  addr: &SocketAddr,
  inner: &Arc<RedisClientInner>,
) -> Result<FramedTcp, RedisError> {
  let server = format!("{}:{}", addr.ip().to_string(), addr.port());
  let codec = RedisCodec::new(inner, server);
  let client_name = inner.client_name();
  let password = inner.config.read().password.clone();
  let username = inner.config.read().username.clone();

  let socket = TcpStream::connect(addr).await?;
  let framed = switch_protocols(inner, Framed::new(socket, codec)).await?;
  let framed = authenticate(framed, &client_name, username, password, inner.is_resp3()).await?;
  let framed = select_database(inner, framed).await?;

  client_utils::set_client_state(&inner.state, ClientState::Connected);
  Ok(framed)
}

#[cfg(feature = "monitor")]
pub async fn create_centralized_connection(inner: &Arc<RedisClientInner>) -> Result<RedisTransport, RedisError> {
  let (host, port) = match inner.config.read().server {
    ServerConfig::Centralized { ref host, ref port } => (host.clone(), *port),
    _ => return Err(RedisError::new(RedisErrorKind::Config, "Expected centralized config.")),
  };

  let transport = if inner.config.read().uses_tls() {
    let domain = host.clone();
    let addr = inner.resolver.resolve(host, port).await?;
    let framed = create_authenticated_connection_tls(&addr, &domain, inner).await?;

    RedisTransport::Tls(framed)
  } else {
    let addr = inner.resolver.resolve(host, port).await?;
    let framed = create_authenticated_connection(&addr, inner).await?;

    RedisTransport::Tcp(framed)
  };

  Ok(transport)
}

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
