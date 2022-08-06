use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{utils, CloseTx, Connections, Counters, SentCommand};
use crate::protocol::codec::RedisCodec;
use crate::protocol::connection::{self, authenticate, select_database, FramedTcp, FramedTls, RedisTransport};
use crate::protocol::types::{RedisCommand, RedisCommandKind};
use crate::protocol::utils as protocol_utils;
use crate::types::ClientState;
use crate::types::Resolve;
use crate::types::{RedisValue, ServerConfig};
use crate::utils as client_utils;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock as AsyncRwLock;
use tokio_util::codec::Framed;

#[cfg(feature = "enable-native-tls")]
use crate::protocol::tls;

/// The amount of time to wait when trying to connect to the redis server.
///
/// This is a different timeout than the timeout connecting to the sentinel, which is controlled via a global setting.
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 30_000;

macro_rules! try_continue (
  ($inner:ident, $expr:expr) => {
    match $expr {
      Ok(v) => v,
      Err(e) => {
        _warn!($inner, "{:?}", e);
        continue;
      }
    }
  }
);

macro_rules! stry (
  ($expr:expr) => {
    match $expr {
      Ok(r) => r,
      Err(mut e) => {
        e.change_kind(RedisErrorKind::Sentinel);
        return Err(e);
      }
    }
  }
);

#[cfg(feature = "sentinel-auth")]
fn read_sentinel_auth(inner: &Arc<RedisClientInner>) -> Result<(Option<String>, Option<String>), RedisError> {
  match inner.config.read().server {
    ServerConfig::Sentinel {
      ref username,
      ref password,
      ..
    } => Ok((username.clone(), password.clone())),
    _ => Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected sentinel server configuration.",
    )),
  }
}

#[cfg(not(feature = "sentinel-auth"))]
fn read_sentinel_auth(inner: &Arc<RedisClientInner>) -> Result<(Option<String>, Option<String>), RedisError> {
  let guard = inner.config.read();
  Ok((guard.username.clone(), guard.password.clone()))
}

fn read_redis_auth(inner: &Arc<RedisClientInner>) -> (Option<String>, Option<String>) {
  let guard = inner.config.read();
  (guard.username.clone(), guard.password.clone())
}

// TODO clean this up in the next major release by breaking up the connection functions
#[cfg(feature = "enable-native-tls")]
pub async fn create_authenticated_connection_tls(
  addr: &SocketAddr,
  domain: &str,
  inner: &Arc<RedisClientInner>,
  is_sentinel: bool,
) -> Result<FramedTls, RedisError> {
  let server = format!("{}:{}", addr.ip().to_string(), addr.port());
  let codec = RedisCodec::new(inner, server);
  let client_name = inner.client_name();
  let ((username, password), is_resp3) = if is_sentinel {
    (read_sentinel_auth(inner)?, false)
  } else {
    (read_redis_auth(inner), inner.is_resp3())
  };

  let socket = TcpStream::connect(addr).await?;
  let tls_stream = tls::create_tls_connector(&inner.config)?;
  let socket = tls_stream.connect(domain, socket).await?;
  let framed = if is_sentinel {
    Framed::new(socket, codec)
  } else {
    connection::switch_protocols(inner, Framed::new(socket, codec)).await?
  };
  let framed = authenticate(framed, &client_name, username, password, is_resp3).await?;
  let framed = if is_sentinel {
    framed
  } else {
    select_database(inner, framed).await?
  };

  Ok(framed)
}

#[cfg(not(feature = "enable-native-tls"))]
pub(crate) async fn create_authenticated_connection_tls(
  addr: &SocketAddr,
  _domain: &str,
  inner: &Arc<RedisClientInner>,
  is_sentinel: bool,
) -> Result<FramedTls, RedisError> {
  create_authenticated_connection(addr, inner, is_sentinel).await
}

pub async fn create_authenticated_connection(
  addr: &SocketAddr,
  inner: &Arc<RedisClientInner>,
  is_sentinel: bool,
) -> Result<FramedTcp, RedisError> {
  let server = format!("{}:{}", addr.ip().to_string(), addr.port());
  let codec = RedisCodec::new(inner, server);
  let client_name = inner.client_name();
  let ((username, password), is_resp3) = if is_sentinel {
    (read_sentinel_auth(inner)?, false)
  } else {
    (read_redis_auth(inner), inner.is_resp3())
  };

  let socket = TcpStream::connect(addr).await?;
  let framed = if is_sentinel {
    Framed::new(socket, codec)
  } else {
    connection::switch_protocols(inner, Framed::new(socket, codec)).await?
  };
  let framed = authenticate(framed, &client_name, username, password, is_resp3).await?;
  let framed = if is_sentinel {
    framed
  } else {
    select_database(inner, framed).await?
  };

  Ok(framed)
}

async fn connect_to_server(
  inner: &Arc<RedisClientInner>,
  host: &str,
  addr: &SocketAddr,
  timeout: u64,
  is_sentinel: bool,
) -> Result<RedisTransport, RedisError> {
  let uses_tls = inner.config.read().uses_tls();

  let transport = if uses_tls {
    let transport_ft = create_authenticated_connection_tls(addr, host, inner, is_sentinel);
    let transport = stry!(client_utils::apply_timeout(transport_ft, timeout).await);

    RedisTransport::Tls(transport)
  } else {
    let transport_ft = create_authenticated_connection(addr, inner, is_sentinel);
    let transport = stry!(client_utils::apply_timeout(transport_ft, timeout).await);

    RedisTransport::Tcp(transport)
  };

  Ok(transport)
}

async fn read_primary_node_address(
  inner: &Arc<RedisClientInner>,
  transport: RedisTransport,
  server_name: &str,
) -> Result<(RedisTransport, String, SocketAddr), RedisError> {
  let request = RedisCommand::new(
    RedisCommandKind::Sentinel,
    vec!["get-master-addr-by-name".into(), server_name.into()],
    None,
  );
  let (frame, transport) = stry!(connection::transport_request_response(transport, &request, false).await);
  let result = stry!(protocol_utils::frame_to_results(frame.into_resp3()));
  let (host, port): (String, u16) = stry!(result.convert());
  let addr = stry!(inner.resolver.resolve(host.clone(), port).await);

  Ok((transport, host, addr))
}

fn swap_first_sentinel_server(inner: &Arc<RedisClientInner>, idx: usize) {
  if idx != 0 {
    if let ServerConfig::Sentinel { ref mut hosts, .. } = inner.config.write().server {
      hosts.swap(0, idx);
    }
  }
}

pub async fn connect_to_sentinel(
  inner: &Arc<RedisClientInner>,
  host: &str,
  port: u16,
  timeout: u64,
) -> Result<RedisTransport, RedisError> {
  let sentinel_addr = inner.resolver.resolve(host.to_owned(), port).await?;
  _debug!(inner, "Connecting to sentinel {}", sentinel_addr);
  connect_to_server(inner, host, &sentinel_addr, timeout, true).await
}

async fn discover_primary_node(
  inner: &Arc<RedisClientInner>,
) -> Result<(String, RedisTransport, String, SocketAddr), RedisError> {
  let (hosts, name) = client_utils::read_sentinel_host(inner)?;
  let timeout = globals().sentinel_connection_timeout_ms() as u64;

  for (idx, (sentinel_host, port)) in hosts.into_iter().enumerate() {
    let sentinel_transport = try_continue!(inner, connect_to_sentinel(inner, &sentinel_host, port, timeout).await);
    swap_first_sentinel_server(inner, idx);

    let (sentinel_transport, host, addr) = read_primary_node_address(inner, sentinel_transport, &name).await?;
    _debug!(
      inner,
      "Found primary node address {}:{} from sentinel {}:{}",
      addr.ip(),
      addr.port(),
      host,
      port
    );
    return Ok((name, sentinel_transport, host, addr));
  }

  Err(RedisError::new(
    RedisErrorKind::Sentinel,
    "Failed to connect to any sentinel node.",
  ))
}

async fn connect_and_check_primary_role(
  inner: &Arc<RedisClientInner>,
  host: &str,
  addr: &SocketAddr,
) -> Result<RedisTransport, RedisError> {
  let request = RedisCommand::new(RedisCommandKind::Role, vec![], None);
  let transport = stry!(connect_to_server(inner, host, addr, DEFAULT_CONNECTION_TIMEOUT_MS, false).await);

  _debug!(inner, "Checking role for redis server at {}:{}", host, addr.port());
  let is_resp3 = inner.is_resp3();
  let (frame, transport) = stry!(connection::transport_request_response(transport, &request, is_resp3).await);
  let result = stry!(protocol_utils::frame_to_results(frame.into_resp3()));

  if let RedisValue::Array(values) = result {
    if let Some(first) = values.first() {
      let is_master = first.as_str().map(|s| s == "master").unwrap_or(false);

      if is_master {
        Ok(transport)
      } else {
        Err(RedisError::new(
          RedisErrorKind::Sentinel,
          format!("Invalid role: {:?}", first.as_str()),
        ))
      }
    } else {
      Err(RedisError::new(RedisErrorKind::Sentinel, "Invalid role response."))
    }
  } else {
    Err(RedisError::new(
      RedisErrorKind::Sentinel,
      "Could not read redis server role.",
    ))
  }
}

async fn update_connection_id(
  inner: &Arc<RedisClientInner>,
  connection_id: &Arc<RwLock<Option<i64>>>,
  transport: RedisTransport,
) -> Result<RedisTransport, RedisError> {
  let transport = match transport {
    RedisTransport::Tcp(framed) => {
      let framed = match connection::read_client_id(inner, framed).await {
        Ok((id, socket)) => {
          if let Some(id) = id {
            _debug!(inner, "Read sentinel connection ID: {}", id);
            connection_id.write().replace(id);
          }
          socket
        }
        Err((_, socket)) => socket,
      };
      RedisTransport::Tcp(framed)
    }
    RedisTransport::Tls(framed) => {
      let framed = match connection::read_client_id(inner, framed).await {
        Ok((id, socket)) => {
          if let Some(id) = id {
            _debug!(inner, "Read sentinel connection ID: {}", id);
            connection_id.write().replace(id);
          }
          socket
        }
        Err((_, socket)) => socket,
      };
      RedisTransport::Tls(framed)
    }
  };

  Ok(transport)
}

async fn update_sentinel_client_state(
  inner: &Arc<RedisClientInner>,
  server: &Arc<AsyncRwLock<Arc<String>>>,
  counters: &Counters,
  new_server: &Arc<String>,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> CloseTx {
  client_utils::set_locked_async(server, new_server.clone()).await;
  inner.update_sentinel_primary(&new_server);
  counters.reset_feed_count();
  counters.reset_in_flight();
  utils::get_or_create_close_tx(inner, close_tx)
}

async fn connect_centralized(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  host: &str,
  addr: SocketAddr,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<(), RedisError> {
  if let Connections::Centralized {
    ref writer,
    ref server,
    ref counters,
    ref connection_id,
    ref commands,
    ..
  } = connections
  {
    let new_server = Arc::new(format!("{}:{}", host, addr.port()));
    let transport = stry!(connect_and_check_primary_role(inner, host, &addr).await);
    _debug!(inner, "Connected via sentinel to server: {}", new_server);

    let tx = update_sentinel_client_state(inner, server, counters, &new_server, close_tx).await;
    let transport = update_connection_id(inner, connection_id, transport).await?;
    let (sink, stream) = connection::split_transport(transport);

    client_utils::set_locked_async(writer, Some(sink)).await;
    utils::spawn_centralized_listener(
      inner,
      &new_server,
      connections,
      tx.subscribe(),
      commands,
      counters,
      stream,
    );
    _debug!(inner, "Spawned socket reader task for server: {}", new_server);

    Ok(())
  } else {
    Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected centralized connection with sentinel config.",
    ))
  }
}

fn parse_sentinel_nodes_response(
  inner: &Arc<RedisClientInner>,
  value: RedisValue,
) -> Result<Vec<(String, u16)>, RedisError> {
  let result_maps: Vec<HashMap<String, String>> = stry!(value.convert());
  let mut out = Vec::with_capacity(result_maps.len());

  for mut map in result_maps.into_iter() {
    let ip = match map.remove("ip") {
      Some(ip) => ip,
      None => {
        _warn!(inner, "Failed to read IP for sentinel node.");
        return Err(RedisError::new(
          RedisErrorKind::Sentinel,
          "Failed to read sentinel node IP address.",
        ));
      }
    };
    let port = match map.get("port") {
      Some(port) => port.parse::<u16>()?,
      None => {
        _warn!(inner, "Failed to read port for sentinel node.");
        return Err(RedisError::new(
          RedisErrorKind::Sentinel,
          "Failed to read sentinel node port.",
        ));
      }
    };

    out.push((ip, port));
  }
  Ok(out)
}

fn has_different_sentinel_nodes(old: &[(String, u16)], new: &[(String, u16)]) -> bool {
  let mut old_set = HashSet::with_capacity(old.len());
  let mut new_set = HashSet::with_capacity(new.len());

  for (host, port) in old.iter() {
    old_set.insert((host, port));
  }
  for (host, port) in new.iter() {
    new_set.insert((host, port));
  }

  old_set.symmetric_difference(&new_set).count() > 0
}

pub async fn update_sentinel_nodes(
  inner: &Arc<RedisClientInner>,
  transport: RedisTransport,
  name: &str,
) -> Result<(), RedisError> {
  _debug!(inner, "Reading sentinel nodes...");
  let command = RedisCommand::new(RedisCommandKind::Sentinel, vec!["sentinels".into(), name.into()], None);
  let (frame, _) = stry!(connection::transport_request_response(transport, &command, false).await);
  let response = stry!(protocol_utils::frame_to_results(frame.into_resp3()));
  _trace!(inner, "Read sentinel response: {:?}", response);
  let sentinel_nodes = stry!(parse_sentinel_nodes_response(inner, response));

  if !sentinel_nodes.is_empty() {
    if let ServerConfig::Sentinel { ref mut hosts, .. } = inner.config.write().server {
      if has_different_sentinel_nodes(&hosts, &sentinel_nodes) {
        _debug!(
          inner,
          "Updating sentinel nodes from {:?} to {:?}",
          hosts,
          sentinel_nodes
        );
        *hosts = sentinel_nodes;
      } else {
        _debug!(inner, "Sentinel nodes did not change.")
      }
    }
  } else {
    _warn!(inner, "Failed to read any known sentinel nodes.")
  }

  Ok(())
}

/// Use the sentinel API to find the correct primary/main node that should act as the centralized server to the multiplexer.
///
/// See [the documentation](https://redis.io/topics/sentinel-clients) for more information.
pub async fn connect_centralized_from_sentinel(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<VecDeque<SentCommand>, RedisError> {
  let pending_commands = utils::take_sent_commands(connections);
  connections.disconnect_centralized().await;
  client_utils::set_client_state(&inner.state, ClientState::Connecting);

  let (service_name, sentinel_transport, host, primary_addr) = discover_primary_node(inner).await?;
  let _ = connect_centralized(inner, connections, &host, primary_addr, close_tx).await?;
  client_utils::set_client_state(&inner.state, ClientState::Connected);

  if let Err(e) = update_sentinel_nodes(inner, sentinel_transport, &service_name).await {
    _warn!(inner, "Failed to update sentinel nodes with error: {:?}", e);
  };
  Ok(pending_commands)
}
