use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::modules::inner::RedisClientInner;
use crate::modules::types::ClientState;
use crate::multiplexer::{utils, CloseTx, Connections, SentCommand};
use crate::protocol::codec::RedisCodec;
use crate::protocol::connection::{
  self, create_authenticated_connection, create_authenticated_connection_tls, request_response_safe,
  transport_request_response, RedisSink, RedisStream, RedisTransport,
};
use crate::protocol::types::{RedisCommand, RedisCommandKind};
use crate::protocol::utils as protocol_utils;
use crate::types::Resolve;
use crate::types::{RedisConfig, RedisValue, ServerConfig};
use crate::utils as client_utils;
use futures::StreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock as AsyncRwLock;

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

fn read_sentinel_hosts(inner: &Arc<RedisClientInner>) -> Result<(String, Vec<(String, u16)>), RedisError> {
  if let ServerConfig::Sentinel {
    ref hosts,
    ref service_name,
  } = inner.config.read().server
  {
    Ok((service_name.to_owned(), hosts.to_vec()))
  } else {
    Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected sentinel server config.",
    ))
  }
}

async fn connect_to_server(
  inner: &Arc<RedisClientInner>,
  host: &str,
  addr: &SocketAddr,
  timeout: u64,
) -> Result<RedisTransport, RedisError> {
  let uses_tls = inner.config.read().tls.is_some();

  let transport = if uses_tls {
    let transport_ft = create_authenticated_connection_tls(addr, host, inner);
    let transport = stry!(client_utils::apply_timeout(transport_ft, timeout).await);

    RedisTransport::Tls(transport)
  } else {
    let transport_ft = create_authenticated_connection(addr, inner);
    let transport = stry!(client_utils::apply_timeout(transport_ft, timeout).await);

    RedisTransport::Tcp(transport)
  };

  Ok(transport)
}

async fn read_primary_node_address(
  inner: &Arc<RedisClientInner>,
  transport: RedisTransport,
  server_name: &str,
) -> Result<(RedisTransport, SocketAddr), RedisError> {
  let request = RedisCommand::new(
    RedisCommandKind::Sentinel,
    vec!["get-master-addr-by-name".into(), server_name.into()],
    None,
  );
  let (frame, transport) = stry!(transport_request_response(transport, &request).await);
  let result = stry!(protocol_utils::frame_to_results(frame));
  let (host, port): (String, u16) = stry!(result.convert());
  let addr = stry!(inner.resolver.resolve(host, port).await);

  Ok((transport, addr))
}

fn swap_first_sentinel_server(inner: &Arc<RedisClientInner>, idx: usize) {
  if idx != 0 {
    if let ServerConfig::Sentinel { ref mut hosts, .. } = inner.config.write().server {
      hosts.swap(0, idx);
    }
  }
}

async fn discover_primary_node(
  inner: &Arc<RedisClientInner>,
) -> Result<(String, RedisTransport, String, SocketAddr), RedisError> {
  let (name, hosts) = read_sentinel_hosts(inner)?;
  let timeout = globals().sentinel_connection_timeout_ms() as u64;

  for (idx, (host, port)) in hosts.into_iter().enumerate() {
    let sentinel_addr = inner.resolver.resolve(host.clone(), port).await?;
    _debug!(inner, "Connecting to sentinel {}", sentinel_addr);
    let sentinel_transport = try_continue!(inner, connect_to_server(inner, &host, &sentinel_addr, timeout).await);
    swap_first_sentinel_server(inner, idx);

    let (sentinel_transport, addr) = read_primary_node_address(inner, sentinel_transport, &name).await?;
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
    "Failed to connect to all sentinel nodes.",
  ))
}

async fn connect_and_check_primary_role(
  inner: &Arc<RedisClientInner>,
  host: &str,
  addr: SocketAddr,
) -> Result<RedisTransport, RedisError> {
  let request = RedisCommand::new(RedisCommandKind::Role, vec![], None);
  let transport = stry!(connect_to_server(inner, host, &addr, DEFAULT_CONNECTION_TIMEOUT_MS).await);
  let (frame, transport) = stry!(transport_request_response(transport, &request).await);
  let result = stry!(protocol_utils::frame_to_results(frame));

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
      "Could not read redis node role.",
    ))
  }
}

async fn update_server_name(server: &Arc<AsyncRwLock<Arc<String>>>, new_server: String) {
  let mut server_guard = server.write().await;
  *server_guard = Arc::new(new_server);
}

async fn update_connection_id(
  inner: &Arc<RedisClientInner>,
  connection_id: &Arc<RwLock<Option<i64>>>,
  transport: RedisTransport,
) -> Result<RedisTransport, RedisError> {
  unimplemented!()
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
    ..
  } = connections
  {
    let new_server = format!("{}:{}", addr.ip(), addr.port());
    let transport = stry!(connect_and_check_primary_role(inner, host, addr).await);

    update_server_name(server, new_server).await;
    let transport = update_connection_id(inner, connection_id, transport).await?;
    let (sink, stream) = connection::split_transport(transport);

    // read client IDs, update those
    // reset counters
    // get or create close tx
    // set sink on writer
    // spawn centralized listener

    unimplemented!()
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

async fn update_sentinel_nodes(
  inner: &Arc<RedisClientInner>,
  transport: RedisTransport,
  name: String,
) -> Result<(), RedisError> {
  _debug!(inner, "Reading sentinel nodes...");
  let command = RedisCommand::new(RedisCommandKind::Sentinel, vec!["sentinels".into(), name.into()], None);
  let (frame, transport) = stry!(transport_request_response(transport, &command).await);
  _trace!(inner, "Read sentinel response: {:?}", frame);
  let response = stry!(protocol_utils::frame_to_results(frame));
  let sentinel_nodes = stry!(parse_sentinel_nodes_response(inner, response));

  // TODO dont update if there's no difference between old and new

  if !sentinel_nodes.is_empty() {
    if let ServerConfig::Sentinel { ref mut hosts, .. } = inner.config.write().server {
      _debug!(
        inner,
        "Updating sentinel nodes from {:?} to {:?}",
        hosts,
        sentinel_nodes
      );
      *hosts = sentinel_nodes;
    }
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
  let pending_commands = utils::take_sent_commands(connections).await;
  connections.disconnect_centralized();
  client_utils::set_client_state(&inner.state, ClientState::Connecting);

  let (service_name, sentinel_transport, host, primary_addr) = discover_primary_node(inner).await?;
  let _ = connect_centralized(inner, connections, &host, primary_addr, close_tx).await?;
  client_utils::set_client_state(&inner.state, ClientState::Connected);
  utils::emit_connect(inner);
  utils::emit_reconnect(inner);

  let _ = update_sentinel_nodes(inner, sentinel_transport, service_name).await;
  // TODO make this work for a backchannel
  Ok(pending_commands)
}
