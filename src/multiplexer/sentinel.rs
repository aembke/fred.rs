use crate::{
  error::{RedisError, RedisErrorKind},
  globals::globals,
  modules::inner::RedisClientInner,
  multiplexer::{centralized, utils, Connections, Counters},
  protocol::{
    codec::RedisCodec,
    command::{RedisCommand, RedisCommandKind},
    connection::{self, CommandBuffer, RedisReader, RedisTransport, RedisWriter},
    utils as protocol_utils,
  },
  types::{ClientState, RedisValue, Resolve, ServerConfig},
  utils as client_utils,
};
use parking_lot::RwLock;
use std::{
  collections::{HashMap, HashSet, VecDeque},
  net::SocketAddr,
  sync::Arc,
};
use tokio::{net::TcpStream, sync::RwLock as AsyncRwLock};
use tokio_util::codec::Framed;

#[cfg(feature = "enable-native-tls")]
use crate::protocol::tls;

/// The amount of time to wait when trying to connect to the redis server.
///
/// This is a different timeout than the timeout connecting to the sentinel, which is controlled via a global setting.
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 30_000;

pub static CONFIG: &'static str = "CONFIG";
pub static SET: &'static str = "SET";
pub static CKQUORUM: &'static str = "CKQUORUM";
pub static FLUSHCONFIG: &'static str = "FLUSHCONFIG";
pub static FAILOVER: &'static str = "FAILOVER";
pub static GET_MASTER_ADDR_BY_NAME: &'static str = "GET-MASTER-ADDR-BY-NAME";
pub static INFO_CACHE: &'static str = "INFO-CACHE";
pub static MASTERS: &'static str = "MASTERS";
pub static MASTER: &'static str = "MASTER";
pub static MONITOR: &'static str = "MONITOR";
pub static MYID: &'static str = "MYID";
pub static PENDING_SCRIPTS: &'static str = "PENDING-SCRIPTS";
pub static REMOVE: &'static str = "REMOVE";
pub static REPLICAS: &'static str = "REPLICAS";
pub static SENTINELS: &'static str = "SENTINELS";
pub static SIMULATE_FAILURE: &'static str = "SIMULATE-FAILURE";

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
      },
    };
    let port = match map.get("port") {
      Some(port) => port.parse::<u16>()?,
      None => {
        _warn!(inner, "Failed to read port for sentinel node.");
        return Err(RedisError::new(
          RedisErrorKind::Sentinel,
          "Failed to read sentinel node port.",
        ));
      },
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

#[cfg(feature = "sentinel-auth")]
fn read_sentinel_auth(inner: &Arc<RedisClientInner>) -> Result<(Option<String>, Option<String>), RedisError> {
  match inner.config.server {
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
  Ok((inner.config.username.clone(), inner.config.password.clone()))
}

/// Read the `(host, port)` tuples for the known sentinel nodes, and the credentials to use when connecting.
fn read_sentinel_nodes_and_auth(
  inner: &Arc<RedisClientInner>,
) -> Result<(Vec<(String, u16)>, (Option<String>, Option<String>)), RedisError> {
  let (username, password) = read_sentinel_auth(inner)?;
  let hosts = match inner.read_sentinel_nodes() {
    Some(hosts) => hosts,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::Sentinel,
        "Failed to read cached sentinel nodes.",
      ))
    },
  };

  Ok((hosts, (username, password)))
}

/// Read the set of sentinel nodes via `SENTINEL sentinels`.
async fn read_sentinels(
  inner: &Arc<RedisClientInner>,
  sentinel: &mut RedisTransport,
) -> Result<Vec<(String, u16)>, RedisError> {
  let command = RedisCommand::new(RedisCommandKind::Sentinel, vec![static_val!(SENTINELS)]);
  let frame = sentinel.request_response(command, false).await?;
  let response = stry!(protocol_utils::frame_to_results(frame));
  _trace!(inner, "Read sentinel `sentinels` response: {:?}", response);
  let sentinel_nodes = stry!(parse_sentinel_nodes_response(inner, response));

  if sentinel_nodes.is_empty() {
    _warn!(inner, "Failed to read any known sentinel nodes.")
  }

  Ok(sentinel_nodes)
}

/// Connect to any of the sentinel nodes provided on the associated `RedisConfig`.
async fn connect_to_sentinel(inner: &Arc<RedisClientInner>) -> Result<RedisTransport, RedisError> {
  let (hosts, (username, password)) = read_sentinel_nodes_and_auth(inner)?;
  let timeout = globals().sentinel_connection_timeout_ms() as u64;

  for (host, port) in hosts.into_iter() {
    _debug!(inner, "Connecting to sentinel {}:{}", host, port);
    let mut transport = try_or_continue!(connection::create(inner, host, port, Some(timeout)).await);
    let _ = try_or_continue!(
      transport
        .authenticate(&inner.id, username.clone(), password.clone(), false)
        .await
    );

    return Ok(transport);
  }

  Err(RedisError::new(
    RedisErrorKind::Sentinel,
    "Failed to connect to all sentinel nodes.",
  ))
}

/// Read the `(host, port)` tuple for the primary Redis server, as identified by the `SENTINEL
/// get-master-addr-by-name` command, then return a connection to that node.
async fn discover_primary_node(
  inner: &Arc<RedisClientInner>,
  sentinel: &mut RedisTransport,
) -> Result<RedisTransport, RedisError> {
  let service_name = match inner.config.server {
    ServerConfig::Sentinel { ref service_name, .. } => service_name,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Sentinel,
        "Missing sentinel service name.",
      ))
    },
  };
  let command = RedisCommand::new(RedisCommandKind::Sentinel, vec![
    static_val!(GET_MASTER_ADDR_BY_NAME),
    service_name.into(),
  ]);
  let frame = sentinel.request_response(command, false).await?;
  let response = stry!(protocol_utils::frame_to_results(frame));
  let (host, port): (String, u16) = if response.is_null() {
    return Err(RedisError::new(
      RedisErrorKind::Sentinel,
      "Missing primary address in response from sentinel node.",
    ));
  } else {
    stry!(response.convert())
  };

  let mut transport = stry!(connection::create(inner, host, port, None).await);
  let _ = stry!(transport.setup(inner).await);
  Ok(transport)
}

/// Verify that the Redis server is a primary node and not a replica.
async fn check_primary_node_role(
  inner: &Arc<RedisClientInner>,
  transport: &mut RedisTransport,
) -> Result<(), RedisError> {
  let command = RedisCommand::new(RedisCommandKind::Role, Vec::new());
  _debug!(inner, "Checking role for redis server at {}", transport.server);

  let frame = stry!(transport.request_response(command, inner.is_resp3()).await);
  let response = stry!(protocol_utils::frame_to_results(frame));

  if let RedisValue::Array(values) = response {
    if let Some(first) = values.first() {
      let is_master = first.as_str().map(|s| s == "master").unwrap_or(false);

      if is_master {
        Ok(())
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

/// Update the cached backchannel state with the new connection information, disconnecting the old connection if
/// needed.
async fn update_sentinel_backchannel(
  inner: &Arc<RedisClientInner>,
  transport: &RedisTransport,
) -> Result<(), RedisError> {
  let mut backchannel = inner.backchannel.write().await;
  backchannel.check_and_disconnect(inner, Some(&transport.server)).await;
  backchannel.connection_ids.clear();
  if let Some(id) = transport.id {
    backchannel.connection_ids.insert(transport.server.clone(), id);
  }

  Ok(())
}

/// Update the cached client and connection state with the latest sentinel state.
///
/// This does the following:
/// * Call `SENTINEL sentinels` on the sentinels
/// * Store the updated sentinel node list on `inner`.
/// * Update the primary node on `inner`.
/// * Update the cached backchannel information.
/// * Split and store the primary node transport on `writer`.
async fn update_cached_client_state(
  inner: &Arc<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  mut sentinel: RedisTransport,
  mut transport: RedisTransport,
) -> Result<(), RedisError> {
  let sentinels = read_sentinels(inner, &mut sentinel).await?;
  inner.update_sentinel_nodes(sentinels);
  inner.update_sentinel_primary(&transport.server);
  update_sentinel_backchannel(inner, &transport).await;

  let (_, _writer) = connection::split_and_initialize(inner, transport, centralized::spawn_reader_task)?;
  *writer = Some(_writer);
  Ok(())
}

/// Initialize fresh connections to the server, dropping any old connections and saving in-flight commands on
/// `buffer`.
///
/// <https://redis.io/docs/reference/sentinel-clients/>
pub async fn initialize_connection(
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut CommandBuffer,
) -> Result<(), RedisError> {
  _debug!(inner, "Initializing sentinel connection.");
  let commands = connections.disconnect_all(inner).await;
  buffer.extend(commands);

  match connections {
    Connections::Sentinel { writer } => {
      let mut sentinel = connect_to_sentinel(inner).await?;
      let mut transport = discover_primary_node(inner, &mut sentinel).await?;
      let _ = check_primary_node_role(inner, &mut transport).await?;
      let _ = update_cached_client_state(inner, writer, sentinel, transport).await?;

      Ok(())
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected sentinel connections.",
    )),
  }
}
