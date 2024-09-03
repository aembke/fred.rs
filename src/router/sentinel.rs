#![allow(dead_code)]

use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    connection::{self, RedisTransport, RedisWriter},
    utils as protocol_utils,
  },
  router::{centralized, Connections},
  runtime::RefCount,
  types::{RedisValue, Server, ServerConfig},
  utils,
};
use bytes_utils::Str;
use std::collections::{HashMap, HashSet, VecDeque};

pub static CONFIG: &str = "CONFIG";
pub static SET: &str = "SET";
pub static CKQUORUM: &str = "CKQUORUM";
pub static FLUSHCONFIG: &str = "FLUSHCONFIG";
pub static FAILOVER: &str = "FAILOVER";
pub static GET_MASTER_ADDR_BY_NAME: &str = "GET-MASTER-ADDR-BY-NAME";
pub static INFO_CACHE: &str = "INFO-CACHE";
pub static MASTERS: &str = "MASTERS";
pub static MASTER: &str = "MASTER";
pub static MONITOR: &str = "MONITOR";
pub static MYID: &str = "MYID";
pub static PENDING_SCRIPTS: &str = "PENDING-SCRIPTS";
pub static REMOVE: &str = "REMOVE";
pub static REPLICAS: &str = "REPLICAS";
pub static SENTINELS: &str = "SENTINELS";
pub static SIMULATE_FAILURE: &str = "SIMULATE-FAILURE";

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
  inner: &RefCount<RedisClientInner>,
  value: RedisValue,
) -> Result<Vec<Server>, RedisError> {
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

    out.push(Server::new(ip, port));
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
fn read_sentinel_auth(inner: &RefCount<RedisClientInner>) -> Result<(Option<String>, Option<String>), RedisError> {
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
fn read_sentinel_auth(inner: &RefCount<RedisClientInner>) -> Result<(Option<String>, Option<String>), RedisError> {
  Ok((inner.config.username.clone(), inner.config.password.clone()))
}

/// Read the `(host, port)` tuples for the known sentinel nodes, and the credentials to use when connecting.
fn read_sentinel_nodes_and_auth(
  inner: &RefCount<RedisClientInner>,
) -> Result<(Vec<Server>, (Option<String>, Option<String>)), RedisError> {
  let (username, password) = read_sentinel_auth(inner)?;
  let hosts = match inner.server_state.read().kind.read_sentinel_nodes(&inner.config.server) {
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
  inner: &RefCount<RedisClientInner>,
  sentinel: &mut RedisTransport,
) -> Result<Vec<Server>, RedisError> {
  let service_name = read_service_name(inner)?;

  let command = RedisCommand::new(RedisCommandKind::Sentinel, vec![
    static_val!(SENTINELS),
    service_name.into(),
  ]);
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
async fn connect_to_sentinel(inner: &RefCount<RedisClientInner>) -> Result<RedisTransport, RedisError> {
  let (hosts, (username, password)) = read_sentinel_nodes_and_auth(inner)?;

  for server in hosts.into_iter() {
    _debug!(inner, "Connecting to sentinel {}", server);
    let mut transport = try_or_continue!(connection::create(inner, &server, None).await);
    try_or_continue!(
      utils::timeout(
        transport.authenticate(&inner.id, username.clone(), password.clone(), false),
        inner.internal_command_timeout()
      )
      .await
    );

    return Ok(transport);
  }

  Err(RedisError::new(
    RedisErrorKind::Sentinel,
    "Failed to connect to all sentinel nodes.",
  ))
}

fn read_service_name(inner: &RefCount<RedisClientInner>) -> Result<String, RedisError> {
  match inner.config.server {
    ServerConfig::Sentinel { ref service_name, .. } => Ok(service_name.to_owned()),
    _ => Err(RedisError::new(
      RedisErrorKind::Sentinel,
      "Missing sentinel service name.",
    )),
  }
}

/// Read the `(host, port)` tuple for the primary Redis server, as identified by the `SENTINEL
/// get-master-addr-by-name` command, then return a connection to that node.
async fn discover_primary_node(
  inner: &RefCount<RedisClientInner>,
  sentinel: &mut RedisTransport,
) -> Result<RedisTransport, RedisError> {
  let service_name = read_service_name(inner)?;
  let command = RedisCommand::new(RedisCommandKind::Sentinel, vec![
    static_val!(GET_MASTER_ADDR_BY_NAME),
    service_name.into(),
  ]);
  let frame = utils::timeout(
    sentinel.request_response(command, false),
    inner.internal_command_timeout(),
  )
  .await?;
  let response = stry!(protocol_utils::frame_to_results(frame));
  let server = if response.is_null() {
    return Err(RedisError::new(
      RedisErrorKind::Sentinel,
      "Missing primary address in response from sentinel node.",
    ));
  } else {
    let (host, port): (Str, u16) = stry!(response.convert());
    Server {
      host,
      port,
      #[cfg(any(
        feature = "enable-rustls",
        feature = "enable-native-tls",
        feature = "enable-rustls-ring"
      ))]
      tls_server_name: None,
    }
  };

  let mut transport = stry!(connection::create(inner, &server, None).await);
  stry!(transport.setup(inner, None).await);
  Ok(transport)
}

/// Verify that the Redis server is a primary node and not a replica.
async fn check_primary_node_role(
  inner: &RefCount<RedisClientInner>,
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
  inner: &RefCount<RedisClientInner>,
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
  inner: &RefCount<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  mut sentinel: RedisTransport,
  transport: RedisTransport,
) -> Result<(), RedisError> {
  let sentinels = read_sentinels(inner, &mut sentinel).await?;
  inner
    .server_state
    .write()
    .kind
    .update_sentinel_nodes(&transport.server, sentinels);
  let _ = update_sentinel_backchannel(inner, &transport).await;

  let (_, _writer) = connection::split(inner, transport, false, centralized::spawn_reader_task)?;
  *writer = Some(_writer);
  Ok(())
}

/// Initialize fresh connections to the server, dropping any old connections and saving in-flight commands on
/// `buffer`.
///
/// <https://redis.io/docs/reference/sentinel-clients/>
pub async fn initialize_connection(
  inner: &RefCount<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut VecDeque<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Initializing sentinel connection.");
  let commands = connections.disconnect_all(inner).await;
  buffer.extend(commands);

  match connections {
    Connections::Sentinel { writer } => {
      let mut sentinel = connect_to_sentinel(inner).await?;
      let mut transport = discover_primary_node(inner, &mut sentinel).await?;
      let server = transport.server.clone();

      utils::timeout(
        Box::pin(async {
          check_primary_node_role(inner, &mut transport).await?;
          update_cached_client_state(inner, writer, sentinel, transport).await?;
          Ok::<_, RedisError>(())
        }),
        inner.internal_command_timeout(),
      )
      .await?;

      inner.notifications.broadcast_reconnect(server);
      Ok(())
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected sentinel connections.",
    )),
  }
}
