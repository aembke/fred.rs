use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RedisCommandKind},
    connection::{self, ExclusiveConnection, RedisConnection},
    responders,
    responders::ResponseKind,
    types::{ClusterRouting, ProtocolFrame, Server, SlotRange},
    utils as protocol_utils,
  },
  router::{responses, types::ClusterChange, Connections},
  runtime::{Mutex, RefCount},
  types::{ClusterDiscoveryPolicy, ClusterStateChange},
  utils as client_utils,
};
use futures::future::{join_all, try_join_all};
use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, FrameKind, Resp3Frame as _Resp3Frame};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// Find the cluster node that should receive the command.
pub fn route_command<'a>(
  inner: &RefCount<RedisClientInner>,
  state: &'a ClusterRouting,
  command: &RedisCommand,
) -> Option<&'a Server> {
  if let Some(ref server) = command.cluster_node {
    // this `_server` has a lifetime tied to `command`, so we switch `server` to refer to the record in `state` while
    // we check whether that node exists in the cluster. we return None here if the command specifies a server that
    // does not exist in the cluster.
    _trace!(inner, "Routing with custom cluster node: {}", server);
    state.slots().iter().find_map(|slot| {
      if slot.primary == *server {
        Some(&slot.primary)
      } else {
        None
      }
    })
  } else {
    command
      .cluster_hash()
      .and_then(|slot| state.get_server(slot))
      .or_else(|| {
        // for some commands we know they can go to any node, but for others it may depend on the arguments provided.
        if command.args().is_empty() || command.kind.use_random_cluster_node() {
          let node = state.random_node();
          _trace!(
            inner,
            "Using random cluster node `{:?}` for {}",
            node,
            command.kind.to_str_debug()
          );
          node
        } else {
          None
        }
      })
  }
}

async fn write_all_nodes(
  inner: &RefCount<RedisClientInner>,
  writers: &mut HashMap<Server, RedisConnection>,
  frame: &ProtocolFrame,
) -> Vec<Result<Server, RedisError>> {
  let num_nodes = writers.len();
  let mut write_ft = Vec::with_capacity(num_nodes);
  for (idx, (server, conn)) in writers.iter_mut().enumerate() {
    let frame = frame.clone();
    write_ft.push(async move {
      _debug!(inner, "Writing command to {} ({}/{})", server, idx + 1, num_nodes);

      if let Some(err) = conn.peek_reader_errors().await {
        _debug!(inner, "Error sending command: {:?}", err);
        return Err(err);
      }

      if let Err(err) = conn.write(frame, true, true, false).await {
        debug!("{}: Error sending frame to socket: {:?}", conn.server, err);
        Err(err)
      } else {
        Ok(server.clone())
      }
    });
  }

  join_all(write_ft).await
}

async fn read_all_nodes(
  inner: &RefCount<RedisClientInner>,
  writers: &mut HashMap<Server, RedisConnection>,
  filter: &HashSet<Server>,
) -> Vec<Result<Option<Server>, RedisError>> {
  let mut read_ft = Vec::with_capacity(filter.len());
  for server in filter.iter() {
    read_ft.push(async move {
      let conn = match writers.get_mut(&server) {
        Some(conn) => conn,
        None => return Ok(None),
      };

      if let Some(_) = conn.read_skip_pubsub(inner).await? {
        _trace!(inner, "Read all cluster nodes response from {}", server);
      }
      Ok(Some(server.clone()))
    });
  }

  join_all(read_ft).await
}

/// Send a command to all cluster nodes.
///
/// The caller must drain the in-flight buffers before calling this.
pub async fn send_all_cluster_command(
  inner: &RefCount<RedisClientInner>,
  writers: &mut HashMap<Server, RedisConnection>,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  let mut out = Ok(());
  let mut disconnect = Vec::new();
  // write to all the cluster nodes, keeping track of which ones failed, then try to read from the ones that
  // succeeded. at the end disconnect from all the nodes that failed writes or reads and return the last error.
  command.response = ResponseKind::Skip;
  let frame = protocol_utils::encode_frame(inner, &command)?;
  let all_nodes: HashSet<_> = writers.keys().cloned().collect();

  let results = write_all_nodes(inner, writers, &frame).await;
  let write_success: HashSet<_> = results
    .into_iter()
    .filter_map(|r| match r {
      Ok(server) => Some(server),
      Err(e) => {
        out = Err(e);
        None
      },
    })
    .collect();
  let write_failed: Vec<_> = {
    all_nodes
      .difference(&write_success)
      .into_iter()
      .map(|server| {
        disconnect.push(server.clone());
        server
      })
      .collect()
  };
  _debug!(inner, "Failed sending command to {:?}", write_failed);

  // try to read from all nodes concurrently, keeping track of which ones failed
  let results = read_all_nodes(inner, writers, &write_success).await;
  let read_success: HashSet<_> = results
    .into_iter()
    .filter_map(|result| match result {
      Ok(Some(server)) => Some(server),
      Ok(None) => None,
      Err(e) => {
        out = Err(e);
        None
      },
    })
    .collect();
  let read_failed: Vec<_> = {
    all_nodes
      .difference(&read_success)
      .into_iter()
      .map(|server| {
        disconnect.push(server.clone());
        server
      })
      .collect()
  };
  _debug!(inner, "Failed reading responses from {:?}", read_failed);

  // disconnect from all the connections that failed writing or reading
  for server in disconnect.into_iter() {
    let conn = match writers.remove(&server) {
      Some(conn) => conn,
      None => continue,
    };

    // the retry buffer is empty since the caller must drain the connection beforehand in this context
    let result = client_utils::timeout(
      async move {
        let _ = conn.close().await;
        Ok(())
      },
      inner.connection.internal_command_timeout,
    )
    .await;
    if let Err(err) = result {
      _warn!(inner, "Error disconnecting {:?}", err);
    }
  }

  out
}

pub fn parse_cluster_changes(
  cluster_state: &ClusterRouting,
  writers: &HashMap<Server, RedisConnection>,
) -> ClusterChange {
  let mut old_servers = BTreeSet::new();
  let mut new_servers = BTreeSet::new();
  for server in cluster_state.unique_primary_nodes().into_iter() {
    new_servers.insert(server);
  }
  for server in writers.keys() {
    old_servers.insert(server.clone());
  }
  let add = new_servers.difference(&old_servers).cloned().collect();
  let remove = old_servers.difference(&new_servers).cloned().collect();

  ClusterChange { add, remove }
}

pub fn broadcast_cluster_change(inner: &RefCount<RedisClientInner>, changes: &ClusterChange) {
  let mut added: Vec<ClusterStateChange> = changes
    .add
    .iter()
    .map(|server| ClusterStateChange::Add(server.clone()))
    .collect();
  let removed: Vec<ClusterStateChange> = changes
    .remove
    .iter()
    .map(|server| ClusterStateChange::Remove(server.clone()))
    .collect();

  let changes = if added.is_empty() && removed.is_empty() {
    vec![ClusterStateChange::Rebalance]
  } else {
    added.extend(removed);
    added
  };

  inner.notifications.broadcast_cluster_change(changes);
}

/// Parse a cluster redirection frame from the provided server, returning the new destination node info.
pub fn parse_cluster_error_frame(
  inner: &RefCount<RedisClientInner>,
  frame: &Resp3Frame,
  server: &Server,
) -> Result<(ClusterErrorKind, u16, Server), RedisError> {
  let (kind, slot, server_str) = match frame.as_str() {
    Some(data) => protocol_utils::parse_cluster_error(data)?,
    None => return Err(RedisError::new(RedisErrorKind::Protocol, "Invalid cluster error.")),
  };
  let server = match Server::from_parts(&server_str, &server.host) {
    Some(server) => server,
    None => {
      _warn!(inner, "Invalid server field in cluster error: {}", server_str);
      return Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Invalid cluster redirection error.",
      ));
    },
  };

  Ok((kind, slot, server))
}

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
pub async fn process_response_frame(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(inner, "Parsing response frame from {}", conn.server);
  let mut command = match conn.buffer.pop_back() {
    Some(command) => command,
    None => {
      _debug!(
        inner,
        "Missing last command from {}. Dropping {:?}.",
        conn.server,
        frame.kind()
      );
      return Ok(());
    },
  };
  _trace!(
    inner,
    "Checking response to {} ({})",
    command.kind.to_str_debug(),
    command.debug_id()
  );
  conn.counters.decr_in_flight();
  if command.blocks_connection() {
    conn.blocked = false;
  }
  responses::check_and_set_unblocked(inner, &command).await;

  _trace!(inner, "Handling clustered response kind: {:?}", command.response);
  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => Ok(()),
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, &conn.server, command, tx, frame),
    ResponseKind::Buffer {
      received,
      expected,
      frames,
      tx,
      index,
      error_early,
    } => responders::respond_buffer(
      inner,
      &conn.server,
      command,
      received,
      expected,
      error_early,
      frames,
      index,
      tx,
      frame,
    ),
    ResponseKind::KeyScan(scanner) => {
      responders::respond_key_scan(inner, &conn.server, command, scanner, frame).await
    },
    ResponseKind::ValueScan(scanner) => {
      responders::respond_value_scan(inner, &conn.server, command, scanner, frame).await
    },
    ResponseKind::KeyScanBuffered(scanner) => {
      responders::respond_key_scan_buffered(inner, &conn.server, command, scanner, frame).await
    },
  }
}

/// Try connecting to any node in the provided `RedisConfig` or `old_servers`.
pub async fn connect_any(
  inner: &RefCount<RedisClientInner>,
  old_cache: Option<&[SlotRange]>,
) -> Result<ExclusiveConnection, RedisError> {
  let mut all_servers: BTreeSet<Server> = if let Some(old_cache) = old_cache {
    old_cache.iter().map(|slot_range| slot_range.primary.clone()).collect()
  } else {
    BTreeSet::new()
  };
  all_servers.extend(inner.config.server.hosts());
  _debug!(inner, "Attempting clustered connections to any of {:?}", all_servers);

  let num_servers = all_servers.len();
  let mut last_error = None;
  for (idx, server) in all_servers.into_iter().enumerate() {
    let mut connection = match connection::create(inner, &server, None).await {
      Ok(connection) => connection,
      Err(e) => {
        last_error = Some(e);
        continue;
      },
    };

    if let Err(e) = connection.setup(inner, None).await {
      last_error = Some(e);
      continue;
    }
    _debug!(
      inner,
      "Connected to {} ({}/{})",
      connection.server,
      idx + 1,
      num_servers
    );
    return Ok(connection);
  }

  Err(last_error.unwrap_or(RedisError::new(
    RedisErrorKind::Cluster,
    "Failed connecting to any cluster node.",
  )))
}

/// Run the `CLUSTER SLOTS` command on the backchannel, creating a new connection if needed.
///
/// This function will attempt to use the existing backchannel connection, if found. Failing that it will
/// try to connect to any of the cluster nodes as identified in the `RedisConfig` or previous cached state.
///
/// If this returns an error then all known cluster nodes are unreachable.
pub async fn cluster_slots_backchannel(
  inner: &RefCount<RedisClientInner>,
  cache: Option<&ClusterRouting>,
  force_disconnect: bool,
) -> Result<ClusterRouting, RedisError> {
  if force_disconnect {
    inner.backchannel.write().await.check_and_disconnect(inner, None).await;
  }

  let (response, host) = {
    let command: RedisCommand = RedisCommandKind::ClusterSlots.into();

    let backchannel_result = {
      // try to use the existing backchannel connection first
      let mut backchannel = inner.backchannel.write().await;
      if let Some(ref mut transport) = backchannel.transport {
        let default_host = transport.default_host.clone();

        _trace!(inner, "Sending backchannel CLUSTER SLOTS to {}", transport.server);
        client_utils::timeout(
          transport.request_response(command, inner.is_resp3()),
          inner.internal_command_timeout(),
        )
        .await
        .ok()
        .map(|frame| (frame, default_host))
      } else {
        None
      }
    };
    if backchannel_result.is_none() {
      inner.backchannel.write().await.check_and_disconnect(inner, None).await;
    }

    // failing the backchannel, try to connect to any of the user-provided hosts or the last known cluster nodes
    let old_cache = if let Some(policy) = inner.cluster_discovery_policy() {
      match policy {
        ClusterDiscoveryPolicy::ConfigEndpoint => None,
        ClusterDiscoveryPolicy::UseCache => cache.map(|cache| cache.slots()),
      }
    } else {
      cache.map(|cache| cache.slots())
    };

    let command: RedisCommand = RedisCommandKind::ClusterSlots.into();
    let (frame, host) = if let Some((frame, host)) = backchannel_result {
      let kind = frame.kind();

      if matches!(kind, FrameKind::SimpleError | FrameKind::BlobError) {
        // try connecting to any of the nodes, then try again
        let mut transport = connect_any(inner, old_cache).await?;
        let frame = client_utils::timeout(
          transport.request_response(command, inner.is_resp3()),
          inner.internal_command_timeout(),
        )
        .await?;
        let host = transport.default_host.clone();
        inner.update_backchannel(transport).await;

        (frame, host)
      } else {
        // use the response from the backchannel command
        (frame, host)
      }
    } else {
      // try connecting to any of the nodes, then try again
      let mut transport = connect_any(inner, old_cache).await?;
      let frame = client_utils::timeout(
        transport.request_response(command, inner.is_resp3()),
        inner.internal_command_timeout(),
      )
      .await?;
      let host = transport.default_host.clone();
      inner.update_backchannel(transport).await;

      (frame, host)
    };

    (protocol_utils::frame_to_results(frame)?, host)
  };
  _trace!(inner, "Recv CLUSTER SLOTS response: {:?}", response);
  if response.is_null() {
    inner.backchannel.write().await.check_and_disconnect(inner, None).await;
    return Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid or missing CLUSTER SLOTS response.",
    ));
  }

  let mut new_cache = ClusterRouting::new();
  _debug!(inner, "Rebuilding cluster state from host: {}", host);
  new_cache.rebuild(inner, response, &host)?;
  Ok(new_cache)
}

/// Check each connection and remove it from the writer map if it's not working.
pub async fn drop_broken_connections(writers: &mut HashMap<Server, RedisConnection>) -> VecDeque<RedisCommand> {
  let mut new_writers = HashMap::with_capacity(writers.len());
  let mut buffer = VecDeque::new();

  for (server, mut writer) in writers.drain() {
    if writer.peek_reader_errors().await.is_some() {
      buffer.extend(writer.close().await);
    } else {
      new_writers.insert(server, writer);
    }
  }

  *writers = new_writers;
  buffer
}

/// Run `CLUSTER SLOTS`, update the cached routing table, and modify the connection map.
pub async fn sync(
  inner: &RefCount<RedisClientInner>,
  connections: &mut HashMap<Server, RedisConnection>,
  cache: &mut ClusterRouting,
  buffer: &mut VecDeque<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Synchronizing cluster state.");

  // force disconnect if connections is empty or any readers have pending errors
  let force_disconnect = connections.is_empty()
    || join_all(connections.values_mut().map(|c| c.peek_reader_errors()))
      .await
      .into_iter()
      .filter(|err| err.is_some())
      .collect::<Vec<_>>()
      .is_empty();

  let state = cluster_slots_backchannel(inner, Some(&*cache), force_disconnect).await?;
  _debug!(inner, "Cluster routing state: {:?}", state.pretty());
  // update the cached routing table
  inner
    .server_state
    .write()
    .kind
    .update_cluster_state(Some(state.clone()));
  *cache = state.clone();

  buffer.extend(drop_broken_connections(connections).await);
  // detect changes to the cluster topology
  let changes = parse_cluster_changes(&state, connections);
  _debug!(inner, "Changing cluster connections: {:?}", changes);
  broadcast_cluster_change(inner, &changes);

  // drop connections that are no longer used
  for removed_server in changes.remove.into_iter() {
    _debug!(inner, "Disconnecting from cluster node {}", removed_server);
    let writer = match connections.remove(&removed_server) {
      Some(writer) => writer,
      None => continue,
    };

    let commands = writer.close().await;
    buffer.extend(commands);
  }

  let mut connections_ft = Vec::with_capacity(changes.add.len());
  let new_writers = RefCount::new(Mutex::new(HashMap::with_capacity(changes.add.len())));
  // connect to each of the new nodes
  for server in changes.add.into_iter() {
    let _inner = inner.clone();
    let _new_writers = new_writers.clone();
    connections_ft.push(async move {
      _debug!(inner, "Connecting to cluster node {}", server);
      let mut transport = connection::create(&_inner, &server, None).await?;
      transport.setup(&_inner, None).await?;
      let connection = transport.into_pipelined(false);
      inner.notifications.broadcast_reconnect(server.clone());
      _new_writers.lock().insert(server, connection);
      Ok::<_, RedisError>(())
    });
  }

  let _ = try_join_all(connections_ft).await?;
  for (server, writer) in new_writers.lock().drain() {
    connections.insert(server, writer);
  }

  _debug!(inner, "Finish synchronizing cluster connections.");
  if let Some(version) = connections.server_version() {
    inner.server_state.write().kind.set_server_version(version);
  }
  Ok(())
}

/// Initialize fresh connections to the server, dropping any old connections and saving in-flight commands on
/// `buffer`.
pub async fn initialize_connections(
  inner: &RefCount<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut VecDeque<RedisCommand>,
) -> Result<(), RedisError> {
  buffer.extend(connections.disconnect_all(inner).await);

  match connections {
    Connections::Clustered {
      ref mut writers,
      ref mut cache,
    } => sync(inner, writers, cache, buffer).await,
    _ => Err(RedisError::new(RedisErrorKind::Config, "Expected clustered config.")),
  }
}
