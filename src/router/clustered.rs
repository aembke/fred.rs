use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RedisCommandKind, RouterCommand, RouterResponse},
    connection::{self, Counters, RedisTransport, RedisWriter, SharedBuffer, SplitStreamKind},
    responders,
    responders::ResponseKind,
    types::{ClusterRouting, Server, SlotRange},
    utils as protocol_utils,
  },
  router::{responses, types::ClusterChange, utils, Connections, Written},
  types::ClusterStateChange,
  utils as client_utils,
};
use futures::future::try_join_all;
use parking_lot::Mutex;
use std::{
  collections::{BTreeSet, HashMap, VecDeque},
  iter::repeat,
  sync::Arc,
};
use tokio::task::JoinHandle;

/// Find the cluster node that should receive the command.
pub fn route_command<'a>(
  inner: &Arc<RedisClientInner>,
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

/// Write a command to the cluster according to the [cluster hashing](https://redis.io/docs/reference/cluster-spec/) interface.
pub async fn write(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<Server, RedisWriter>,
  state: &ClusterRouting,
  command: RedisCommand,
  force_flush: bool,
) -> Written {
  let has_custom_server = command.cluster_node.is_some();
  let server = match route_command(inner, state, &command) {
    Some(server) => server,
    None => {
      return if has_custom_server {
        _debug!(
          inner,
          "Respond to caller with error from missing cluster node override ({:?})",
          command.cluster_node
        );
        command.finish(
          inner,
          Err(RedisError::new(
            RedisErrorKind::Cluster,
            "Missing cluster node override.",
          )),
        );

        Written::Ignore
      } else {
        // these errors usually mean the cluster is partially down or misconfigured
        _warn!(
          inner,
          "Possible cluster misconfiguration. Missing hash slot owner for {:?}.",
          command.cluster_hash()
        );
        Written::NotFound(command)
      };
    },
  };

  if let Some(writer) = writers.get_mut(server) {
    _debug!(inner, "Writing command `{}` to {}", command.kind.to_str_debug(), server);
    utils::write_command(inner, writer, command, force_flush).await
  } else {
    // a reconnect message should already be queued from the reader task
    _debug!(
      inner,
      "Failed to read connection {} for {}",
      server,
      command.kind.to_str_debug()
    );

    Written::Disconnected((
      Some(server.clone()),
      Some(command),
      RedisError::new(RedisErrorKind::IO, "Missing connection."),
    ))
  }
}

/// Send a command to all cluster nodes.
///
/// Note: if any of the commands fail to send the entire command is interrupted.
// There's probably a much cleaner way to express this. Most of the complexity here comes from the need to
// pre-allocate and assign response locations in the buffer ahead of time. This is done to avoid any race conditions.
pub async fn send_all_cluster_command(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<Server, RedisWriter>,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  let num_nodes = writers.len();
  if let ResponseKind::Buffer {
    ref mut frames,
    ref mut expected,
    ..
  } = command.response
  {
    *expected = num_nodes;

    _trace!(
      inner,
      "Allocating {} null responses in buffer for {}.",
      num_nodes,
      command.kind.to_str_debug(),
    );
    let mut guard = frames.lock();
    // pre-allocate responses
    *guard = repeat(Resp3Frame::Null).take(num_nodes).collect();
  }
  let mut responder = match command.response.duplicate() {
    Some(resp) => resp,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::Config,
        "Invalid command response type.",
      ))
    },
  };

  for (idx, (server, writer)) in writers.iter_mut().enumerate() {
    _debug!(
      inner,
      "Sending all cluster command to {} with index {}, ID: {}",
      server,
      idx,
      command.debug_id()
    );
    let mut cmd_responder = responder.duplicate().unwrap_or(ResponseKind::Skip);
    cmd_responder.set_expected_index(idx);
    let mut cmd = command.duplicate(cmd_responder);
    cmd.skip_backpressure = true;

    if let Written::Disconnected((server, _, err)) = utils::write_command(inner, writer, cmd, true).await {
      _debug!(
        inner,
        "Exit all nodes command early ({}/{}: {:?}) from error: {:?}",
        idx + 1,
        num_nodes,
        server,
        err
      );
      responder.respond_with_error(err);
      break;
    }
  }

  Ok(())
}

pub fn parse_cluster_changes(
  cluster_state: &ClusterRouting,
  writers: &HashMap<Server, RedisWriter>,
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

pub fn broadcast_cluster_change(inner: &Arc<RedisClientInner>, changes: &ClusterChange) {
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

/// Spawn a task to read response frames from the reader half of the socket.
#[allow(unused_assignments)]
pub fn spawn_reader_task(
  inner: &Arc<RedisClientInner>,
  mut reader: SplitStreamKind,
  server: &Server,
  buffer: &SharedBuffer,
  counters: &Counters,
  is_replica: bool,
) -> JoinHandle<Result<(), RedisError>> {
  let (inner, server) = (inner.clone(), server.clone());
  let (buffer, counters) = (buffer.clone(), counters.clone());

  tokio::spawn(async move {
    let mut last_error = None;

    loop {
      let frame = match utils::next_frame(&inner, &mut reader, &server, &buffer).await {
        Ok(Some(frame)) => frame.into_resp3(),
        Ok(None) => {
          last_error = None;
          break;
        },
        Err(e) => {
          last_error = Some(e);
          break;
        },
      };

      if let Some(error) = responses::check_special_errors(&inner, &frame) {
        last_error = Some(error);
        break;
      }
      if let Some(frame) = responses::check_pubsub_message(&inner, &server, frame) {
        if let Err(e) = process_response_frame(&inner, &server, &buffer, &counters, frame).await {
          _debug!(
            inner,
            "Error processing clustered response frame from {}: {:?}",
            server,
            e
          );
          last_error = Some(e);
          break;
        }
      }
    }

    // see the centralized variant of this function for more information.
    utils::check_blocked_router(&inner, &buffer, &last_error);
    utils::check_final_write_attempt(&inner, &buffer, &last_error);
    if is_replica {
      responses::broadcast_replica_error(&inner, &server, last_error);
    } else {
      responses::broadcast_reader_error(&inner, &server, last_error);
    }

    _debug!(inner, "Ending reader task from {}", server);
    Ok(())
  })
}

/// Send a MOVED or ASK command to the router, using the router channel if possible and falling back on the
/// command queue if appropriate.
// Cluster errors within a transaction can only be handled via the blocking router channel.
fn process_cluster_error(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  mut command: RedisCommand,
  frame: Resp3Frame,
) {
  // commands are not redirected to replica nodes
  command.use_replica = false;

  let (kind, slot, server_str) = match frame.as_str() {
    Some(data) => match protocol_utils::parse_cluster_error(data) {
      Ok(result) => result,
      Err(e) => {
        command.respond_to_router(inner, RouterResponse::Continue);
        command.respond_to_caller(Err(e));
        return;
      },
    },
    None => {
      command.respond_to_router(inner, RouterResponse::Continue);
      command.respond_to_caller(Err(RedisError::new(RedisErrorKind::Protocol, "Invalid cluster error.")));
      return;
    },
  };
  let server = match Server::from_parts(&server_str, &server.host) {
    Some(server) => server,
    None => {
      _warn!(inner, "Invalid server field in cluster error: {}", server_str);
      command.respond_to_router(inner, RouterResponse::Continue);
      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Invalid cluster redirection error.",
      )));
      return;
    },
  };

  if let Some(tx) = command.take_router_tx() {
    let response = match kind {
      ClusterErrorKind::Ask => RouterResponse::Ask((slot, server, command)),
      ClusterErrorKind::Moved => RouterResponse::Moved((slot, server, command)),
    };

    _debug!(inner, "Sending cluster error to router channel.");
    if let Err(response) = tx.send(response) {
      // if it could not be sent on the router tx then send it on the command channel
      let command = match response {
        RouterResponse::Ask((slot, server, command)) => {
          if command.transaction_id.is_some() {
            _debug!(
              inner,
              "Failed sending ASK cluster error to router in transaction: {}",
              command.kind.to_str_debug()
            );
            // do not send the command to the command queue
            return;
          } else {
            RouterCommand::Ask { slot, server, command }
          }
        },
        RouterResponse::Moved((slot, server, command)) => {
          if command.transaction_id.is_some() {
            _debug!(
              inner,
              "Failed sending MOVED cluster error to router in transaction: {}",
              command.kind.to_str_debug()
            );
            // do not send the command to the command queue
            return;
          } else {
            RouterCommand::Moved { slot, server, command }
          }
        },
        _ => {
          _error!(inner, "Invalid cluster error router response type.");
          return;
        },
      };

      _debug!(inner, "Sending cluster error to command queue.");
      if let Err(e) = interfaces::send_to_router(inner, command) {
        _warn!(inner, "Cannot send MOVED to router channel: {:?}", e);
      }
    }
  } else {
    let command = match kind {
      ClusterErrorKind::Ask => RouterCommand::Ask { slot, server, command },
      ClusterErrorKind::Moved => RouterCommand::Moved { slot, server, command },
    };

    _debug!(inner, "Sending cluster error to command queue.");
    if let Err(e) = interfaces::send_to_router(inner, command) {
      _warn!(inner, "Cannot send ASKED to router channel: {:?}", e);
    }
  }
}

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
pub async fn process_response_frame(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  buffer: &SharedBuffer,
  counters: &Counters,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(inner, "Parsing response frame from {}", server);
  let mut command = match buffer.pop() {
    Some(command) => command,
    None => {
      _debug!(
        inner,
        "Missing last command from {}. Dropping {:?}.",
        server,
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
  counters.decr_in_flight();
  if command.blocks_connection() {
    buffer.set_unblocked();
  }
  responses::check_and_set_unblocked_flag(inner, &command).await;

  if frame.is_moved_or_ask_error() {
    _debug!(
      inner,
      "Recv MOVED or ASK error for `{}` from {}: {:?}",
      command.kind.to_str_debug(),
      server,
      frame.as_str()
    );
    process_cluster_error(inner, server, command, frame);
    return Ok(());
  }

  if command.transaction_id.is_some() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      if let Some(tx) = command.take_router_tx() {
        let _ = tx.send(RouterResponse::TransactionError((error, command)));
      }
      return Ok(());
    } else if command.kind.ends_transaction() {
      command.respond_to_router(inner, RouterResponse::TransactionResult(frame));
      return Ok(());
    } else {
      command.respond_to_router(inner, RouterResponse::Continue);
      return Ok(());
    }
  }

  _trace!(inner, "Handling clustered response kind: {:?}", command.response);
  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => {
      command.respond_to_router(inner, RouterResponse::Continue);
      Ok(())
    },
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, server, command, tx, frame),
    ResponseKind::Buffer {
      received,
      expected,
      frames,
      tx,
      index,
      error_early,
    } => responders::respond_buffer(
      inner,
      server,
      command,
      received,
      expected,
      error_early,
      frames,
      index,
      tx,
      frame,
    ),
    ResponseKind::KeyScan(scanner) => responders::respond_key_scan(inner, server, command, scanner, frame),
    ResponseKind::ValueScan(scanner) => responders::respond_value_scan(inner, server, command, scanner, frame),
  }
}

/// Try connecting to any node in the provided `RedisConfig` or `old_servers`.
pub async fn connect_any(
  inner: &Arc<RedisClientInner>,
  old_cache: Option<&[SlotRange]>,
) -> Result<RedisTransport, RedisError> {
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
  inner: &Arc<RedisClientInner>,
  cache: Option<&ClusterRouting>,
) -> Result<ClusterRouting, RedisError> {
  let (response, host) = {
    let command: RedisCommand = RedisCommandKind::ClusterSlots.into();

    let backchannel_result = {
      // try to use the existing backchannel connection first
      let mut backchannel = inner.backchannel.write().await;
      if let Some(ref mut transport) = backchannel.transport {
        let default_host = transport.default_host.clone();

        _trace!(inner, "Sending backchannel CLUSTER SLOTS to {}", transport.server);
        client_utils::apply_timeout(
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
    let old_cache = cache.map(|cache| cache.slots());

    let command: RedisCommand = RedisCommandKind::ClusterSlots.into();
    let (frame, host) = if let Some((frame, host)) = backchannel_result {
      if frame.is_error() {
        // try connecting to any of the nodes, then try again
        let mut transport = connect_any(inner, old_cache).await?;
        let frame = client_utils::apply_timeout(
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
      let frame = client_utils::apply_timeout(
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

/// Check each connection and remove it from the writer map if it's not [working](RedisWriter::is_working).
pub async fn drop_broken_connections(writers: &mut HashMap<Server, RedisWriter>) -> VecDeque<RedisCommand> {
  let mut new_writers = HashMap::with_capacity(writers.len());
  let mut buffer = VecDeque::new();

  for (server, writer) in writers.drain() {
    if writer.is_working() {
      new_writers.insert(server, writer);
    } else {
      buffer.extend(writer.graceful_close().await);
    }
  }

  *writers = new_writers;
  buffer
}

/// Run `CLUSTER SLOTS`, update the cached routing table, and modify the connection map.
pub async fn sync(
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut VecDeque<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Synchronizing cluster state.");

  if let Connections::Clustered { cache, writers } = connections {
    // send `CLUSTER SLOTS` to any of the cluster nodes via a backchannel
    let state = cluster_slots_backchannel(inner, Some(&*cache)).await?;
    _debug!(inner, "Cluster routing state: {:?}", state.pretty());
    // update the cached routing table
    inner
      .server_state
      .write()
      .kind
      .update_cluster_state(Some(state.clone()));
    *cache = state.clone();

    buffer.extend(drop_broken_connections(writers).await);
    // detect changes to the cluster topology
    let changes = parse_cluster_changes(&state, writers);
    _debug!(inner, "Changing cluster connections: {:?}", changes);
    broadcast_cluster_change(inner, &changes);

    // drop connections that are no longer used
    for removed_server in changes.remove.into_iter() {
      _debug!(inner, "Disconnecting from cluster node {}", removed_server);
      let writer = match writers.remove(&removed_server) {
        Some(writer) => writer,
        None => continue,
      };

      let commands = writer.graceful_close().await;
      buffer.extend(commands);
    }

    let mut connections_ft = Vec::with_capacity(changes.add.len());
    let new_writers = Arc::new(Mutex::new(HashMap::with_capacity(changes.add.len())));
    // connect to each of the new nodes
    for server in changes.add.into_iter() {
      let _inner = inner.clone();
      let _new_writers = new_writers.clone();
      connections_ft.push(async move {
        _debug!(inner, "Connecting to cluster node {}", server);
        let mut transport = connection::create(&_inner, &server, None).await?;
        transport.setup(&_inner, None).await?;

        let (server, writer) = connection::split_and_initialize(&_inner, transport, false, spawn_reader_task)?;
        inner.notifications.broadcast_reconnect(server.clone());
        _new_writers.lock().insert(server, writer);
        Ok::<_, RedisError>(())
      });
    }

    let _ = try_join_all(connections_ft).await?;
    for (server, writer) in new_writers.lock().drain() {
      writers.insert(server, writer);
    }

    _debug!(inner, "Finish synchronizing cluster connections.");
  } else {
    return Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected clustered connections.",
    ));
  }

  if let Some(version) = connections.server_version() {
    inner.server_state.write().kind.set_server_version(version);
  }
  Ok(())
}

/// Initialize fresh connections to the server, dropping any old connections and saving in-flight commands on
/// `buffer`.
pub async fn initialize_connections(
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut VecDeque<RedisCommand>,
) -> Result<(), RedisError> {
  let commands = connections.disconnect_all(inner).await;
  _trace!(inner, "Adding {} commands to retry buffer.", commands.len());
  buffer.extend(commands);
  sync(inner, connections, buffer).await
}
