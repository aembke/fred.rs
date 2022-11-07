use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  multiplexer::{responses, types::ClusterChange, utils, Connections, Written},
  protocol::{
    command::{ClusterErrorKind, MultiplexerCommand, MultiplexerResponse, RedisCommand, RedisCommandKind},
    connection::{self, CommandBuffer, Counters, RedisTransport, RedisWriter, SharedBuffer, SplitStreamKind},
    responders,
    responders::ResponseKind,
    types::ClusterRouting,
    utils as protocol_utils,
    utils::server_to_parts,
  },
  types::ClusterStateChange,
};
use arcstr::ArcStr;
use futures::TryStreamExt;
use std::{
  collections::{BTreeSet, HashMap},
  sync::Arc,
};
use tokio::task::JoinHandle;

pub fn find_cluster_node<'a>(
  inner: &Arc<RedisClientInner>,
  state: &'a ClusterRouting,
  command: &RedisCommand,
) -> Option<&'a ArcStr> {
  command
    .cluster_hash()
    .and_then(|slot| state.get_server(slot))
    .or_else(|| {
      let node = state.random_node();
      _trace!(
        inner,
        "Using random cluster node `{:?}` for {}",
        node,
        command.kind.to_str_debug()
      );
      node
    })
}

/// Write a command to the cluster according to the [cluster hashing](https://redis.io/docs/reference/cluster-spec/) interface.
pub async fn send_command(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<ArcStr, RedisWriter>,
  state: &ClusterRouting,
  mut command: RedisCommand,
) -> Result<Written, (RedisError, RedisCommand)> {
  let server = match find_cluster_node(inner, state, &command) {
    Some(server) => server,
    None => {
      // these errors almost always mean the cluster is misconfigured
      _warn!(
        inner,
        "Possible cluster misconfiguration. Missing hash slot owner for {:?}",
        command.cluster_hash()
      );
      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Missing cluster hash slot owner.",
      )));
      return Ok(Written::Ignore);
    },
  };

  if let Some(writer) = writers.get_mut(server) {
    _debug!(inner, "Writing command `{}` to {}", command.kind.to_str_debug(), server);
    Ok(utils::write_command(inner, writer, command, false).await)
  } else {
    // a reconnect message should already be queued from the reader task
    _debug!(
      inner,
      "Failed to read connection {} for {}",
      server,
      command.kind.to_str_debug()
    );
    Err((RedisError::new(RedisErrorKind::IO, "Missing connection."), command))
  }
}

/// Send a command to all cluster nodes.
///
/// Note: if any of the commands fail to send the entire command is interrupted.
pub async fn send_all_cluster_command(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<ArcStr, RedisWriter>,
  command: RedisCommand,
) -> Result<(), RedisError> {
  let num_nodes = writers.len();
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
    _debug!(inner, "Sending all cluster command to {}", server);
    let mut cmd_responder = responder.duplicate().unwrap_or(ResponseKind::Skip);
    cmd_responder.set_expected_index(idx);
    let mut cmd = command.duplicate(cmd_responder);
    cmd.skip_backpressure = true;

    if let Written::Disconnect((server, _, err)) = utils::write_command(inner, writer, cmd, true).await {
      _debug!(
        inner,
        "Exit all nodes command early ({}/{}: {}) from error: {:?}",
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
  writers: &HashMap<ArcStr, RedisWriter>,
) -> ClusterChange {
  let mut old_servers = BTreeSet::new();
  let mut new_servers = BTreeSet::new();
  for server in cluster_state.unique_primary_nodes().into_iter() {
    new_servers.insert(server);
  }
  for server in writers.keys() {
    old_servers.insert(server.clone());
  }
  let add = new_servers.difference(&old_servers).map(|s| s.clone()).collect();
  let remove = old_servers.difference(&new_servers).map(|s| s.clone()).collect();

  ClusterChange { add, remove }
}

pub fn broadcast_cluster_change(inner: &Arc<RedisClientInner>, changes: &ClusterChange) {
  let mut added: Vec<ClusterStateChange> = changes
    .add
    .iter()
    .filter_map(|server| {
      protocol_utils::server_to_parts(server)
        .ok()
        .map(|(h, p)| ClusterStateChange::Add((h.to_owned(), p)))
    })
    .collect();
  let removed: Vec<ClusterStateChange> = changes
    .remove
    .iter()
    .filter_map(|server| {
      protocol_utils::server_to_parts(server)
        .ok()
        .map(|(h, p)| ClusterStateChange::Remove((h.to_owned(), p)))
    })
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
  server: &ArcStr,
  buffer: &SharedBuffer,
  counters: &Counters,
) -> JoinHandle<Result<(), RedisError>> {
  let (inner, server) = (inner.clone(), server.clone());
  let (buffer, counters) = (buffer.clone(), counters.clone());

  tokio::spawn(async move {
    let mut last_error = None;

    loop {
      let frame = match reader.try_next().await {
        Ok(Some(frame)) => frame.into_resp3(),
        Ok(None) => {
          // FIXME make sure try_next return this when the connection closes without an error
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
      if let Some(frame) = responses::check_pubsub_message(&inner, frame) {
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

    utils::check_blocked_multiplexer(&inner, &buffer, &last_error);
    utils::check_final_write_attempt(&inner, &buffer, &last_error);
    responses::handle_reader_error(&inner, &server, last_error);

    _debug!(inner, "Ending reader task from {}", server);
    Ok(())
  })
}

/// Send a MOVED or ASK command to the multiplexer, using the multiplexer channel if possible and falling back on the
/// command queue if appropriate.
///
/// Note: Cluster errors within a transaction can only be handled via the blocking multiplexer channel.
fn process_cluster_error(inner: &Arc<RedisClientInner>, mut command: RedisCommand, frame: Resp3Frame) {
  let (kind, slot, server) = match frame.as_str() {
    Some(data) => match protocol_utils::parse_cluster_error(data) {
      Ok(result) => result,
      Err(e) => {
        command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
        command.respond_to_caller(Err(e));
        return;
      },
    },
    None => {
      command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
      command.respond_to_caller(Err(RedisError::new(RedisErrorKind::Protocol, "Invalid cluster error.")));
      return;
    },
  };

  if let Some(tx) = command.multiplexer_tx.take() {
    let response = match kind {
      ClusterErrorKind::Ask => MultiplexerResponse::Ask((slot, server, command)),
      ClusterErrorKind::Moved => MultiplexerResponse::Moved((slot, server, command)),
    };

    _debug!(inner, "Sending cluster error to multiplexer channel.");
    if let Err(response) = tx.send(response) {
      // if it could not be sent on the multiplexer tx then send it on the command channel
      let command = match response {
        MultiplexerResponse::Ask((slot, server, command)) => {
          if command.transaction_id.is_some() {
            _debug!(
              inner,
              "Failed sending ASK cluster error to multiplexer in transaction: {}",
              command.kind.to_str_debug()
            );
            // do not send the command to the command queue
            return;
          } else {
            MultiplexerCommand::Ask { slot, server, command }
          }
        },
        MultiplexerResponse::Moved((slot, server, command)) => {
          if command.transaction_id.is_some() {
            _debug!(
              inner,
              "Failed sending MOVED cluster error to multiplexer in transaction: {}",
              command.kind.to_str_debug()
            );
            // do not send the command to the command queue
            return;
          } else {
            MultiplexerCommand::Moved { slot, server, command }
          }
        },
        _ => {
          _error!(inner, "Invalid cluster error multiplexer response type.");
          return;
        },
      };

      _debug!(inner, "Sending cluster error to command queue.");
      if let Err(e) = interfaces::send_to_multiplexer(inner, command) {
        _warn!(inner, "Cannot send MOVED to multiplexer channel: {:?}", e);
      }
    }
  } else {
    let command = match kind {
      ClusterErrorKind::Ask => MultiplexerCommand::Ask { slot, server, command },
      ClusterErrorKind::Moved => MultiplexerCommand::Moved { slot, server, command },
    };

    _debug!(inner, "Sending cluster error to command queue.");
    if let Err(e) = interfaces::send_to_multiplexer(inner, command) {
      _warn!(inner, "Cannot send ASKED to multiplexer channel: {:?}", e);
    }
  }
}

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
pub async fn process_response_frame(
  inner: &Arc<RedisClientInner>,
  server: &ArcStr,
  buffer: &SharedBuffer,
  counters: &Counters,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  let mut command = {
    let mut guard = buffer.lock();

    let command = match guard.pop_front() {
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

    if utils::should_drop_extra_pubsub_frame(inner, &command, &frame) {
      guard.push_front(command);
      return Ok(());
    } else {
      command
    }
  };
  counters.decr_in_flight();
  responses::check_and_set_unblocked_flag(inner, &command).await;

  if frame.is_moved_or_ask_error() {
    _debug!(
      inner,
      "Recv MOVED or ASK error for `{}` from {}",
      command.kind.to_str_debug(),
      server
    );
    process_cluster_error(inner, command, frame);
    return Ok(());
  }

  if command.transaction_id.is_some() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      if let Some(tx) = command.take_multiplexer_tx() {
        let _ = tx.send(MultiplexerResponse::TransactionError((error, command)));
      }
      return Ok(());
    } else {
      if command.kind.ends_transaction() {
        command.respond_to_multiplexer(inner, MultiplexerResponse::TransactionResult(frame));
        return Ok(());
      } else {
        command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
        return Ok(());
      }
    }
  }

  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => {
      command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
      Ok(())
    },
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, server, command, tx, frame),
    ResponseKind::Multiple { received, expected, tx } => {
      if let Some(command) = responders::respond_multiple(inner, server, command, received, expected, tx, frame)? {
        // the `Multiple` policy works by processing a series of responses on the same connection. the response
        // channel is not shared across commands (since there's only one), so we re-queue it while waiting on
        // response frames.
        buffer.lock().push_front(command);
        counters.incr_in_flight();
      }

      Ok(())
    },
    ResponseKind::Buffer {
      received,
      expected,
      frames,
      tx,
      index,
    } => responders::respond_buffer(inner, server, command, received, expected, frames, index, tx, frame),
    ResponseKind::KeyScan(scanner) => responders::respond_key_scan(inner, server, command, scanner, frame),
    ResponseKind::ValueScan(scanner) => responders::respond_value_scan(inner, server, command, scanner, frame),
  }
}

/// Try connecting to any node in the provided `RedisConfig` or `old_servers`.
pub async fn connect_any(
  inner: &Arc<RedisClientInner>,
  old_servers: Vec<ArcStr>,
) -> Result<RedisTransport, RedisError> {
  let mut all_servers: BTreeSet<(String, u16)> = old_servers
    .into_iter()
    .filter_map(|server| {
      server_to_parts(&server)
        .ok()
        .map(|(host, port)| (host.to_owned(), port))
    })
    .collect();
  all_servers.extend(
    inner
      .config
      .server
      .hosts()
      .into_iter()
      .map(|(host, port)| (host.to_owned(), port)),
  );
  _debug!(inner, "Attempting clustered connections to any of {:?}", all_servers);

  let num_servers = all_servers.len();
  for (idx, (host, port)) in all_servers.into_iter().enumerate() {
    let mut connection = try_or_continue!(connection::create(inner, host, port, None).await);
    let _ = try_or_continue!(connection.setup(inner).await);

    _debug!(
      inner,
      "Connected to {} ({}/{})",
      connection.server,
      idx + 1,
      num_servers
    );
    return Ok(connection);
  }

  Err(RedisError::new(
    RedisErrorKind::Cluster,
    "Failed connecting to any cluster node.",
  ))
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

    let frame = {
      // try to use the existing backchannel connection first
      let mut backchannel = inner.backchannel.write().await;
      if let Some(server) = backchannel.current_server() {
        if let Some(ref transport) = backchannel.transport {
          let default_host = transport.default_host.clone();

          backchannel
            .request_response(inner, &server, command)
            .await
            .ok()
            .map(|frame| (frame, default_host))
        } else {
          None
        }
      } else {
        None
      }
    };

    // failing the backchannel, try to connect to any of the user-provided hosts or the last known cluster nodes
    let old_hosts = cache.map(|cache| cache.unique_primary_nodes()).unwrap_or(Vec::new());

    let command: RedisCommand = RedisCommandKind::ClusterSlots.into();
    let (frame, host) = if let Some((frame, host)) = frame {
      if frame.is_error() {
        // try connecting to any of the nodes, then try again
        let mut transport = connect_any(inner, old_hosts).await?;
        let frame = transport.request_response(command, inner.is_resp3()).await?;
        let host = transport.default_host.clone();
        inner.update_backchannel(transport).await;

        (frame, host)
      } else {
        // use the response from the backchannel command
        (frame, host)
      }
    } else {
      // try connecting to any of the nodes, then try again
      let mut transport = connect_any(inner, old_hosts).await?;
      let frame = transport.request_response(command, inner.is_resp3()).await?;
      let host = transport.default_host.clone();
      inner.update_backchannel(transport).await;

      (frame, host)
    };

    (protocol_utils::frame_to_results_raw(frame)?, host)
  };

  let mut new_cache = ClusterRouting::new();
  _debug!(inner, "Rebuilding cluster state from host: {}", host);
  new_cache.rebuild(response, host.as_str())?;
  Ok(new_cache)
}

/// Run `CLUSTER SLOTS`, update the cached routing table, and modify the connection map.
pub async fn sync(
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut CommandBuffer,
) -> Result<(), RedisError> {
  _debug!(inner, "Synchronizing cluster state.");

  if let Connections::Clustered { cache, writers } = connections {
    // send `CLUSTER SLOTS` to any of the cluster nodes via a backchannel
    let state = cluster_slots_backchannel(inner, Some(&*cache)).await?;
    // update the cached routing table
    inner.update_cluster_state(Some(state.clone()));
    *cache = state.clone();

    // detect changes to the cluster topology
    let changes = parse_cluster_changes(&state, &writers);
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

    // connect to each of the new nodes
    for server in changes.add.into_iter() {
      _debug!(inner, "Connecting to cluster node {}", server);
      let (host, port) = protocol_utils::server_to_parts(&server)?;
      let mut transport = connection::create(inner, host.to_owned(), port, None).await?;
      let _ = transport.setup(inner).await?;

      let (server, writer) = connection::split_and_initialize(inner, transport, spawn_reader_task)?;
      writers.insert(server, writer);
    }

    _debug!(inner, "Finish synchronizing cluster connections.");
    Ok(())
  } else {
    Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected clustered connections.",
    ))
  }
}

/// Initialize fresh connections to the server, dropping any old connections and saving in-flight commands on
/// `buffer`.
pub async fn initialize_connections(
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut CommandBuffer,
) -> Result<(), RedisError> {
  let commands = connections.disconnect_all(inner).await;
  buffer.extend(commands);
  sync(inner, connections, buffer).await
}
