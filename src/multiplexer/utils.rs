use crate::clients::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::{ClosedState, RedisClientInner};
use crate::multiplexer::types::ClusterChange;
use crate::multiplexer::{responses, Multiplexer};
use crate::multiplexer::{Backpressure, CloseTx, Connections, Counters, SentCommand, SentCommands};
use crate::protocol::connection::{self, RedisSink, RedisStream};
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::protocol::utils::server_to_parts;
use crate::trace;
use crate::types::*;
use crate::utils as client_utils;
use futures::future::Either;
use futures::pin_mut;
use futures::select;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use log::Level;
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, mem, str};
use tokio;
use tokio::sync::broadcast::{channel as broadcast_channel, Receiver as BroadcastReceiver};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::RwLock as AsyncRwLock;

const DEFAULT_BROADCAST_CAPACITY: usize = 16;

pub fn close_error_tx(error_tx: &RwLock<VecDeque<UnboundedSender<RedisError>>>) {
  for _ in error_tx.write().drain(..) {
    trace!("Closing error tx.");
  }
}

pub fn close_reconnect_tx(reconnect_tx: &RwLock<VecDeque<UnboundedSender<RedisClient>>>) {
  for _ in reconnect_tx.write().drain(..) {
    trace!("Closing reconnect tx.");
  }
}

pub fn close_messages_tx(messages_tx: &RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>) {
  for _ in messages_tx.write().drain(..) {
    trace!("Closing messages tx.");
  }
}

pub fn close_keyspace_events_tx(keyspace_tx: &RwLock<VecDeque<UnboundedSender<KeyspaceEvent>>>) {
  for _ in keyspace_tx.write().drain(..) {
    trace!("Closing keyspace tx");
  }
}

pub fn close_connect_tx(connect_tx: &RwLock<VecDeque<OneshotSender<Result<(), RedisError>>>>) {
  for tx in connect_tx.write().drain(..) {
    trace!("Closing connect tx");
    let _ = tx.send(Err(RedisError::new_canceled()));
  }
}

pub fn emit_connect(inner: &Arc<RedisClientInner>) {
  _debug!(inner, "Emitting connect message.");
  for tx in inner.connect_tx.write().drain(..) {
    let _ = tx.send(Ok(()));
  }
}

pub fn emit_connect_error(inner: &Arc<RedisClientInner>, error: &RedisError) {
  _debug!(inner, "Emitting connect error: {:?}", error);
  for tx in inner.connect_tx.write().drain(..) {
    let _ = tx.send(Err(error.clone()));
  }
}

pub fn emit_error(inner: &Arc<RedisClientInner>, error: &RedisError) {
  let mut new_tx = VecDeque::new();
  let mut tx_guard = inner.error_tx.write();

  for tx in tx_guard.drain(..) {
    if let Err(e) = tx.send(error.clone()) {
      _debug!(inner, "Error emitting error message: {:?}", e);
    } else {
      new_tx.push_back(tx);
    }
  }

  *tx_guard = new_tx;
}

pub fn emit_reconnect(inner: &Arc<RedisClientInner>) {
  let mut new_tx = VecDeque::new();
  let mut tx_guard = inner.reconnect_tx.write();

  for tx in tx_guard.drain(..) {
    if let Err(_e) = tx.send(inner.into()) {
      _debug!(inner, "Error emitting reconnect message.");
    } else {
      new_tx.push_back(tx);
    }
  }

  *tx_guard = new_tx;
}

fn take_commands(
  commands: &Arc<Mutex<BTreeMap<Arc<String>, SentCommands>>>,
  server: &Arc<String>,
) -> Option<SentCommands> {
  commands.lock().remove(server)
}

/// Emit a message to the task monitoring for connection closed events.
///
/// If the caller has provided a reconnect policy it will kick in when this message is received.
pub fn emit_connection_closed(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  server: &Arc<String>,
  error: RedisError,
) {
  _debug!(inner, "Emit connection closed from error: {:?}", error);
  let closed_tx = { inner.connection_closed_tx.read().clone() };
  let commands = match connections {
    Connections::Clustered { ref commands, .. } => take_commands(commands, server),
    Connections::Centralized { ref commands, .. } => {
      let commands: SentCommands = commands.lock().drain(..).collect();
      Some(commands)
    }
  };

  if let Some(tx) = closed_tx {
    let commands = commands.unwrap_or(VecDeque::new());
    _trace!(inner, "Emitting connection closed with {} messages", commands.len());

    if let Err(_e) = tx.send(ClosedState { commands, error }) {
      _warn!(
        inner,
        "Could not send connection closed event. Reconnection logic will not run."
      );
    }
  } else {
    _warn!(inner, "Redis client does not have connection closed sender.");
  }
}

/// Send a command to the reconnect task to check and sync connections and to refresh cluster state.
pub fn refresh_cluster_state(inner: &Arc<RedisClientInner>, mut command: SentCommand, error: RedisError) {
  _debug!(
    inner,
    "Refresh cluster state and retry command: {}",
    command.command.kind.to_str_debug()
  );
  let closed_tx = { inner.connection_closed_tx.read().clone() };

  if let Some(tx) = closed_tx {
    // reset the attempted count since MOVED/ASK errors shouldn't count as failed write attempts
    command.command.attempted = 0;
    let mut commands = VecDeque::with_capacity(1);
    commands.push_back(command);
    _trace!(inner, "Emitting cluster refresh with {} messages", commands.len());

    if let Err(_e) = tx.send(ClosedState { commands, error }) {
      _warn!(
        inner,
        "Could not send refresh cluster event. Reconnection logic will not run."
      );
    }
  } else {
    _warn!(inner, "Redis client does not have connection closed sender.");
  }
}

pub fn insert_locked_map<K: Ord, V>(locked: &RwLock<BTreeMap<K, V>>, key: K, value: V) -> Option<V> {
  locked.write().insert(key, value)
}

pub fn insert_locked_map_mutex<K: Ord, V>(locked: &Mutex<BTreeMap<K, V>>, key: K, value: V) -> Option<V> {
  locked.lock().insert(key, value)
}

pub async fn insert_locked_map_async<K: Ord, V>(locked: &AsyncRwLock<BTreeMap<K, V>>, key: K, value: V) -> Option<V> {
  locked.write().await.insert(key, value)
}

/// Check whether the command has reached the max number of write attempts, and if so emit an error to the caller.
pub fn max_attempts_reached(inner: &Arc<RedisClientInner>, command: &mut RedisCommand) -> bool {
  if command.max_attempts_exceeded(inner) {
    _warn!(
      inner,
      "Exceeded max write attempts for command: {}",
      command.kind.to_str_debug()
    );

    if let Some(tx) = command.tx.take() {
      if let Err(e) = tx.send(Err(RedisError::new(
        RedisErrorKind::Canceled,
        "Max write attempts reached.",
      ))) {
        _warn!(inner, "Error responding to caller with max attempts error: {:?}", e);
      }
    }
    if let Some(tx) = client_utils::take_locked(&command.resp_tx) {
      if let Err(e) = tx.send(()) {
        _warn!(inner, "Error unblocking multiplexer command loop: {:?}", e);
      }
    }

    true
  } else {
    false
  }
}

pub fn should_apply_backpressure(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  server: Option<&Arc<String>>,
) -> Result<Option<u64>, RedisError> {
  let in_flight = match connections {
    Connections::Centralized { ref counters, .. } => client_utils::read_atomic(&counters.in_flight),
    Connections::Clustered { ref counters, .. } => server
      .and_then(|server| {
        counters
          .read()
          .get(server)
          .map(|counters| client_utils::read_atomic(&counters.in_flight))
      })
      .unwrap_or(0),
  };
  let min_backpressure_time_ms = inner.perf_config.min_sleep_duration();
  let backpressure_command_count = inner.perf_config.max_in_flight_commands();
  let disable_backpressure_scaling = inner.perf_config.disable_backpressure_scaling();

  let amt = if in_flight > backpressure_command_count {
    if inner.perf_config.disable_auto_backpressure() {
      return Err(RedisError::new(
        RedisErrorKind::Backpressure,
        "Max number of in-flight commands reached.",
      ));
    }

    if disable_backpressure_scaling {
      Some(min_backpressure_time_ms as u64)
    } else {
      Some(cmp::max(min_backpressure_time_ms as u64, in_flight as u64))
    }
  } else {
    None
  };

  Ok(amt)
}

pub fn centralized_server_name(inner: &Arc<RedisClientInner>) -> String {
  match inner.config.read().server {
    ServerConfig::Centralized { ref host, ref port, .. } => format!("{}:{}", host, port),
    // for sentinel configs this will be replaced later after reading the primary node from the sentinel(s)
    _ => "unknown".to_owned(),
  }
}

/// Emit a message that closes the stream portion of the TcpStream if it's not already closed.
///
/// The handler for this message will emit another message that triggers reconnect, if necessary.
pub fn emit_closed_message(
  inner: &Arc<RedisClientInner>,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
  error: &RedisError,
) {
  if let Some(ref tx) = *close_tx.read() {
    _debug!(inner, "Emitting close all sockets message: {:?}", error);
    if let Err(e) = tx.send(error.clone()) {
      _warn!(inner, "Error sending close message to socket streams: {:?}", e);
    }
  }
}

pub fn unblock_multiplexer(inner: &Arc<RedisClientInner>, command: &RedisCommand) {
  if let Some(tx) = command.resp_tx.write().take() {
    _debug!(inner, "Unblocking multiplexer command: {}", command.kind.to_str_debug());
    let _ = tx.send(());
  }
}

/// Write a command to all nodes in the cluster.
///
/// The callback will come from the first node to respond to the request.
pub async fn write_all_nodes(
  inner: &Arc<RedisClientInner>,
  writers: &Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
  commands: &Arc<Mutex<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  command: RedisCommand,
) -> Result<Backpressure, RedisError> {
  if let Some(inner) = command.kind.all_nodes_response() {
    inner.set_num_nodes(writers.read().await.len());
  } else {
    return Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected command with all node response.",
    ));
  }

  for (server, writer) in writers.write().await.iter_mut() {
    let counter = match counters.read().get(server) {
      Some(counter) => counter.clone(),
      None => {
        return Err(RedisError::new(
          RedisErrorKind::Config,
          format!("Failed to lookup counter for {}", server),
        ))
      }
    };

    let kind = match command.kind.clone_all_nodes() {
      Some(k) => k,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::Config,
          "Invalid redis command kind to send to all nodes.",
        ));
      }
    };
    let _command = command.duplicate(kind);

    let was_flushed = send_clustered_command(inner, &server, &counter, writer, commands, _command).await?;
    if !was_flushed {
      if let Err(e) = writer.flush().await {
        _warn!(inner, "Failed to flush socket: {:?}", e);
      }
    }
  }

  Ok(Backpressure::Skipped)
}

pub fn prepare_command(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  command: RedisCommand,
) -> Result<(SentCommand, ProtocolFrame, bool), RedisError> {
  let frame = command.to_frame(inner.is_resp3())?;
  let mut sent_command: SentCommand = command.into();
  sent_command.command.incr_attempted();
  sent_command.network_start = Some(Instant::now());
  if inner.should_trace() {
    trace::set_network_span(&mut sent_command.command, true);
  }
  // flush the socket under the following conditions:
  // * we don't know of any queued commands following this command
  // * we've fed up to the global max feed count commands already
  // * the command closes the connection
  // * the command ends a transaction
  // * the command does some form of authentication
  // * the command goes to multiple sockets at once
  // * the command blocks the multiplexer command loop
  let should_flush = counters.should_send(inner)
    || sent_command.command.is_quit()
    || sent_command.command.kind.ends_transaction()
    || sent_command.command.kind.is_hello()
    || sent_command.command.kind.is_auth()
    || sent_command.command.kind.is_all_cluster_nodes()
    || client_utils::is_locked_some(&sent_command.command.resp_tx);

  Ok((sent_command, frame, should_flush))
}

pub async fn flush_sinks(
  inner: &Arc<RedisClientInner>,
  connections: &mut BTreeMap<Arc<String>, RedisSink>,
  skip: Option<&Arc<String>>,
) {
  for (server, sink) in connections.iter_mut() {
    if let Some(skip) = skip {
      if skip == server {
        continue;
      }
    }

    _debug!(inner, "Flushing socket to {}", server);
    if let Err(e) = sink.flush().await {
      _warn!(inner, "Error flushing sink: {:?}", e);
    }
  }
}

pub async fn send_centralized_command(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Counters,
  writer: &mut RedisSink,
  commands: &Arc<Mutex<SentCommands>>,
  command: RedisCommand,
) -> Result<(), RedisError> {
  let (command, frame, should_flush) = prepare_command(inner, counters, command)?;
  _debug!(
    inner,
    "Writing command {} to {}",
    command.command.kind.to_str_debug(),
    server
  );

  {
    commands.lock().push_back(command.into());
  }
  // if writing the command fails it will be retried from this point forward since it has been added to the commands queue
  connection::write_command(inner, writer, counters, frame, should_flush).await
}

pub async fn send_clustered_command(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Counters,
  writer: &mut RedisSink,
  commands: &Arc<Mutex<BTreeMap<Arc<String>, SentCommands>>>,
  command: RedisCommand,
) -> Result<bool, RedisError> {
  let (command, frame, should_flush) = prepare_command(inner, counters, command)?;
  _debug!(
    inner,
    "Writing command {} to {}",
    command.command.kind.to_str_debug(),
    server
  );

  {
    if let Some(commands) = commands.lock().get_mut(server) {
      commands.push_back(command);
    } else {
      _error!(inner, "Failed to lookup command queue for {}", server);
      return Err(RedisError::new_context(
        RedisErrorKind::IO,
        format!("Missing command queue for {}", server),
        command.command,
      ));
    }
  }
  // if writing the command fails it will be retried from this point forward since it has been added to the commands queue
  let _ = connection::write_command(inner, writer, counters, frame, should_flush).await?;
  // return whether the socket was flushed so the caller can flush the other sockets if needed
  Ok(should_flush)
}

fn respond_early_to_caller_error(inner: &Arc<RedisClientInner>, mut command: RedisCommand, error: RedisError) {
  _debug!(inner, "Responding early to caller with error {:?}", error);

  if let Some(tx) = command.tx.take() {
    if let Err(e) = tx.send(Err(error)) {
      _warn!(inner, "Error sending response to caller: {:?}", e);
    }
  }

  // check for a multiplexer response sender too
  if let Some(tx) = command.resp_tx.write().take() {
    let _ = tx.send(());
  }
}

pub async fn write_centralized_command(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  command: RedisCommand,
  no_backpressure: bool,
) -> Result<Backpressure, RedisError> {
  if !no_backpressure {
    let backpressure = match should_apply_backpressure(inner, connections, None) {
      Ok(backpressure) => backpressure,
      Err(e) => {
        respond_early_to_caller_error(inner, command, e);
        return Ok(Backpressure::Skipped);
      }
    };

    if let Some(backpressure) = backpressure {
      _warn!(inner, "Applying backpressure for {} ms", backpressure);
      return Ok(Backpressure::Wait((Duration::from_millis(backpressure), command)));
    }
  }

  if let Connections::Centralized {
    ref counters,
    ref commands,
    ref writer,
    ref server,
    ..
  } = connections
  {
    if let Some(writer) = writer.write().await.deref_mut() {
      let server_guard = server.read().await;

      send_centralized_command(inner, &*server_guard, counters, writer, &commands, command)
        .await
        .map(|_| Backpressure::Ok((*server_guard).clone()))
    } else {
      Err(RedisError::new_context(
        RedisErrorKind::Unknown,
        "Redis connection is not initialized.",
        command,
      ))
    }
  } else {
    _error!(
      inner,
      "Expected centralized connection when writing command. This is a bug."
    );
    Err(RedisError::new(RedisErrorKind::Config, "Invalid connection type."))
  }
}

pub async fn write_clustered_command(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  command: RedisCommand,
  hash_slot: Option<u16>,
  no_backpressure: bool,
) -> Result<Backpressure, RedisError> {
  if let Connections::Clustered {
    ref writers,
    ref commands,
    ref counters,
    ref cache,
    ..
  } = connections
  {
    let hash_slot = match hash_slot {
      Some(slot) => Some(slot),
      None => command.extract_key().map(|key| redis_keyslot(key)),
    };
    let server = match hash_slot {
      Some(hash_slot) => match cache.read().get_server(hash_slot) {
        Some(slot) => slot.server.clone(),
        None => {
          return Err(RedisError::new_context(
            RedisErrorKind::Unknown,
            format!("Unable to find server for keyslot {}", hash_slot),
            command,
          ));
        }
      },
      None => match cache.read().random_slot() {
        Some(slot) => slot.server.clone(),
        None => {
          return Err(RedisError::new_context(
            RedisErrorKind::Unknown,
            "Cluster state is not initialized.",
            command,
          ));
        }
      },
    };

    if !no_backpressure {
      let backpressure = match should_apply_backpressure(inner, connections, Some(&server)) {
        Ok(backpressure) => backpressure,
        Err(e) => {
          respond_early_to_caller_error(inner, command, e);
          return Ok(Backpressure::Skipped);
        }
      };

      if let Some(backpressure) = backpressure {
        _warn!(inner, "Applying backpressure for {} ms", backpressure);
        return Ok(Backpressure::Wait((Duration::from_millis(backpressure), command)));
      }
    }
    if log_enabled!(Level::Trace) {
      if let Some(key) = command.extract_key() {
        _trace!(
          inner,
          "Using server {} with hash slot {:?} from key {}",
          server,
          hash_slot,
          String::from_utf8_lossy(key)
        );
      }
    }

    let counters_opt = counters.read().get(&server).cloned();
    if let Some(counters) = counters_opt {
      let mut writers_guard = writers.write().await;

      let was_flushed = if let Some(writer) = writers_guard.get_mut(&server) {
        send_clustered_command(inner, &server, &counters, writer, commands, command).await?
      } else {
        return Err(RedisError::new_context(
          RedisErrorKind::Cluster,
          format!("Unable to find server connection for {}", server),
          command,
        ));
      };

      if was_flushed {
        // flush the other connections to other nodes to be safe, skipping the socket that was already flushed
        // TODO improve this by checking in-flight counters on each server. if zero then dont flush.
        flush_sinks(inner, &mut *writers_guard, Some(&server)).await;
      }
      Ok(Backpressure::Ok(server.clone()))
    } else {
      return Err(RedisError::new_context(
        RedisErrorKind::Unknown,
        format!("Unable to find server counters for {}", server),
        command,
      ));
    }
  } else {
    _error!(
      inner,
      "Expected clustered connection when writing command. This is a bug."
    );
    Err(RedisError::new(RedisErrorKind::Config, "Invalid connection type."))
  }
}

pub fn take_sent_commands(connections: &Connections) -> VecDeque<SentCommand> {
  match connections {
    Connections::Centralized { ref commands, .. } => commands.lock().drain(..).collect(),
    Connections::Clustered {
      ref cache,
      ref commands,
      ..
    } => zip_cluster_commands(cache, commands),
  }
}

/// Zip up all the command queues on each connection, returning an array with all commands that is sorted by the inner ordering within each command queue and across all queues.
///
/// For example, given a command queue map such as:
///
/// ```ignore
/// a -> [1,2,3,4]
/// b -> [5,6]
/// c -> [7,8,9]
/// ```
///
/// This will return `[1,7,5,2,8,6,3,9,4]`
pub fn zip_cluster_commands(
  cache: &Arc<RwLock<ClusterKeyCache>>,
  commands: &Arc<Mutex<BTreeMap<Arc<String>, SentCommands>>>,
) -> VecDeque<SentCommand> {
  let num_connections = {
    let mut out = BTreeSet::new();
    for slot_range in cache.read().slots() {
      out.insert(slot_range.server.clone());
    }
    out.len()
  };

  let (capacity, mut command_queues) = {
    let mut out = Vec::with_capacity(num_connections);
    let mut capacity = 0;

    for (_, commands) in commands.lock().iter_mut() {
      capacity += commands.len();
      out.push(mem::replace(commands, VecDeque::new()));
    }
    // sort the arrays by length in desc order so that we can iterate for the length of the first array
    // and when we come to an array that doesnt have an element at that idx we can just pop it off the
    // outer vector of command queues.
    out.sort_by(|a, b| b.len().cmp(&a.len()));

    (capacity, out)
  };

  if command_queues.is_empty() {
    return VecDeque::new();
  }

  let mut zipped_commands = VecDeque::with_capacity(capacity);
  // unwrap checked above. first queue is the longest one
  for _ in 0..command_queues.first().unwrap().len() {
    let mut to_pop = 0;

    for queue in command_queues.iter_mut() {
      if let Some(cmd) = queue.pop_front() {
        zipped_commands.push_back(cmd);
      } else {
        to_pop += 1;
      }
    }

    for _ in 0..to_pop {
      let _ = command_queues.pop();
    }
  }

  zipped_commands
}

async fn remove_cluster_writer(connections: &Connections, server: &Arc<String>) {
  if let Connections::Clustered { ref writers, .. } = connections {
    let _ = writers.write().await.remove(server);
  }
}

pub fn spawn_clustered_listener(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  commands: &Arc<Mutex<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  mut close_rx: BroadcastReceiver<RedisError>,
  server: &Arc<String>,
  stream: RedisStream,
) {
  let inner = inner.clone();
  let connections = connections.clone();
  let commands = commands.clone();
  let counters = counters.clone();
  let server = server.clone();

  let _ = tokio::spawn(async move {
    let memo = (inner.clone(), server.clone(), counters, commands);

    let stream_ft = match stream {
      RedisStream::Tls(stream) => Either::Left(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
            let frame = frame.into_resp3();
            responses::process_clustered_frame(&inner, &server, &counters, &commands, frame).await?;
            Ok((inner, server, counters, commands))
          })
          .and_then(|_| async { Ok(()) }),
      ),
      RedisStream::Tcp(stream) => Either::Right(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
            let frame = frame.into_resp3();
            responses::process_clustered_frame(&inner, &server, &counters, &commands, frame).await?;
            Ok((inner, server, counters, commands))
          })
          .and_then(|_| async { Ok(()) }),
      ),
    }
    .fuse();
    pin_mut!(stream_ft);

    let close_inner = inner.clone();
    let close_ft = close_rx
      .recv()
      .err_into::<RedisError>()
      .and_then(|e| async move {
        _debug!(close_inner, "Close rx recv error: {:?}", e);
        Err(e)
      })
      .fuse();
    pin_mut!(close_ft);

    let result = select! {
      close_res = close_ft => close_res,
      stream_res = stream_ft => stream_res
    };
    let error = result
      .and_then(|_| Err::<(), _>(RedisError::new_canceled()))
      .unwrap_err();

    if client_utils::read_client_state(&inner.state) == ClientState::Disconnecting {
      // client was closed intentionally via Quit
      client_utils::set_client_state(&inner.state, ClientState::Disconnected);
      return Ok(());
    }

    _debug!(inner, "Redis clustered frame stream closed with error {:?}", error);
    remove_cluster_writer(&connections, &server).await;
    client_utils::set_client_state(&inner.state, ClientState::Disconnected);
    emit_connection_closed(&inner, &connections, &server, error);

    Ok::<(), RedisError>(())
  });
}

async fn create_cluster_connection(
  inner: &Arc<RedisClientInner>,
  connection_ids: &Arc<RwLock<BTreeMap<Arc<String>, i64>>>,
  server: &Arc<String>,
  uses_tls: bool,
) -> Result<(RedisSink, RedisStream), RedisError> {
  let (host, port) = protocol_utils::server_to_parts(server)?;
  let addr = inner.resolver.resolve(host.to_owned(), port).await?;

  if uses_tls {
    let (domain, addr) = protocol_utils::parse_cluster_server(inner, &server).await?;

    let socket = connection::create_authenticated_connection_tls(&addr, &domain, inner).await?;
    let socket = match connection::read_client_id(inner, socket).await {
      Ok((id, socket)) => {
        if let Some(id) = id {
          connection_ids.write().insert(server.clone(), id);
        }
        socket
      }
      Err((_, socket)) => socket,
    };

    let (sink, stream) = socket.split();
    Ok((RedisSink::Tls(sink), RedisStream::Tls(stream)))
  } else {
    let socket = connection::create_authenticated_connection(&addr, inner).await?;
    let socket = match connection::read_client_id(inner, socket).await {
      Ok((id, socket)) => {
        if let Some(id) = id {
          connection_ids.write().insert(server.clone(), id);
        }
        socket
      }
      Err((_, socket)) => socket,
    };

    let (sink, stream) = socket.split();
    Ok((RedisSink::Tcp(sink), RedisStream::Tcp(stream)))
  }
}

pub fn get_or_create_close_tx(inner: &Arc<RedisClientInner>, close_tx: &Arc<RwLock<Option<CloseTx>>>) -> CloseTx {
  let mut guard = close_tx.write();

  if let Some(tx) = { guard.clone() } {
    tx
  } else {
    _debug!(inner, "Creating new close tx sender.");
    let (tx, _) = broadcast_channel(DEFAULT_BROADCAST_CAPACITY);
    *guard = Some(tx.clone());
    tx
  }
}

pub async fn connect_clustered(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<VecDeque<SentCommand>, RedisError> {
  let pending_commands = take_sent_commands(connections);

  if let Connections::Clustered {
    ref commands,
    ref counters,
    ref writers,
    ref cache,
    ref connection_ids,
  } = connections
  {
    client_utils::set_client_state(&inner.state, ClientState::Connecting);
    let uses_tls = protocol_utils::uses_tls(inner);
    let cluster_state = connection::read_cluster_nodes(inner).await?;
    let main_nodes = cluster_state.unique_main_nodes();
    client_utils::set_locked(cache, cluster_state);
    connection_ids.write().clear();

    let tx = get_or_create_close_tx(inner, close_tx);
    for server in main_nodes.into_iter() {
      let (sink, stream) = create_cluster_connection(inner, connection_ids, &server, uses_tls).await?;

      insert_locked_map_mutex(commands, server.clone(), VecDeque::new());
      insert_locked_map_async(writers, server.clone(), sink).await;
      insert_locked_map(counters, server.clone(), Counters::new(&inner.cmd_buffer_len));
      spawn_clustered_listener(inner, connections, commands, counters, tx.subscribe(), &server, stream);
    }

    _debug!(inner, "Set clustered connection closed sender.");
    client_utils::set_client_state(&inner.state, ClientState::Connected);
    Ok(pending_commands)
  } else {
    Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected clustered connections.",
    ))
  }
}

pub fn spawn_centralized_listener(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  connections: &Connections,
  mut close_rx: BroadcastReceiver<RedisError>,
  commands: &Arc<Mutex<VecDeque<SentCommand>>>,
  counters: &Counters,
  stream: RedisStream,
) {
  let inner = inner.clone();
  let commands = commands.clone();
  let counters = counters.clone();
  let connections = connections.clone();
  let server = server.clone();

  let _ = tokio::spawn(async move {
    let memo = (inner.clone(), server.clone(), counters, commands);

    let stream_ft = match stream {
      RedisStream::Tls(stream) => Either::Left(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
            let frame = frame.into_resp3();
            responses::process_centralized_frame(&inner, &server, &counters, &commands, frame).await?;
            Ok((inner, server, counters, commands))
          })
          .and_then(|_| async { Ok(()) }),
      ),
      RedisStream::Tcp(stream) => Either::Right(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
            let frame = frame.into_resp3();
            responses::process_centralized_frame(&inner, &server, &counters, &commands, frame).await?;
            Ok((inner, server, counters, commands))
          })
          .and_then(|_| async { Ok(()) }),
      ),
    }
    .fuse();
    pin_mut!(stream_ft);

    let close_inner = inner.clone();
    let close_ft = close_rx
      .recv()
      .err_into::<RedisError>()
      .and_then(|e| async move {
        _debug!(close_inner, "Close rx recv error: {:?}", e);
        Err(e)
      })
      .fuse();
    pin_mut!(close_ft);

    let result = select! {
      close_res = close_ft => close_res,
      stream_res = stream_ft => stream_res
    };
    let error = result
      .and_then(|_| Err::<(), _>(RedisError::new_canceled()))
      .unwrap_err();

    if client_utils::read_client_state(&inner.state) == ClientState::Disconnecting {
      // client was closed intentionally via Quit
      client_utils::set_client_state(&inner.state, ClientState::Disconnected);
      return Ok(());
    }

    _debug!(inner, "Redis frame stream closed with error {:?}", error);
    client_utils::set_client_state(&inner.state, ClientState::Disconnected);
    emit_connection_closed(&inner, &connections, &server, error);

    Ok::<(), RedisError>(())
  });
}

pub async fn connect_centralized(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<VecDeque<SentCommand>, RedisError> {
  let pending_commands = take_sent_commands(connections);

  if let Connections::Centralized {
    ref commands,
    ref writer,
    ref counters,
    ref server,
    ref connection_id,
  } = connections
  {
    let addr = protocol_utils::read_centralized_addr(&inner).await?;
    let uses_tls = protocol_utils::uses_tls(inner);
    client_utils::set_client_state(&inner.state, ClientState::Connecting);

    let (sink, stream) = if uses_tls {
      let domain = protocol_utils::read_centralized_domain(&inner.config)?;
      _trace!(inner, "Connecting to {} with domain {}", addr, domain);
      let socket = connection::create_authenticated_connection_tls(&addr, &domain, inner).await?;
      let socket = match connection::read_client_id(inner, socket).await {
        Ok((id, socket)) => {
          if let Some(id) = id {
            connection_id.write().replace(id);
          }
          socket
        }
        Err((_, socket)) => socket,
      };

      let (sink, stream) = socket.split();
      (RedisSink::Tls(sink), RedisStream::Tls(stream))
    } else {
      _trace!(inner, "Connecting to {}", addr);
      let socket = connection::create_authenticated_connection(&addr, inner).await?;
      let socket = match connection::read_client_id(inner, socket).await {
        Ok((id, socket)) => {
          if let Some(id) = id {
            connection_id.write().replace(id);
          }
          socket
        }
        Err((_, socket)) => socket,
      };

      let (sink, stream) = socket.split();
      (RedisSink::Tcp(sink), RedisStream::Tcp(stream))
    };
    counters.reset_in_flight();
    counters.reset_feed_count();

    let tx = get_or_create_close_tx(inner, close_tx);
    _debug!(inner, "Set centralized connection closed sender.");
    let _ = client_utils::set_locked_async(&writer, Some(sink)).await;
    let server = server.read().await.clone();

    spawn_centralized_listener(inner, &server, connections, tx.subscribe(), commands, counters, stream);
    client_utils::set_client_state(&inner.state, ClientState::Connected);

    Ok(pending_commands)
  } else {
    Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected centralized connection.",
    ))
  }
}

/// Check the keys provided in an `mget` command when run against a cluster to ensure the keys all live on one node in the cluster.
pub fn check_mget_cluster_keys(multiplexer: &Multiplexer, keys: &Vec<RedisValue>) -> Result<(), RedisError> {
  if let Connections::Clustered { ref cache, .. } = multiplexer.connections {
    let mut nodes = BTreeSet::new();

    for key in keys.iter() {
      let key_bytes = match key.as_bytes() {
        Some(s) => s,
        None => return Err(RedisError::new(RedisErrorKind::InvalidArgument, "Expected key bytes.")),
      };
      let hash_slot = redis_protocol::redis_keyslot(key_bytes);
      let server = match cache.read().get_server(hash_slot) {
        Some(s) => s.id.clone(),
        None => {
          return Err(RedisError::new(
            RedisErrorKind::InvalidArgument,
            "Failed to find cluster node",
          ));
        }
      };

      nodes.insert(server);
    }

    if nodes.len() == 1 {
      Ok(())
    } else {
      Err(RedisError::new(
        RedisErrorKind::InvalidArgument,
        "MGET keys must all belong to the same cluster node.",
      ))
    }
  } else {
    Ok(())
  }
}

pub fn check_mset_cluster_keys(multiplexer: &Multiplexer, args: &Vec<RedisValue>) -> Result<(), RedisError> {
  if args.len() % 2 != 0 {
    return Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "MSET must contain an even number of arguments.",
    ));
  }

  if let Connections::Clustered { ref cache, .. } = multiplexer.connections {
    let mut nodes = BTreeSet::new();

    for chunk in args.chunks(2) {
      let key = match chunk[0].as_bytes() {
        Some(s) => s,
        None => return Err(RedisError::new(RedisErrorKind::InvalidArgument, "Expected key bytes.")),
      };
      let hash_slot = redis_protocol::redis_keyslot(key);
      let server = match cache.read().get_server(hash_slot) {
        Some(s) => s.id.clone(),
        None => {
          return Err(RedisError::new(
            RedisErrorKind::InvalidArgument,
            "Failed to find cluster node.",
          ));
        }
      };

      nodes.insert(server);
    }

    if nodes.len() == 1 {
      Ok(())
    } else {
      Err(RedisError::new(
        RedisErrorKind::InvalidArgument,
        "MSET keys must all belong to the same cluster node.",
      ))
    }
  } else {
    Ok(())
  }
}

async fn create_cluster_change(
  cluster_state: &ClusterKeyCache,
  writers: &Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
) -> ClusterChange {
  let mut old_servers = BTreeSet::new();
  let mut new_servers = BTreeSet::new();
  for server in cluster_state.unique_main_nodes().into_iter() {
    new_servers.insert(server);
  }
  {
    for server in writers.write().await.keys() {
      old_servers.insert(server.clone());
    }
  }

  ClusterChange {
    add: new_servers.difference(&old_servers).map(|s| s.clone()).collect(),
    remove: old_servers.difference(&new_servers).map(|s| s.clone()).collect(),
  }
}

pub fn finish_synchronizing(inner: &Arc<RedisClientInner>, tx: &Arc<RwLock<VecDeque<OneshotSender<()>>>>) {
  for tx in tx.write().drain(..) {
    if let Err(_) = tx.send(()) {
      _warn!(inner, "Error sending repairing message to caller.");
    }
  }
}

async fn remove_server(
  inner: &Arc<RedisClientInner>,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  writers: &Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
  commands: &Arc<Mutex<BTreeMap<Arc<String>, SentCommands>>>,
  connection_ids: &Arc<RwLock<BTreeMap<Arc<String>, i64>>>,
  server: &Arc<String>,
) -> Result<(), RedisError> {
  _debug!(inner, "Removing clustered connection to server {}", server);
  let commands = {
    let _ = { writers.write().await.remove(server) };
    let _ = { counters.write().remove(server) };
    let _ = { connection_ids.write().remove(server) };
    commands.lock().remove(server)
  };

  if let Some(commands) = commands {
    for command in commands.into_iter() {
      _debug!(
        inner,
        "Retrying {} command when removing server {}",
        command.command.kind.to_str_debug(),
        server
      );

      unblock_multiplexer(inner, &command.command);
      client_utils::send_command(inner, command.command)?;
    }
  }
  Ok(())
}

async fn add_server(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  writers: &Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
  commands: &Arc<Mutex<BTreeMap<Arc<String>, SentCommands>>>,
  connection_ids: &Arc<RwLock<BTreeMap<Arc<String>, i64>>>,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
  server: &Arc<String>,
) -> Result<(), RedisError> {
  _debug!(inner, "Adding new clustered connection to {}", server);
  let uses_tls = protocol_utils::uses_tls(inner);
  let (sink, stream) = create_cluster_connection(inner, connection_ids, server, uses_tls).await?;
  let tx = get_or_create_close_tx(inner, close_tx);

  insert_locked_map_mutex(commands, server.clone(), VecDeque::new());
  insert_locked_map_async(writers, server.clone(), sink).await;
  insert_locked_map(counters, server.clone(), Counters::new(&inner.cmd_buffer_len));
  spawn_clustered_listener(inner, connections, commands, counters, tx.subscribe(), &server, stream);
  Ok(())
}

/// Read the offset of the existing backchannel server in `servers`, if found.
async fn existing_backchannel_connection(inner: &Arc<RedisClientInner>, servers: &Vec<Arc<String>>) -> Option<usize> {
  if let Some((_, ref backchannel_server)) = inner.backchannel.read().await.transport {
    let mut swap = None;

    for (idx, server) in servers.iter().enumerate() {
      if idx != 0 && server == backchannel_server {
        swap = Some(idx);
        break;
      }
    }

    swap
  } else {
    None
  }
}

async fn cluster_nodes_backchannel(inner: &Arc<RedisClientInner>) -> Result<ClusterKeyCache, RedisError> {
  let mut servers = if let Some(ref state) = *inner.cluster_state.read() {
    state.unique_main_nodes()
  } else {
    _debug!(
      inner,
      "Falling back to hosts from config in cluster backchannel due to missing cluster state."
    );
    inner
      .config
      .read()
      .server
      .hosts()
      .iter()
      .map(|(h, p)| Arc::new(format!("{}:{}", h, p)))
      .collect()
  };

  _debug!(inner, "Creating or using backchannel from {:?}", servers);
  if let Some(swap) = existing_backchannel_connection(inner, &servers).await {
    servers.swap(0, swap);
  }

  for server in servers.into_iter() {
    let cmd = RedisCommand::new(RedisCommandKind::ClusterNodes, vec![], None);
    let mut backchannel = inner.backchannel.write().await;

    _debug!(inner, "Reading cluster nodes on backchannel: {}", server);
    let frame = match backchannel.request_response(inner, &server, cmd, true).await {
      Ok(frame) => frame,
      Err(e) => {
        _warn!(inner, "Error creating or using backchannel for cluster nodes: {:?}", e);
        continue;
      }
    };

    if let Resp3Frame::BlobString { data, .. } = frame {
      let response = str::from_utf8(&data)?;
      let state = match ClusterKeyCache::new(Some(response)) {
        Ok(state) => state,
        Err(e) => {
          _warn!(inner, "Error parsing cluster nodes response from backchannel: {:?}", e);
          continue;
        }
      };
      return Ok(state);
    } else {
      _warn!(
        inner,
        "Failed to read cluster nodes on backchannel: {:?}",
        frame.as_str()
      );
    }
  }

  Err(RedisError::new(
    RedisErrorKind::Cluster,
    "Failed to read cluster nodes on all possible backchannel servers.",
  ))
}

/// Emit cluster state change events to listeners, dropping any there were closed.
fn emit_cluster_changes(inner: &Arc<RedisClientInner>, changes: Vec<ClusterStateChange>) {
  let mut to_remove = BTreeSet::new();

  // check for closed senders as we emit messages, and drop them at the end
  {
    for (idx, tx) in inner.cluster_change_tx.read().iter().enumerate() {
      if let Err(_) = tx.send(changes.clone()) {
        to_remove.insert(idx);
      }
    }
  }

  if !to_remove.is_empty() {
    _trace!(inner, "Removing {} closed cluster change listeners", to_remove.len());
    let mut message_tx_guard = inner.cluster_change_tx.write();
    let message_tx_ref = &mut *message_tx_guard;

    let mut new_listeners = VecDeque::with_capacity(message_tx_ref.len() - to_remove.len());

    for (idx, tx) in message_tx_ref.drain(..).enumerate() {
      if !to_remove.contains(&idx) {
        new_listeners.push_back(tx);
      }
    }
    *message_tx_ref = new_listeners;
  }
}

fn broadcast_cluster_changes(inner: &Arc<RedisClientInner>, changes: &ClusterChange) {
  let has_listeners = { inner.cluster_change_tx.read().len() > 0 };

  if has_listeners {
    let (added, removed) = {
      let mut added = Vec::with_capacity(changes.add.len());
      let mut removed = Vec::with_capacity(changes.remove.len());

      for server in changes.add.iter() {
        let parts = match server_to_parts(server) {
          Ok((host, port)) => (host.to_owned(), port),
          Err(_) => continue,
        };

        added.push(parts);
      }
      for server in changes.remove.iter() {
        let parts = match server_to_parts(server) {
          Ok((host, port)) => (host.to_owned(), port),
          Err(_) => continue,
        };

        removed.push(parts);
      }

      (added, removed)
    };
    let mut changes = Vec::with_capacity(added.len() + removed.len() + 1);
    if added.is_empty() && removed.is_empty() {
      changes.push(ClusterStateChange::Rebalance);
    } else {
      for parts in added.into_iter() {
        changes.push(ClusterStateChange::Add(parts))
      }
      for parts in removed.into_iter() {
        changes.push(ClusterStateChange::Remove(parts));
      }
    }

    emit_cluster_changes(inner, changes);
  }
}

pub async fn sync_cluster(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<(), RedisError> {
  _debug!(inner, "Synchronizing cluster state.");

  if let Connections::Clustered {
    ref cache,
    ref writers,
    ref counters,
    ref commands,
    ref connection_ids,
    ..
  } = connections
  {
    let cluster_state = {
      let state = cluster_nodes_backchannel(inner).await?;
      let mut old_cache = cache.write();
      *old_cache = state.clone();
      state
    };
    let changes = create_cluster_change(&cluster_state, &writers).await;
    _debug!(inner, "Changing cluster connections: {:?}", changes);
    broadcast_cluster_changes(inner, &changes);

    for removed_server in changes.remove.into_iter() {
      remove_server(inner, counters, writers, commands, connection_ids, &removed_server).await?;
    }
    for new_server in changes.add.into_iter() {
      add_server(
        inner,
        connections,
        counters,
        writers,
        commands,
        connection_ids,
        close_tx,
        &new_server,
      )
      .await?;
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

#[cfg(test)]
mod tests {
  use super::*;
  use std::time::Instant;
  use tokio;

  #[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
  use crate::trace::CommandTraces;

  fn add_command(commands: &mut VecDeque<SentCommand>, idx: u32) {
    let cmd = RedisCommand {
      kind: RedisCommandKind::Ping,
      args: vec![idx.into()],
      tx: None,
      attempted: 0,
      sent: Instant::now(),
      resp_tx: Arc::new(RwLock::new(None)),
      #[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
      traces: CommandTraces::default(),
    };
    let sent_cmd: SentCommand = cmd.into();

    commands.push_back(sent_cmd);
  }

  #[tokio::test]
  async fn should_zip_command_streams() {
    let server_a = Arc::new("a".to_owned());
    let server_b = Arc::new("b".to_owned());
    let server_c = Arc::new("c".to_owned());
    let cache = vec![
      Arc::new(SlotRange {
        start: 0,
        end: 1,
        server: server_a.clone(),
        id: Arc::new("a1".into()),
      }),
      Arc::new(SlotRange {
        start: 1,
        end: 2,
        server: server_b.clone(),
        id: Arc::new("b1".into()),
      }),
      Arc::new(SlotRange {
        start: 2,
        end: 3,
        server: server_c.clone(),
        id: Arc::new("c1".into()),
      }),
    ];
    let cache = Arc::new(RwLock::new(cache.into()));

    let mut server_a_commands = VecDeque::new();
    for idx in 1..5 {
      add_command(&mut server_a_commands, idx);
    }
    let mut server_b_commands = VecDeque::new();
    for idx in 5..7 {
      add_command(&mut server_b_commands, idx);
    }
    let mut server_c_commands = VecDeque::new();
    for idx in 7..10 {
      add_command(&mut server_c_commands, idx);
    }

    let mut commands = BTreeMap::new();
    commands.insert(server_a, server_a_commands);
    commands.insert(server_b, server_b_commands);
    commands.insert(server_c, server_c_commands);
    let commands = Arc::new(Mutex::new(commands));

    let zipped: Vec<u64> = zip_cluster_commands(&cache, &commands)
      .into_iter()
      .map(|mut cmd| cmd.command.args.pop().unwrap().as_u64().unwrap())
      .collect();
    let expected = vec![1, 7, 5, 2, 8, 6, 3, 9, 4];

    assert_eq!(zipped, expected);

    for (_, commands) in commands.lock().iter() {
      assert!(commands.is_empty());
    }
  }
}
