use crate::client::{CommandSender, RedisClient};
use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::inner::{ClosedState, RedisClientInner};
use crate::multiplexer::{responses, Multiplexer};
use crate::multiplexer::{Backpressure, CloseTx, Connections, Counters, SentCommand, SentCommands};
use crate::protocol::connection::{self, RedisSink, RedisStream};
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils as client_utils;
use futures::future::Either;
use futures::pin_mut;
use futures::select;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use log::Level;
use parking_lot::RwLock;
use std::cmp;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
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

pub fn close_command_tx(command_tx: &RwLock<Option<CommandSender>>) {
  let _ = command_tx.write().take();
}

pub fn emit_connect(connect_tx: &RwLock<VecDeque<OneshotSender<Result<(), RedisError>>>>) {
  for tx in connect_tx.write().drain(..) {
    let _ = tx.send(Ok(()));
  }
}

pub fn emit_connect_error(connect_tx: &RwLock<VecDeque<OneshotSender<Result<(), RedisError>>>>, error: &RedisError) {
  for tx in connect_tx.write().drain(..) {
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

/// Emit a message to the task monitoring for connection closed events.
///
/// If the caller has provided a reconnect policy it will kick in when this message is received.
pub async fn emit_connection_closed(inner: &Arc<RedisClientInner>, connections: &Connections, error: RedisError) {
  _debug!(inner, "Emit connection closed from error: {:?}", error);
  if client_utils::read_client_state(&inner.state) == ClientState::Disconnected {
    let closed_tx = { inner.connection_closed_tx.write().take() };

    if let Some(tx) = closed_tx {
      let commands = reset_connections(&connections).await;
      _trace!(inner, "Emitting connection closed with {} messages", commands.len());
      let state = ClosedState {
        commands,
        error,
        // in a clustered environment multiple connections may simultaneously try to emit a message on this channel.
        // this is a (probably roundabout) way of ensuring that only one message gets through by taking the sender
        // half off the connection state, sending it in the channel, and setting it back on the connection state when
        // the connection has been established again. any competing writes to this channel will have finished by then
        // and new reader tasks for handling frames will have been spawned in that process. after finishing the reconnection
        // process the reconnection task will put this sender back on the connection state.
        tx: tx.clone(),
      };

      if let Err(_e) = tx.send(state) {
        _warn!(
          inner,
          "Could not send connection closed event. Reconnection logic will not run."
        );
      }
    } else {
      _warn!(inner, "Redis client does not have connection closed sender.");
    }
  } else {
    _debug!(inner, "Skip sending connection closed message.")
  }
}

pub fn insert_locked_map<K: Ord, V>(locked: &RwLock<BTreeMap<K, V>>, key: K, value: V) -> Option<V> {
  locked.write().insert(key, value)
}

pub async fn insert_locked_map_async<K: Ord, V>(locked: &AsyncRwLock<BTreeMap<K, V>>, key: K, value: V) -> Option<V> {
  locked.write().await.insert(key, value)
}

/// Check whether the command has reached the max number of write attempts, and if so emit an error to the caller.
pub fn max_attempts_reached(inner: &Arc<RedisClientInner>, command: &mut RedisCommand) -> bool {
  if command.max_attempts_exceeded() {
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

/// Reset the state of the multiplexer connections, preparing it to be overwritten with new connection(s).
///
/// This function also takes all of the in-flight messages that did not receive a response and prepares
/// them to be sent again once the connection is re-established.
pub async fn reset_connections(connections: &Connections) -> VecDeque<SentCommand> {
  let sent_commands = take_sent_commands(connections).await;

  match *connections {
    Connections::Centralized {
      ref counters,
      commands: _,
      ref writer,
      ..
    } => {
      counters.reset_feed_count();
      counters.reset_in_flight();
      client_utils::set_locked_async(writer, None).await;
    }
    Connections::Clustered {
      ref counters,
      commands: _,
      ref writers,
      ref cache,
      ..
    } => {
      cache.write().clear();
      client_utils::set_locked(counters, BTreeMap::new());
      client_utils::set_locked_async(writers, BTreeMap::new()).await;
    }
  };

  sent_commands
}

pub fn should_apply_backpressure(connections: &Connections, server: Option<&Arc<String>>) -> Option<u64> {
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
  let min_backpressure_time_ms = globals().min_backpressure_time_ms();
  let backpressure_command_count = globals().backpressure_count();

  if in_flight > backpressure_command_count {
    Some(cmp::max(in_flight - backpressure_command_count, min_backpressure_time_ms) as u64)
  } else {
    None
  }
}

pub fn centralized_server_name(inner: &Arc<RedisClientInner>) -> Arc<String> {
  match inner.config.read().server {
    ServerConfig::Centralized { ref host, ref port, .. } => Arc::new(format!("{}:{}", host, port)),
    ServerConfig::Clustered { .. } => {
      _error!(
        inner,
        "Falling back to default server name due to unexpected clustered config. This is a bug."
      );
      Arc::new("unknown".to_owned())
    }
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
  if let Some(tx) = close_tx.write().take() {
    _debug!(inner, "Emitting close socket message: {:?}", error);
    if let Err(e) = tx.send(error.clone()) {
      _warn!(inner, "Error sending close message to socket streams: {:?}", e);
    }
  }
}

/// Write a command to all nodes in the cluster.
///
/// The callback will come from the first node to respond to the request.
pub async fn write_all_nodes(
  inner: &Arc<RedisClientInner>,
  writers: &Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
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

    if let Some(commands) = commands.write().await.get_mut(server) {
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

      write_command(inner, server, &counter, writer, commands, _command).await?;
    } else {
      return Err(RedisError::new(
        RedisErrorKind::Config,
        format!("Failed to lookup command queue for {}", server),
      ));
    }
  }

  Ok(Backpressure::Skipped)
}

pub async fn write_command(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Counters,
  writer: &mut RedisSink,
  commands: &mut SentCommands,
  command: RedisCommand,
) -> Result<(), RedisError> {
  _debug!(inner, "Writing command {} to {}", command.kind.to_str_debug(), server);

  commands.push_back(command.into());
  let command = commands.back_mut().expect("Failed to read last command sent.");

  connection::write_command(inner, writer, counters, command).await
}

pub async fn write_centralized_command(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  command: RedisCommand,
  no_backpressure: bool,
) -> Result<Backpressure, RedisError> {
  if !no_backpressure {
    if let Some(backpressure) = should_apply_backpressure(connections, None) {
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
      let mut commands_guard = commands.write().await;

      write_command(inner, server, counters, writer, &mut *commands_guard, command)
        .await
        .map(|_| Backpressure::Ok(server.clone()))
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
      None => command.extract_key().map(|key| redis_keyslot(&key)),
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
      if let Some(backpressure) = should_apply_backpressure(connections, Some(&server)) {
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
          key
        );
      }
    }

    let counters_opt = counters.read().get(&server).cloned();
    if let Some(counters) = counters_opt {
      let mut writers_guard = writers.write().await;
      let mut commands_guard = commands.write().await;

      if let Some(writer) = writers_guard.get_mut(&server) {
        if let Some(commands) = commands_guard.get_mut(&server) {
          write_command(inner, &server, &counters, writer, commands, command)
            .await
            .map(|_| Backpressure::Ok(server.clone()))
        } else {
          return Err(RedisError::new_context(
            RedisErrorKind::Unknown,
            format!("Unable to find server command queue for {}", server),
            command,
          ));
        }
      } else {
        return Err(RedisError::new_context(
          RedisErrorKind::Unknown,
          format!("Unable to find server connection for {}", server),
          command,
        ));
      }
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

pub async fn take_sent_commands(connections: &Connections) -> VecDeque<SentCommand> {
  match connections {
    Connections::Centralized { ref commands, .. } => commands.write().await.drain(..).collect(),
    Connections::Clustered {
      ref cache,
      ref commands,
      ..
    } => zip_cluster_commands(cache, commands).await,
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
pub async fn zip_cluster_commands(
  cache: &Arc<RwLock<ClusterKeyCache>>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
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

    for (_, commands) in commands.write().await.iter_mut() {
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

pub fn spawn_clustered_listener(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
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
    let memo = (inner.clone(), server, counters, commands);

    let stream_ft = match stream {
      RedisStream::Tls(stream) => Either::Left(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
            responses::process_clustered_frame(&inner, &server, &counters, &commands, frame).await?;
            Ok((inner, server, counters, commands))
          })
          .and_then(|_| async { Ok(()) }),
      ),
      RedisStream::Tcp(stream) => Either::Right(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
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
    client_utils::set_client_state(&inner.state, ClientState::Disconnected);
    emit_connection_closed(&inner, &connections, error).await;

    Ok::<(), RedisError>(())
  });
}

pub async fn connect_clustered(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<VecDeque<SentCommand>, RedisError> {
  let pending_commands = take_sent_commands(connections).await;

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

    let (tx, _) = broadcast_channel(DEFAULT_BROADCAST_CAPACITY);
    for server in main_nodes.into_iter() {
      let (domain, addr) = protocol_utils::parse_cluster_server(inner, &server).await?;

      let (sink, stream) = if uses_tls {
        _trace!(inner, "Connecting to {} with domain {}", addr, domain);
        let socket = connection::create_authenticated_connection_tls(&addr, &domain, inner).await?;
        let socket = match connection::read_client_id(socket).await {
          Ok((id, socket)) => {
            if let Some(id) = id {
              connection_ids.write().insert(server.clone(), id);
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
        let socket = match connection::read_client_id(socket).await {
          Ok((id, socket)) => {
            if let Some(id) = id {
              connection_ids.write().insert(server.clone(), id);
            }
            socket
          }
          Err((_, socket)) => socket,
        };

        let (sink, stream) = socket.split();
        (RedisSink::Tcp(sink), RedisStream::Tcp(stream))
      };
      insert_locked_map_async(commands, server.clone(), VecDeque::new()).await;
      insert_locked_map_async(writers, server.clone(), sink).await;
      insert_locked_map(counters, server.clone(), Counters::new(&inner.cmd_buffer_len));

      spawn_clustered_listener(inner, connections, commands, counters, tx.subscribe(), &server, stream);
    }

    _debug!(inner, "Set clustered connection closed sender.");
    client_utils::set_locked(close_tx, Some(tx));
    client_utils::set_client_state(&inner.state, ClientState::Connected);
    Ok(pending_commands)
  } else {
    Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected clustered connections.",
    ))
  }
}

fn spawn_centralized_listener(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  connections: &Connections,
  mut close_rx: BroadcastReceiver<RedisError>,
  commands: &Arc<AsyncRwLock<VecDeque<SentCommand>>>,
  counters: &Counters,
  stream: RedisStream,
) {
  let inner = inner.clone();
  let commands = commands.clone();
  let counters = counters.clone();
  let connections = connections.clone();
  let server = server.clone();

  let _ = tokio::spawn(async move {
    let memo = (inner.clone(), server, counters, commands);

    let stream_ft = match stream {
      RedisStream::Tls(stream) => Either::Left(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
            responses::process_centralized_frame(&inner, &server, &counters, &commands, frame).await?;
            Ok((inner, server, counters, commands))
          })
          .and_then(|_| async { Ok(()) }),
      ),
      RedisStream::Tcp(stream) => Either::Right(
        stream
          .try_fold(memo, |(inner, server, counters, commands), frame| async {
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
    emit_connection_closed(&inner, &connections, error).await;

    Ok::<(), RedisError>(())
  });
}

pub async fn connect_centralized(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<VecDeque<SentCommand>, RedisError> {
  let pending_commands = take_sent_commands(connections).await;

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
      let socket = match connection::read_client_id(socket).await {
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
      let socket = match connection::read_client_id(socket).await {
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

    let (tx, rx) = broadcast_channel(DEFAULT_BROADCAST_CAPACITY);
    _debug!(inner, "Set centralized connection closed sender.");
    let _ = client_utils::set_locked(close_tx, Some(tx));
    let _ = client_utils::set_locked_async(&writer, Some(sink)).await;
    spawn_centralized_listener(inner, server, connections, rx, commands, counters, stream);
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
      let key_str = match key.as_str() {
        Some(s) => s,
        None => return Err(RedisError::new(RedisErrorKind::InvalidArgument, "Expected key string.")),
      };
      let hash_slot = redis_protocol::redis_keyslot(&key_str);
      let server = match cache.read().get_server(hash_slot) {
        Some(s) => s.id.clone(),
        None => {
          return Err(RedisError::new(
            RedisErrorKind::InvalidArgument,
            format!("Failed to find cluster node for {}", key_str),
          ))
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
      let key = match chunk[0].as_str() {
        Some(s) => s,
        None => return Err(RedisError::new(RedisErrorKind::InvalidArgument, "Expected key string.")),
      };
      let hash_slot = redis_protocol::redis_keyslot(&key);
      let server = match cache.read().get_server(hash_slot) {
        Some(s) => s.id.clone(),
        None => {
          return Err(RedisError::new(
            RedisErrorKind::InvalidArgument,
            format!("Failed to find cluster node for {}", key),
          ))
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
    let commands = Arc::new(AsyncRwLock::new(commands));

    let zipped: Vec<u64> = zip_cluster_commands(&cache, &commands)
      .await
      .into_iter()
      .map(|mut cmd| cmd.command.args.pop().unwrap().as_u64().unwrap())
      .collect();
    let expected = vec![1, 7, 5, 2, 8, 6, 3, 9, 4];

    assert_eq!(zipped, expected);

    for (_, commands) in commands.read().await.iter() {
      assert!(commands.is_empty());
    }
  }
}
