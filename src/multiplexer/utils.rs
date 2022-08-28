use crate::clients::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::types::ClusterChange;
use crate::multiplexer::{responses, utils, Backpressure, Connections, Counters, Written};
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::connection::{self, RedisReader, RedisTransport, RedisWriter};
use crate::protocol::types::*;
use crate::protocol::{responders, utils as protocol_utils};
use crate::trace;
use crate::types::*;
use crate::utils as client_utils;
use arcstr::ArcStr;
use futures::future::Either;
use futures::{pin_mut, select, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use log::Level;
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, mem, str};
use tokio;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast::{channel as broadcast_channel, Receiver as BroadcastReceiver};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::RwLock as AsyncRwLock;

const DEFAULT_BROADCAST_CAPACITY: usize = 16;

pub async fn initialize_connections<T>(
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections<T>,
) -> Result<(), RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  unimplemented!()
}

/// Check the connection state and command flags to determine the backpressure policy to apply, if any.
pub fn check_backpressure<T>(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  command: &RedisCommand,
) -> Result<Option<Backpressure>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  if command.skip_backpressure {
    return Ok(None);
  }
  let in_flight = client_utils::read_atomic(&counters.in_flight);

  inner.with_perf_config(|perf_config| {
    if in_flight as u64 > perf_config.backpressure.max_in_flight_commands {
      if perf_config.backpressure.disable_auto_backpressure {
        Err(RedisError::new_backpressure())
      } else {
        match perf_config.backpressure.policy {
          BackpressurePolicy::Drain => Ok(Some(Backpressure::Block)),
          BackpressurePolicy::Sleep {
            disable_backpressure_scaling,
            min_sleep_duration_ms,
          } => {
            let duration = if disable_backpressure_scaling {
              Duration::from_millis(min_sleep_duration_ms)
            } else {
              Duration::from_millis(cmp::max(min_sleep_duration_ms, in_flight as u64))
            };

            Ok(Some(Backpressure::Wait(duration)))
          },
        }
      }
    } else {
      Ok(None)
    }
  })
}

/// Prepare the command, updating flags in place.
///
/// Returns the RESP frame and whether the socket should be flushed.
pub fn prepare_command(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  command: &mut RedisCommand,
) -> Result<(ProtocolFrame, bool), RedisError> {
  let frame = command.to_frame(inner.is_resp3())?;
  if inner.should_trace() {
    command.network_start = Some(Instant::now());
    trace::set_network_span(command, true);
  }
  // flush the socket under any of the following conditions:
  // * we don't know of any queued commands following this command
  // * we've fed up to the max feed count commands already
  // * the command closes the connection
  // * the command ends a transaction
  // * the command does some form of authentication
  // * the command goes to multiple sockets at once
  // * the command blocks the multiplexer command loop
  let should_flush = counters.should_send(inner)
    || command.kind.should_flush()
    || command.kind.is_all_cluster_nodes()
    || command.multiplexer_tx.is_some();

  Ok((frame, should_flush))
}

/// Write a command on the provided writer half of a socket.
pub async fn write_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut RedisWriter,
  mut command: RedisCommand,
) -> Written {
  match utils::check_backpressure(inner, &writer.counters, &command) {
    Ok(Some(backpressure)) => {
      _trace!(inner, "Returning backpressure for {}", command.kind.to_str_debug());
      return Written::Backpressure((command, backpressure));
    },
    Err(e) => {
      // return manual backpressure errors directly to the caller
      command.respond_to_caller(Err(e));
      return Written::Ignore;
    },
    _ => {},
  };

  let (frame, should_flush) = match utils::prepare_command(inner, &writer.counters, &mut command) {
    Ok((frame, should_flush)) => (frame, should_flush),
    Err(e) => {
      // do not retry commands that trigger frame encoding errors
      command.respond_to_caller(Err(e));
      return Written::Ignore;
    },
  };

  if let Err(e) = writer.write_frame(frame, should_flush).await {
    _debug!(inner, "Error sending command {}: {:?}", command.kind.to_str_debug(), e);
    if command.should_send_write_error(inner) {
      command.respond_to_caller(Err(e));
      Written::Disconnect((server, None))
    } else {
      inner.notifications.broadcast_error(e);
      Written::Disconnect((server, Some(command)))
    }
  } else {
    _trace!(inner, "Successfully sent command {}", command.kind.to_str_debug());
    writer.buffer.lock().await.push_back(command);
    Written::Sent((server, should_flush))
  }
}

// --------------------------------------------------------------------------------------------------------

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
      },
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
      },
      Err((_, socket)) => socket,
    };

    let (sink, stream) = socket.split();
    Ok((RedisSink::Tcp(sink), RedisStream::Tcp(stream)))
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
        },
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
        },
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
        },
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
        },
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

async fn remove_server<T>(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<ArcStr, RedisWriter<T>>,
  server: &ArcStr,
) -> Result<Option<VecDeque<SentCommand>>, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  _debug!(inner, "Removing clustered connection to server {}", server);

  let mut writer = match writers.remove(server) {
    Some(writer) => writer,
    None => return Ok(None),
  };
  let commands = writer.buffer.lock().drain(..).collect();

  Ok(commands)
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
      },
    };

    if let Resp3Frame::BlobString { data, .. } = frame {
      let response = str::from_utf8(&data)?;
      let state = match ClusterKeyCache::new(Some(response)) {
        Ok(state) => state,
        Err(e) => {
          _warn!(inner, "Error parsing cluster nodes response from backchannel: {:?}", e);
          continue;
        },
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

#[cfg(test)]
mod tests {}
