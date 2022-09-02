use crate::clients::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::types::ClusterChange;
use crate::multiplexer::{responses, utils, Backpressure, Connections, Counters, Multiplexer, Written};
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::connection::{self, CommandBuffer, RedisReader, RedisTransport, RedisWriter, SharedBuffer};
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
    writer.buffer.lock().await.push_back(command);
    _trace!(inner, "Successfully sent command {}", command.kind.to_str_debug());
    Written::Sent((server, should_flush))
  }
}

// --------------------------------------------------------------------------------------------------------

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
      let server = match cache.get_server(hash_slot) {
        Some(s) => s,
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
      let server = match cache.get_server(hash_slot) {
        Some(s) => s,
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
