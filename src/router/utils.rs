use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  modules::inner::RedisClientInner,
  prelude::Resp3Frame,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RedisCommandKind, RouterCommand, RouterResponse},
    connection::{RedisWriter, SharedBuffer, SplitStreamKind},
    responders::ResponseKind,
    types::*,
  },
  router::{utils, Backpressure, Counters, Router, Written},
  types::*,
  utils as client_utils,
};
use futures::TryStreamExt;
use redis_protocol::resp3::types::PUBSUB_PUSH_PREFIX;
use std::{
  cmp,
  sync::Arc,
  time::{Duration, Instant},
};
use tokio::{
  self,
  sync::{mpsc::UnboundedReceiver, oneshot::channel as oneshot_channel},
};

#[cfg(feature = "check-unresponsive")]
use futures::future::Either;
#[cfg(feature = "check-unresponsive")]
use tokio::pin;

/// Check the connection state and command flags to determine the backpressure policy to apply, if any.
pub fn check_backpressure(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  command: &RedisCommand,
) -> Result<Option<Backpressure>, RedisError> {
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
            min_sleep_duration,
          } => {
            let duration = if disable_backpressure_scaling {
              min_sleep_duration
            } else {
              Duration::from_millis(cmp::max(min_sleep_duration.as_millis() as u64, in_flight as u64))
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

#[cfg(feature = "partial-tracing")]
fn set_command_trace(inner: &Arc<RedisClientInner>, command: &mut RedisCommand) {
  if inner.should_trace() {
    crate::trace::set_network_span(inner, command, true);
  }
}

#[cfg(not(feature = "partial-tracing"))]
fn set_command_trace(_inner: &Arc<RedisClientInner>, _: &mut RedisCommand) {}

/// Prepare the command, updating flags in place.
///
/// Returns the RESP frame and whether the socket should be flushed.
pub fn prepare_command(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  command: &mut RedisCommand,
) -> Result<(ProtocolFrame, bool), RedisError> {
  let frame = command.to_frame(inner.is_resp3())?;

  // flush the socket under any of the following conditions:
  // * we don't know of any queued commands following this command
  // * we've fed up to the max feed count commands already
  // * the command closes the connection
  // * the command ends a transaction
  // * the command does some form of authentication
  // * the command goes to multiple sockets at once
  // * the command blocks the router command loop
  let should_flush = counters.should_send(inner)
    || command.kind.should_flush()
    || command.kind.is_all_cluster_nodes()
    || command.has_router_channel();

  command.network_start = Some(Instant::now());
  set_command_trace(inner, command);

  Ok((frame, should_flush))
}

/// Write a command on the provided writer half of a socket.
pub async fn write_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut RedisWriter,
  mut command: RedisCommand,
  force_flush: bool,
) -> Written {
  _trace!(
    inner,
    "Writing {} ({}). Timed out: {}, Force flush: {}",
    command.kind.to_str_debug(),
    command.debug_id(),
    client_utils::read_bool_atomic(&command.timed_out),
    force_flush
  );
  if client_utils::read_bool_atomic(&command.timed_out) {
    _debug!(
      inner,
      "Ignore writing timed out command: {}",
      command.kind.to_str_debug()
    );
    return Written::Ignore;
  }

  match check_backpressure(inner, &writer.counters, &command) {
    Ok(Some(backpressure)) => {
      _trace!(inner, "Returning backpressure for {}", command.kind.to_str_debug());
      return Written::Backpressure((command, backpressure));
    },
    Err(e) => {
      // return manual backpressure errors directly to the caller
      command.finish(inner, Err(e));
      return Written::Ignore;
    },
    _ => {},
  };

  let (frame, should_flush) = match prepare_command(inner, &writer.counters, &mut command) {
    Ok((frame, should_flush)) => (frame, should_flush || force_flush),
    Err(e) => {
      _warn!(inner, "Frame encoding error for {}", command.kind.to_str_debug());
      // do not retry commands that trigger frame encoding errors
      command.finish(inner, Err(e));
      return Written::Ignore;
    },
  };

  _trace!(
    inner,
    "Sending command {} ({}) to {}",
    command.kind.to_str_debug(),
    command.debug_id(),
    writer.server
  );
  command.write_attempts += 1;
  writer.push_command(inner, command);
  if let Err(e) = writer.write_frame(frame, should_flush).await {
    let command = writer.pop_recent_command();
    _debug!(inner, "Error sending command: {:?}", e);
    Written::Disconnected((Some(writer.server.clone()), command, e))
  } else {
    Written::Sent((writer.server.clone(), should_flush))
  }
}

/// Check the shared connection command buffer to see if the oldest command blocks the router task on a
/// response (not pipelined).
pub fn check_blocked_router(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  let command = {
    let mut guard = buffer.lock();
    let should_pop = guard
      .front()
      .map(|command| command.has_router_channel())
      .unwrap_or(false);

    if should_pop {
      guard.pop_front().unwrap()
    } else {
      return;
    }
  };

  let tx = match command.take_router_tx() {
    Some(tx) => tx,
    None => return,
  };
  let error = error
    .clone()
    .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed"));

  if let Err(_) = tx.send(RouterResponse::ConnectionClosed((error, command))) {
    _warn!(inner, "Failed to send router connection closed error.");
  }
}

/// Filter the shared buffer, removing commands that reached the max number of attempts and responding to each caller
/// with the underlying error.
pub fn check_final_write_attempt(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  let mut guard = buffer.lock();
  let commands = guard
    .drain(..)
    .filter_map(|command| {
      if command.should_finish_with_error(inner) {
        command.finish(
          inner,
          Err(
            error
              .clone()
              .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed")),
          ),
        );

        None
      } else {
        Some(command)
      }
    })
    .collect();

  *guard = commands;
}

/// Check whether to drop the frame if it was sent in response to a pubsub command as a part of an unknown number of
/// response frames.
// This is a special case for when UNSUBSCRIBE, PUNSUBSCRIBE, or SUNSUBSCRIBE are called without arguments, which has
// the effect of unsubscribing from every channel and sending one message per channel to the client in response.
// However, in this scenario the client does not know how many responses to expect, so we discard all of them unless
// we know the client expects the current response frame.
pub fn should_drop_extra_pubsub_frame(
  inner: &Arc<RedisClientInner>,
  command: &RedisCommand,
  frame: &Resp3Frame,
) -> bool {
  let from_unsubscribe = match frame {
    Resp3Frame::Array { ref data, .. } | Resp3Frame::Push { ref data, .. } => {
      if data.len() >= 3 && data.len() <= 4 {
        // check for ["pubsub", "punsubscribe"|"sunsubscribe", ..] or ["punsubscribe"|"sunsubscribe", ..]
        (data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1]
            .as_str()
            .map(|s| s == "unsubscribe" || s == "punsubscribe" || s == "sunsubscribe")
            .unwrap_or(false))
          || (data[0]
            .as_str()
            .map(|s| s == "unsubscribe" || s == "punsubscribe" || s == "sunsubscribe")
            .unwrap_or(false))
      } else {
        false
      }
    },
    _ => return false,
  };

  let should_drop = if from_unsubscribe {
    match command.kind {
      // frame is from an unsubscribe call and the current frame expects it, so don't drop it
      RedisCommandKind::Unsubscribe | RedisCommandKind::Punsubscribe | RedisCommandKind::Sunsubscribe => false,
      // frame is from an unsubscribe call and the current command does not expect it, so drop it
      _ => true,
    }
  } else {
    false
  };

  if should_drop {
    _debug!(inner, "Dropping extra unsubscribe response.");
  }
  should_drop
}

/// Read the next reconnection delay for the client.
pub fn next_reconnection_delay(inner: &Arc<RedisClientInner>) -> Result<Duration, RedisError> {
  inner
    .policy
    .write()
    .as_mut()
    .and_then(|policy| policy.next_delay())
    .map(Duration::from_millis)
    .ok_or(RedisError::new(
      RedisErrorKind::Canceled,
      "Max reconnection attempts reached.",
    ))
}

/// Attempt to reconnect and replay queued commands.
pub async fn reconnect_once(inner: &Arc<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  client_utils::set_client_state(&inner.state, ClientState::Connecting);
  if let Err(e) = router.connect().await {
    _debug!(inner, "Failed reconnecting with error: {:?}", e);
    client_utils::set_client_state(&inner.state, ClientState::Disconnected);
    inner.notifications.broadcast_error(e.clone());
    Err(e)
  } else {
    if let Err(e) = router.sync_replicas().await {
      _warn!(inner, "Error syncing replicas: {:?}", e);
      if !inner.ignore_replica_reconnect_errors() {
        client_utils::set_client_state(&inner.state, ClientState::Disconnected);
        inner.notifications.broadcast_error(e.clone());
        return Err(e);
      }
    }
    // try to flush any previously in-flight commands
    router.retry_buffer().await;

    client_utils::set_client_state(&inner.state, ClientState::Connected);
    inner.notifications.broadcast_connect(Ok(()));
    inner.reset_reconnection_attempts();
    Ok(())
  }
}

/// Reconnect to the server(s) until the max reconnect policy attempts are reached.
///
/// Errors from this function should end the connection task.
pub async fn reconnect_with_policy(inner: &Arc<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  let mut delay = utils::next_reconnection_delay(inner)?;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = reconnect_once(inner, router).await {
      delay = match next_reconnection_delay(inner) {
        Ok(delay) => delay,
        Err(_) => return Err(e),
      };

      continue;
    } else {
      break;
    }
  }

  Ok(())
}

/// Attempt to follow a cluster redirect, reconnecting as needed until the max reconnections attempts is reached.
pub async fn cluster_redirect_with_policy(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  kind: ClusterErrorKind,
  slot: u16,
  server: &Server,
) -> Result<(), RedisError> {
  let mut delay = inner.connection.cluster_cache_update_delay;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = router.cluster_redirection(&kind, slot, server).await {
      delay = next_reconnection_delay(inner).map_err(|_| e)?;

      continue;
    } else {
      break;
    }
  }

  Ok(())
}

/// Repeatedly try to send `ASKING` to the provided server, reconnecting as needed.f
///
/// Errors from this function should end the connection task.
pub async fn send_asking_with_policy(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  slot: u16,
) -> Result<(), RedisError> {
  let mut delay = inner.connection.cluster_cache_update_delay;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      inner.wait_with_interrupt(delay).await?;
    }

    if !router.connections.has_server_connection(server) {
      if let Err(e) = router.sync_cluster().await {
        _debug!(inner, "Error syncing cluster before ASKING: {:?}", e);
        delay = utils::next_reconnection_delay(inner)?;
        continue;
      }
    }

    let mut command = RedisCommand::new_asking(slot);
    let (tx, rx) = oneshot_channel();
    command.skip_backpressure = true;
    command.response = ResponseKind::Respond(Some(tx));

    let result = match router.write_direct(command, server).await {
      Written::Error((error, _)) => Err(error),
      Written::Disconnected((_, _, error)) => Err(error),
      Written::NotFound(_) => Err(RedisError::new(RedisErrorKind::Cluster, "Connection not found.")),
      _ => Ok(()),
    };

    if let Err(error) = result {
      if error.should_not_reconnect() {
        break;
      } else if let Err(_) = reconnect_once(inner, router).await {
        delay = utils::next_reconnection_delay(inner)?;
        continue;
      } else {
        delay = Duration::from_millis(0);
        continue;
      }
    } else {
      match client_utils::apply_timeout(rx, inner.internal_command_timeout()).await {
        Ok(Err(e)) => {
          // error writing the command
          _debug!(inner, "Reconnect once after error from ASKING: {:?}", e);
          if let Err(_) = reconnect_once(inner, router).await {
            delay = utils::next_reconnection_delay(inner)?;
            continue;
          } else {
            delay = Duration::from_millis(0);
            continue;
          }
        },
        Err(e) => {
          // command was dropped due to connection closing
          _debug!(inner, "Reconnect once after rx error from ASKING: {:?}", e);
          if let Err(_) = reconnect_once(inner, router).await {
            delay = utils::next_reconnection_delay(inner)?;
            continue;
          } else {
            delay = Duration::from_millis(0);
            continue;
          }
        },
        _ => break,
      }
    }
  }

  inner.reset_reconnection_attempts();
  Ok(())
}

/// Repeatedly try to sync the cluster state, reconnecting as needed until the max reconnection attempts is reached.
#[cfg(feature = "replicas")]
pub async fn sync_replicas_with_policy(inner: &Arc<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  let mut delay = utils::next_reconnection_delay(inner)?;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      let _ = inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = router.sync_replicas().await {
      _warn!(inner, "Error syncing replicas: {:?}", e);

      if e.should_not_reconnect() {
        break;
      } else {
        // return the underlying error on the last attempt
        delay = match utils::next_reconnection_delay(inner) {
          Ok(delay) => delay,
          Err(_) => return Err(e),
        };

        continue;
      }
    } else {
      break;
    }
  }

  Ok(())
}

/// Repeatedly try to sync the cluster state, reconnecting as needed until the max reconnection attempts is reached.
///
/// Errors from this function should end the connection task.
pub async fn sync_cluster_with_policy(inner: &Arc<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  let mut delay = inner.connection.cluster_cache_update_delay;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = router.sync_cluster().await {
      _warn!(inner, "Error syncing cluster after redirect: {:?}", e);

      if e.should_not_reconnect() {
        break;
      } else {
        // return the underlying error on the last attempt
        delay = match utils::next_reconnection_delay(inner) {
          Ok(delay) => delay,
          Err(_) => return Err(e),
        };

        continue;
      }
    } else {
      break;
    }
  }

  Ok(())
}

#[cfg(feature = "replicas")]
pub fn defer_replica_sync(inner: &Arc<RedisClientInner>) {
  let (tx, _) = oneshot_channel();
  let cmd = RouterCommand::SyncReplicas { tx };
  if let Err(_) = interfaces::send_to_router(inner, cmd) {
    _warn!(inner, "Failed to start deferred replica sync.")
  }
}

pub fn defer_reconnect(inner: &Arc<RedisClientInner>) {
  if inner.config.server.is_clustered() {
    let (tx, _) = oneshot_channel();
    let cmd = RouterCommand::SyncCluster { tx };
    if let Err(_) = interfaces::send_to_router(inner, cmd) {
      _warn!(inner, "Failed to send deferred cluster sync.")
    }
  } else {
    let cmd = RouterCommand::Reconnect {
      server:                               None,
      tx:                                   None,
      force:                                false,
      #[cfg(feature = "replicas")]
      replica:                              false,
    };
    if let Err(_) = interfaces::send_to_router(inner, cmd) {
      _warn!(inner, "Failed to send deferred cluster sync.")
    }
  }
}

#[cfg(feature = "check-unresponsive")]
pub async fn next_frame(
  inner: &Arc<RedisClientInner>,
  conn: &mut SplitStreamKind,
  server: &Server,
  rx: &mut Option<UnboundedReceiver<()>>,
) -> Result<Option<ProtocolFrame>, RedisError> {
  if let Some(interrupt_rx) = rx.as_mut() {
    let recv_ft = interrupt_rx.recv();
    let frame_ft = conn.try_next();
    pin!(recv_ft);
    pin!(frame_ft);

    match futures::future::select(recv_ft, frame_ft).await {
      Either::Left((Some(_), _)) => {
        _debug!(inner, "Recv interrupt on {}", server);
        Err(RedisError::new(RedisErrorKind::IO, "Unresponsive connection."))
      },
      Either::Left((None, frame_ft)) => {
        _debug!(inner, "Interrupt channel closed for {}", server);
        frame_ft.await
      },
      Either::Right((frame, _)) => frame,
    }
  } else {
    _debug!(inner, "Skip waiting on interrupt rx.");
    conn.try_next().await
  }
}

#[cfg(not(feature = "check-unresponsive"))]
pub async fn next_frame(
  _: &Arc<RedisClientInner>,
  conn: &mut SplitStreamKind,
  _: &Server,
  _: &mut Option<UnboundedReceiver<()>>,
) -> Result<Option<ProtocolFrame>, RedisError> {
  conn.try_next().await
}

#[cfg(feature = "check-unresponsive")]
pub fn reader_subscribe(inner: &Arc<RedisClientInner>, server: &Server) -> Option<UnboundedReceiver<()>> {
  Some(inner.network_timeouts.state().subscribe(inner, server))
}

#[cfg(not(feature = "check-unresponsive"))]
pub fn reader_subscribe(_: &Arc<RedisClientInner>, _: &Server) -> Option<UnboundedReceiver<()>> {
  None
}

#[cfg(feature = "check-unresponsive")]
pub fn reader_unsubscribe(inner: &Arc<RedisClientInner>, server: &Server) {
  inner.network_timeouts.state().unsubscribe(inner, server);
}

#[cfg(not(feature = "check-unresponsive"))]
pub fn reader_unsubscribe(_: &Arc<RedisClientInner>, _: &Server) {}

#[cfg(test)]
mod tests {}
