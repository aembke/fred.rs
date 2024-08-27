use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RouterCommand, RouterResponse},
    connection::{RedisWriter, SharedBuffer, SplitStreamKind},
    responders::ResponseKind,
    types::*,
    utils as protocol_utils,
  },
  router::{utils, Backpressure, Counters, Router, Written},
  runtime::{oneshot_channel, sleep},
  types::*,
  utils as client_utils,
};
use futures::TryStreamExt;
use std::{
  cmp,
  sync::Arc,
  time::{Duration, Instant},
};

#[cfg(feature = "transactions")]
use crate::protocol::command::ClusterErrorKind;

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
    // TODO clean this up and write better docs
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
  let frame = protocol_utils::encode_frame(inner, command)?;

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
    || command.is_all_cluster_nodes()
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

  if !writer.is_working() {
    let error = RedisError::new(RedisErrorKind::IO, "Connection closed.");
    _debug!(inner, "Error sending command: {:?}", error);
    return Written::Disconnected((Some(writer.server.clone()), Some(command), error));
  }

  let no_incr = command.has_no_responses();
  writer.push_command(inner, command);
  if let Err(err) = writer.write_frame(frame, should_flush, no_incr).await {
    Written::Disconnected((Some(writer.server.clone()), None, err))
  } else {
    Written::Sent((writer.server.clone(), should_flush))
  }
}

/// Check the shared connection command buffer to see if the oldest command blocks the router task on a
/// response (not pipelined).
pub fn check_blocked_router(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  let command = match buffer.pop() {
    Some(cmd) => cmd,
    None => return,
  };
  if command.has_router_channel() {
    #[allow(unused_mut)]
    let mut tx = match command.take_router_tx() {
      Some(tx) => tx,
      None => return,
    };
    let error = error
      .clone()
      .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed"));

    if let Err(_) = tx.send(RouterResponse::ConnectionClosed((error, command))) {
      _warn!(inner, "Failed to send router connection closed error.");
    }
  } else {
    // this is safe to rearrange since the connection has closed and we can't guarantee command ordering when
    // connections close while an entire pipeline is in flight
    buffer.push(command);
  }
}

/// Filter the shared buffer, removing commands that reached the max number of attempts and responding to each caller
/// with the underlying error.
pub fn check_final_write_attempt(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  buffer
    .drain()
    .into_iter()
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
    .for_each(|command| {
      buffer.push(command);
    });
}

/// Read the next reconnection delay for the client.
pub fn next_reconnection_delay(inner: &Arc<RedisClientInner>) -> Result<Duration, RedisError> {
  inner
    .policy
    .write()
    .as_mut()
    .and_then(|policy| policy.next_delay())
    .map(Duration::from_millis)
    .ok_or_else(|| RedisError::new(RedisErrorKind::Canceled, "Max reconnection attempts reached."))
}

/// Attempt to reconnect and replay queued commands.
pub async fn reconnect_once(inner: &Arc<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  client_utils::set_client_state(&inner.state, ClientState::Connecting);
  if let Err(e) = Box::pin(router.connect()).await {
    _debug!(inner, "Failed reconnecting with error: {:?}", e);
    client_utils::set_client_state(&inner.state, ClientState::Disconnected);
    inner.notifications.broadcast_error(e.clone());
    Err(e)
  } else {
    #[cfg(feature = "replicas")]
    if let Err(e) = router.refresh_replica_routing().await {
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
      if e.should_not_reconnect() {
        return Err(e);
      }

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
#[cfg(feature = "transactions")]
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
      match client_utils::timeout(rx, inner.internal_command_timeout()).await {
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

#[cfg(feature = "replicas")]
async fn sync_cluster_replicas(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  reset: bool,
) -> Result<(), RedisError> {
  if reset {
    router.replicas.clear_connections(inner).await?;
  }

  if inner.config.server.is_clustered() {
    router.sync_cluster().await
  } else {
    router.sync_replicas().await
  }
}

/// Repeatedly try to sync the cluster state, reconnecting as needed until the max reconnection attempts is reached.
#[cfg(feature = "replicas")]
pub async fn sync_replicas_with_policy(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  reset: bool,
) -> Result<(), RedisError> {
  let mut delay = Duration::from_millis(0);

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = sync_cluster_replicas(inner, router, reset).await {
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

/// Wait for `inner.connection.cluster_cache_update_delay`.
pub async fn delay_cluster_sync(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  if inner.config.server.is_clustered() && !inner.connection.cluster_cache_update_delay.is_zero() {
    inner
      .wait_with_interrupt(inner.connection.cluster_cache_update_delay)
      .await
  } else {
    Ok(())
  }
}

/// Repeatedly try to sync the cluster state, reconnecting as needed until the max reconnection attempts is reached.
///
/// Errors from this function should end the connection task.
pub async fn sync_cluster_with_policy(inner: &Arc<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  let mut delay = Duration::from_millis(0);

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

/// Attempt to read the next frame from the reader half of a connection.
pub async fn next_frame(
  inner: &Arc<RedisClientInner>,
  conn: &mut SplitStreamKind,
  server: &Server,
  buffer: &SharedBuffer,
) -> Result<Option<ProtocolFrame>, RedisError> {
  if let Some(ref max_resp_latency) = inner.connection.unresponsive.max_timeout {
    // These shenanigans were implemented in an attempt to strike a balance between a few recent changes.
    //
    // The entire request-response path can be lock-free if we use crossbeam-queue types under the shared buffer
    // between socket halves, but these types do not support `peek` or `push_front`. Unfortunately this really limits
    // or prevents most forms of conditional `pop_front` use cases. There are 3-4 places in the code where this
    // matters, and this is one of them.
    //
    // The `UnresponsiveConfig` interface implements a heuristic where callers can express that a connection should be
    // considered unresponsive if a command waits too long for a response. Before switching to crossbeam types we used
    // a `Mutex<VecDeque>` container which made this scenario easier to implement, but with crossbeam types it's more
    // complicated.
    //
    // The approach here implements a ~~hack~~ heuristic where we measure the time since first noticing a new
    // frame in the shared buffer from the reader task's perspective. This only works because we use `Stream::next`
    // which is noted to be cancellation-safe in the tokio::select! docs. With this implementation the worst case
    // error margin is an extra `interval`.

    let mut last_frame_sent: Option<Instant> = None;
    'outer: loop {
      tokio::select! {
        // prefer polling the connection first
        biased;
        frame = conn.try_next() => return frame,

        // repeatedly check the duration since we first noticed a pending frame
        _ = sleep(inner.connection.unresponsive.interval) => {
          _trace!(inner, "Checking unresponsive connection to {}", server);

          // continue early if the buffer is empty or we're waiting on a blocking command. this isn't ideal, but
          // this strategy just doesn't work well with blocking commands.
          let buffer_len = buffer.len();
          if buffer_len == 0 || buffer.is_blocked() {
            last_frame_sent = None;
            continue 'outer;
          } else if buffer_len > 0 && last_frame_sent.is_none() {
            _trace!(inner, "Observed new request frame in unresponsive loop");
            last_frame_sent = Some(Instant::now());
          }

          if let Some(ref last_frame_sent) = last_frame_sent {
            let latency = Instant::now().saturating_duration_since(*last_frame_sent);
            if latency > *max_resp_latency {
              _warn!(inner, "Unresponsive connection to {} after {:?}", server, latency);
              inner.notifications.broadcast_unresponsive(server.clone());
              return Err(RedisError::new(RedisErrorKind::IO, "Unresponsive connection."))
            }
          }
        },
      }
    }
  } else {
    _trace!(inner, "Skip waiting on interrupt rx.");
    conn.try_next().await
  }
}

#[cfg(test)]
mod tests {}
