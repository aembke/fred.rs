use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{command::RedisCommand, connection::RedisConnection, types::*, utils as protocol_utils},
  router::{utils, Backpressure, Counters, Router, WriteResult},
  runtime::RefCount,
  types::*,
  utils as client_utils,
};
use std::{
  cmp,
  collections::VecDeque,
  time::{Duration, Instant},
};

/// Check the connection state and command flags to determine the backpressure policy to apply, if any.
pub fn check_backpressure(
  inner: &RefCount<RedisClientInner>,
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
fn set_command_trace(inner: &RefCount<RedisClientInner>, command: &mut RedisCommand) {
  if inner.should_trace() {
    crate::trace::set_network_span(inner, command, true);
  }
}

#[cfg(not(feature = "partial-tracing"))]
fn set_command_trace(_inner: &RefCount<RedisClientInner>, _: &mut RedisCommand) {}

/// Prepare the command, updating flags in place.
///
/// Returns the RESP frame and whether the socket should be flushed.
pub fn prepare_command(
  inner: &RefCount<RedisClientInner>,
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
  let should_flush = counters.should_send(inner) || command.kind.should_flush() || command.is_all_cluster_nodes();
  command.network_start = Some(Instant::now());
  set_command_trace(inner, command);

  Ok((frame, should_flush))
}

/// Write a command to the connection.
#[inline(always)]
pub async fn write_command(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
  mut command: RedisCommand,
  force_flush: bool,
) -> WriteResult {
  if client_utils::read_bool_atomic(&command.timed_out) {
    _debug!(
      inner,
      "Ignore writing timed out command: {}",
      command.kind.to_str_debug()
    );
    return WriteResult::Ignore;
  }
  match check_backpressure(inner, &conn.counters, &command) {
    Ok(Some(backpressure)) => {
      _trace!(inner, "Returning backpressure for {}", command.kind.to_str_debug());
      return WriteResult::Backpressure((command, backpressure));
    },
    Err(e) => {
      // return manual backpressure errors directly to the caller
      command.respond_to_caller(Err(e));
      return WriteResult::Ignore;
    },
    _ => {},
  };
  let (frame, should_flush) = match prepare_command(inner, &conn.counters, &mut command) {
    Ok((frame, should_flush)) => (frame, should_flush || force_flush),
    Err(e) => {
      _warn!(inner, "Frame encoding error for {}", command.kind.to_str_debug());
      // do not retry commands that trigger frame encoding errors
      command.respond_to_caller(Err(e));
      return WriteResult::Ignore;
    },
  };

  _trace!(
    inner,
    "Sending command {} ({}) to {}",
    command.kind.to_str_debug(),
    command.debug_id(),
    conn.server
  );
  command.write_attempts += 1;

  // TODO is this still necessary in the write path?
  // if let Some(error) = conn.peek_reader_errors().await {
  // _debug!(inner, "Error sending command: {:?}", error);
  // return WriteResult::Disconnected((Some(conn.server.clone()), Some(command), error));
  // }
  //

  let no_incr = command.has_no_responses();
  let check_unresponsive = !command.kind.is_pubsub() && inner.has_unresponsive_duration();
  conn.push_command(command);
  let write_result = conn.write(frame, should_flush, no_incr, check_unresponsive).await;

  if let Err(e) = write_result {
    debug!("{}: Error sending frame to socket: {:?}", conn.server, e);
    WriteResult::Disconnected((Some(conn.server.clone()), None, e))
  } else {
    WriteResult::Sent((conn.server.clone(), should_flush))
  }
}

pub async fn remove_cached_connection_id(inner: &RefCount<RedisClientInner>, server: &Server) {
  inner.backchannel.write().await.remove_connection_id(server);
}

/// Filter the shared buffer, removing commands that reached the max number of attempts and responding to each caller
/// with the underlying error.
pub fn check_final_write_attempt(
  inner: &RefCount<RedisClientInner>,
  buffer: VecDeque<RedisCommand>,
  error: Option<&RedisError>,
) -> VecDeque<RedisCommand> {
  buffer
    .into_iter()
    .filter_map(|mut command| {
      if command.should_finish_with_error(inner) {
        command.respond_to_caller(Err(
          error
            .map(|e| e.clone())
            .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed")),
        ));

        None
      } else {
        Some(command)
      }
    })
    .collect()
}

/// Read the next reconnection delay for the client.
pub fn next_reconnection_delay(inner: &RefCount<RedisClientInner>) -> Result<Duration, RedisError> {
  inner
    .policy
    .write()
    .as_mut()
    .and_then(|policy| policy.next_delay())
    .map(Duration::from_millis)
    .ok_or_else(|| RedisError::new(RedisErrorKind::Canceled, "Max reconnection attempts reached."))
}

/// Attempt to reconnect and replay queued commands.
pub async fn reconnect_once(inner: &RefCount<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
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
pub async fn reconnect_with_policy(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
) -> Result<(), RedisError> {
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

/// Send `ASKING` to the provided server, reconnecting as needed.
pub async fn send_asking_with_policy(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  slot: u16,
  mut attempts_remaining: u32,
) -> Result<(), RedisError> {
  loop {
    let mut command = RedisCommand::new_asking(slot);
    command.cluster_node = Some(server.clone());

    if attempts_remaining == 0 {
      return Err(RedisError::new(RedisErrorKind::Unknown, "Max attempts reached."));
    }
    attempts_remaining -= 1;

    match router.write(command, true).await {
      WriteResult::Disconnected((_, _, err)) => {
        if attempts_remaining == 0 {
          return Err(err);
        }

        if let Err(err) = reconnect_with_policy(inner, router).await {
          return Err(err);
        } else {
          continue;
        }
      },
      WriteResult::NotFound(_) => {
        if attempts_remaining == 0 {
          return Err(RedisError::new(
            RedisErrorKind::Unknown,
            "Failed to route ASKING command.",
          ));
        }

        if let Err(err) = reconnect_with_policy(inner, router).await {
          return Err(err);
        } else {
          continue;
        }
      },
      WriteResult::Error((err, _)) => {
        return Err(err);
      },
      _ => {},
    };

    if let Err(err) = router.drain().await {
      if attempts_remaining == 0 {
        return Err(err);
      }

      _debug!(inner, "Failed to read ASKING response: {:?}", err);
      if let Err(err) = reconnect_with_policy(inner, router).await {
        return Err(err);
      } else {
        continue;
      }
    } else {
      // ASKING always responds with OK so we don't need to read the frame
      break;
    }
  }

  inner.reset_reconnection_attempts();
  Ok(())
}

#[cfg(feature = "replicas")]
async fn sync_cluster_replicas(
  inner: &RefCount<RedisClientInner>,
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
  inner: &RefCount<RedisClientInner>,
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
pub async fn delay_cluster_sync(inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
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
pub async fn sync_cluster_with_policy(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
) -> Result<(), RedisError> {
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

macro_rules! next_frame {
  ($inner:expr, $conn:expr) => {{
    if let Some(duration) = $inner.connection.unresponsive.max_timeout {
      if $conn.buffer.is_empty() {
        $conn.read().await
      } else {
        if let Some(ref last_written) = $conn.last_write {
          if last_written.elapsed() > duration {
            // check for frames waiting a response across multiple calls to poll_read
            $inner.notifications.broadcast_unresponsive($conn.server.clone());
            Err(RedisError::new(RedisErrorKind::IO, "Unresponsive connection."))
          } else {
            client_utils::timeout($conn.read(), duration).await
          }
        } else {
          $conn.read().await
        }
      }
    } else {
      $conn.read().await
    }
  }};
}

pub(crate) use next_frame;
