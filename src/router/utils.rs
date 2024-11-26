use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RouterCommand},
    connection::RedisConnection,
    types::*,
    utils as protocol_utils,
  },
  router::{centralized, clustered, responses, Counters, ReconnectServer, Router},
  runtime::RefCount,
  types::*,
  utils as client_utils,
};
use bytes::Bytes;
use std::{
  collections::VecDeque,
  time::{Duration, Instant},
};
use tokio::pin;

#[cfg(feature = "full-tracing")]
use tracing_futures::Instrument;

static OK_FRAME: Resp3Frame = Resp3Frame::SimpleString {
  data:       Bytes::from_static(b"OK"),
  attributes: None,
};

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

/// Write a command to the connection, returning whether the socket was flushed.
#[inline(always)]
pub async fn write_command(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
  mut command: RedisCommand,
  force_flush: bool,
) -> Result<bool, (RedisError, Option<RedisCommand>)> {
  if client_utils::read_bool_atomic(&command.timed_out) {
    _debug!(
      inner,
      "Ignore writing timed out command: {}",
      command.kind.to_str_debug()
    );
    return Ok(false);
  }
  let (frame, should_flush) = match prepare_command(inner, &conn.counters, &mut command) {
    Ok((frame, should_flush)) => (frame, should_flush || force_flush),
    Err(e) => {
      _warn!(inner, "Frame encoding error for {}", command.kind.to_str_debug());
      // do not retry commands that trigger frame encoding errors
      command.respond_to_caller(Err(e));
      return Ok(false);
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

  let check_unresponsive = !command.kind.is_pubsub() && inner.has_unresponsive_duration();
  let respond_early = if command.kind.closes_connection() {
    command.take_responder()
  } else {
    None
  };

  conn.push_command(command);
  let write_result = conn.write(frame, should_flush, check_unresponsive).await;
  if let Err(err) = write_result {
    debug!("{}: Error sending frame to socket: {:?}", conn.server, err);
    Err((err, None))
  } else {
    if let Some(tx) = respond_early {
      let _ = tx.send(Ok(OK_FRAME.clone()));
    }
    Ok(should_flush)
  }
}

pub fn defer_reconnection(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: Option<&Server>,
  error: RedisError,
  _replica: bool,
) -> Result<(), RedisError> {
  if !inner.should_reconnect() || error.should_not_reconnect() {
    return Err(error);
  }

  if router.has_pending_reconnection(&server) {
    _debug!(inner, "Skip defer reconnection.");
    Ok(())
  } else {
    _debug!(inner, "Defer reconnection to {:?} after {:?}", server, error);
    interfaces::send_to_router(inner, RouterCommand::Reconnect {
      server:                               server.cloned(),
      force:                                false,
      tx:                                   None,
      #[cfg(feature = "replicas")]
      replica:                              _replica,
    })
  }
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
            .cloned()
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
    if let Err(e) = Box::pin(router.retry_buffer()).await {
      _warn!(inner, "Error flushing retry buffer: {:?}", e);
      client_utils::set_client_state(&inner.state, ClientState::Disconnected);
      inner.notifications.broadcast_error(e.clone());
      return Err(e);
    }

    client_utils::set_client_state(&inner.state, ClientState::Connected);
    inner.notifications.broadcast_connect(Ok(()));
    inner.reset_reconnection_attempts();
    Ok(())
  }
}

/// Disconnect, broadcast events to callers, and remove cached connection info.
pub async fn disconnect(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
  error: &RedisError,
) -> VecDeque<RedisCommand> {
  let commands = conn.close().await;
  let commands = check_final_write_attempt(inner, commands, Some(error));

  if conn.replica {
    responses::broadcast_replica_error(inner, &conn.server, Some(error.clone()));
  } else {
    responses::broadcast_reader_error(inner, &conn.server, Some(error.clone()));
  }
  inner.backchannel.remove_connection_id(&conn.server);
  inner.backchannel.check_and_unblock(&conn.server);
  inner.remove_connection(&conn.server);
  commands
}

/// Disconnect and buffer any commands to be retried later.
pub async fn drop_connection(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  error: &RedisError,
) {
  _debug!(inner, "Resetting connection to {} with error: {:?}", server, error);
  if let Some(mut conn) = router.take_connection(server) {
    let commands = disconnect(inner, &mut conn, error).await;
    router.retry_commands(commands);
  }
}

/// Process the response frame from the provided server.
///
/// Errors returned here should interrupt the routing task.
pub async fn process_response(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  result: Option<Result<Resp3Frame, RedisError>>,
) -> Result<(), RedisError> {
  _trace!(inner, "Recv read result from {}", server);

  macro_rules! disconnect {
    ($inner:expr, $router:expr, $server:expr, $err:expr) => {{
      let replica = $router.is_replica($server);
      drop_connection($inner, $router, $server, &$err).await;
      defer_reconnection($inner, $router, Some($server), $err, replica)
    }};
  }

  match result {
    Some(Ok(frame)) => {
      let frame = match responses::preprocess_frame(inner, server, frame).await {
        Ok(frame) => frame,
        Err(err) => {
          _debug!(inner, "Error reading frame from server {}: {:?}", server, err);
          return disconnect!(inner, router, server, err);
        },
      };

      if let Some(frame) = frame {
        let conn = match router.get_connection_mut(server) {
          Some(conn) => conn,
          None => return Err(RedisError::new(RedisErrorKind::Unknown, "Missing expected connection.")),
        };

        if inner.config.server.is_clustered() {
          clustered::process_response_frame(inner, conn, frame)
        } else {
          centralized::process_response_frame(inner, conn, frame)
        }
      } else {
        Ok(())
      }
    },
    Some(Err(err)) => {
      _debug!(inner, "Error reading frame from server {}: {:?}", server, err);
      disconnect!(inner, router, server, err)
    },
    None => {
      _debug!(inner, "Connection closed to {}", server);
      let err = RedisError::new(RedisErrorKind::IO, "Connection closed.");
      disconnect!(inner, router, server, err)
    },
  }
}

/// Read from sockets while waiting for the provided duration.
pub async fn read_and_sleep(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  duration: Duration,
) -> Result<(), RedisError> {
  let sleep_ft = inner.wait_with_interrupt(duration);
  pin!(sleep_ft);

  loop {
    tokio::select! {
      result = &mut sleep_ft => return result,
      results = router.select_read(inner) => {
        for (server, result) in results.into_iter() {
          if let Err(err) = process_response(inner, router, &server, result).await {
            // defer reconnections until after waiting the full duration
            let replica = router.is_replica(&server);
            _debug!(inner, "Error reading from {} while sleeping: {:?}", server, err);
            drop_connection(inner, router, &server, &err).await;
            defer_reconnection(inner, router, Some(&server), err, replica)?;
          }
        }
      }
    }
  }
}

/// Reconnect to the server(s) until the max reconnect policy attempts are reached.
///
/// Errors from this function should end the connection task.
pub async fn reconnect_with_policy(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
) -> Result<(), RedisError> {
  let mut delay = next_reconnection_delay(inner)?;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      read_and_sleep(inner, router, delay).await?;
    }

    if let Err(e) = Box::pin(reconnect_once(inner, router)).await {
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
  macro_rules! next_sleep {
    ($err:expr) => {{
      let delay = match next_reconnection_delay(inner) {
        Ok(delay) => delay,
        Err(_) => {
          return Err(
            $err.unwrap_or_else(|| RedisError::new(RedisErrorKind::Unknown, "Unable to route command or reconnect.")),
          );
        },
      };
      let _ = read_and_sleep(inner, router, delay).await;
      continue;
    }};
  }

  loop {
    let mut command = RedisCommand::new_asking(slot);
    command.cluster_node = Some(server.clone());
    command.hasher = ClusterHash::Custom(slot);

    if attempts_remaining == 0 {
      return Err(RedisError::new(RedisErrorKind::Unknown, "Max attempts reached."));
    }
    attempts_remaining -= 1;

    let conn = match router.route(&command) {
      Some(conn) => conn,
      None => next_sleep!(None),
    };
    let frame = protocol_utils::encode_frame(inner, &command)?;
    if let Err(err) = conn.write(frame, true, false).await {
      next_sleep!(Some(err));
    }
    if let Err(err) = conn.flush().await {
      next_sleep!(Some(err));
    }
    if let Err(err) = conn.read_skip_pubsub(inner).await {
      next_sleep!(Some(err));
    } else {
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
      read_and_sleep(inner, router, delay).await?;
    }

    if let Err(e) = sync_cluster_replicas(inner, router, reset).await {
      _warn!(inner, "Error syncing replicas: {:?}", e);

      if e.should_not_reconnect() {
        break;
      } else {
        // return the underlying error on the last attempt
        delay = match next_reconnection_delay(inner) {
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
pub async fn delay_cluster_sync(inner: &RefCount<RedisClientInner>, router: &mut Router) -> Result<(), RedisError> {
  if inner.config.server.is_clustered() && !inner.connection.cluster_cache_update_delay.is_zero() {
    read_and_sleep(inner, router, inner.connection.cluster_cache_update_delay).await
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
      read_and_sleep(inner, router, delay).await?;
    }

    if let Err(e) = router.sync_cluster().await {
      _warn!(inner, "Error syncing cluster after redirect: {:?}", e);

      if e.should_not_reconnect() {
        break;
      } else {
        // return the underlying error on the last attempt
        delay = match next_reconnection_delay(inner) {
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
