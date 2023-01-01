use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  multiplexer::{utils, Backpressure, Counters, Multiplexer, Written},
  prelude::Resp3Frame,
  protocol::{
    command::{ClusterErrorKind, MultiplexerResponse, RedisCommand, RedisCommandKind},
    connection::{RedisWriter, SharedBuffer, SplitStreamKind},
    responders::ResponseKind,
    types::*,
  },
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

#[cfg(any(feature = "metrics", feature = "partial-tracing"))]
use crate::trace;
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

#[cfg(feature = "partial-tracing")]
fn set_command_trace(inner: &Arc<RedisClientInner>, command: &mut RedisCommand) {
  if inner.should_trace() {
    trace::set_network_span(inner, command, true);
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
  // * the command blocks the multiplexer command loop
  let should_flush = counters.should_send(inner)
    || command.kind.should_flush()
    || command.kind.is_all_cluster_nodes()
    || command.has_multiplexer_channel();

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
    "Writing command {}. Timed out: {}, Force flush: {}",
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
      command.respond_to_caller(Err(e));
      return Written::Ignore;
    },
    _ => {},
  };

  let (frame, should_flush) = match prepare_command(inner, &writer.counters, &mut command) {
    Ok((frame, should_flush)) => (frame, should_flush || force_flush),
    Err(e) => {
      _warn!(inner, "Frame encoding error for {}", command.kind.to_str_debug());
      // do not retry commands that trigger frame encoding errors
      command.respond_to_caller(Err(e));
      return Written::Ignore;
    },
  };

  _trace!(
    inner,
    "Sending command {} to {}, ID: {}",
    command.kind.to_str_debug(),
    writer.server,
    command.debug_id()
  );
  // TODO i don't think we need to hold a lock across this await point...
  writer.push_command(command);
  if let Err(e) = writer.write_frame(frame, should_flush).await {
    let mut command = match writer.pop_recent_command() {
      Some(cmd) => cmd,
      None => {
        _error!(inner, "Failed to take recent command off queue after write failure.");
        return Written::Ignore;
      },
    };

    _debug!(inner, "Error sending command {}: {:?}", command.kind.to_str_debug(), e);
    if command.should_send_write_error(inner) {
      command.respond_to_caller(Err(e.clone()));
      Written::Disconnect((writer.server.clone(), None, e))
    } else {
      inner.notifications.broadcast_error(e.clone());
      Written::Disconnect((writer.server.clone(), Some(command), e))
    }
  } else {
    Written::Sent((writer.server.clone(), should_flush))
  }
}

/// Check the shared connection command buffer to see if the oldest command blocks the multiplexer task on a
/// response (not pipelined).
pub fn check_blocked_multiplexer(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  let command = {
    let mut guard = buffer.lock();
    let should_pop = guard
      .front()
      .map(|command| command.has_multiplexer_channel())
      .unwrap_or(false);

    if should_pop {
      guard.pop_front().unwrap()
    } else {
      return;
    }
  };

  let tx = match command.take_multiplexer_tx() {
    Some(tx) => tx,
    None => return,
  };
  let error = error
    .clone()
    .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed"));

  if let Err(_) = tx.send(MultiplexerResponse::ConnectionClosed((error, command))) {
    _warn!(inner, "Failed to send multiplexer connection closed error.");
  }
}

/// Filter the shared buffer, removing commands that reached the max number of attempts and responding to each caller
/// with the underlying error.
pub fn check_final_write_attempt(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  let mut guard = buffer.lock();
  let commands = guard
    .drain(..)
    .filter_map(|mut command| {
      if command.has_multiplexer_channel() {
        // TODO double check if this should be `>`
        if command.attempted >= inner.max_command_attempts() {
          let error = error
            .clone()
            .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed"));
          command.respond_to_caller(Err(error));
          None
        } else {
          Some(command)
        }
      } else {
        Some(command)
      }
    })
    .collect();

  *guard = commands;
}

/// Check whether to drop the frame if it was sent in response to a pubsub command as a part of an unknown number of
/// response frames.
///
/// This is a special case for when PUNSUBSCRIBE or SUNSUBSCRIBE are called without arguments, which has the effect of
/// unsubscribing from every channel and sending one message per channel to the client in response.
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
            .map(|s| s == "punsubscribe" || s == "sunsubscribe")
            .unwrap_or(false))
          || (data[0]
            .as_str()
            .map(|s| s == "punsubscribe" || s == "sunsubscribe")
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
      RedisCommandKind::Punsubscribe | RedisCommandKind::Sunsubscribe => false,
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
    .map(|amt| Duration::from_millis(amt))
    .ok_or(RedisError::new(
      RedisErrorKind::Canceled,
      "Max reconnection attempts reached.",
    ))
}

/// Attempt to reconnect and replay queued commands.
pub async fn reconnect_once(inner: &Arc<RedisClientInner>, multiplexer: &mut Multiplexer) -> Result<(), RedisError> {
  client_utils::set_client_state(&inner.state, ClientState::Connecting);
  if let Err(e) = multiplexer.connect().await {
    _debug!(inner, "Failed reconnecting with error: {:?}", e);
    client_utils::set_client_state(&inner.state, ClientState::Disconnected);
    inner.notifications.broadcast_error(e.clone());
    Err(e)
  } else {
    // try to flush any previously in-flight commands
    multiplexer.retry_buffer().await;
    client_utils::set_client_state(&inner.state, ClientState::Connected);
    inner.notifications.broadcast_connect(Ok(()));
    inner.notifications.broadcast_reconnect();
    inner.reset_reconnection_attempts();
    Ok(())
  }
}

/// Reconnect to the server(s) until the max reconnect policy attempts are reached.
pub async fn reconnect_with_policy(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  let mut delay = utils::next_reconnection_delay(inner)?;

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      let _ = inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = reconnect_once(inner, multiplexer).await {
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
  multiplexer: &mut Multiplexer,
  kind: ClusterErrorKind,
  slot: u16,
  server: &Server,
) -> Result<(), RedisError> {
  let mut delay = inner.with_perf_config(|perf| Duration::from_millis(perf.cluster_cache_update_delay_ms as u64));

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      let _ = inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = multiplexer.cluster_redirection(&kind, slot, server).await {
      delay = next_reconnection_delay(inner).map_err(|_| e)?;

      continue;
    } else {
      break;
    }
  }

  Ok(())
}

/// Repeatedly try to send `ASKING` to the provided server, reconnecting as needed.
pub async fn send_asking_with_policy(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: &Server,
  slot: u16,
) -> Result<(), RedisError> {
  let mut delay = inner.with_perf_config(|perf| Duration::from_millis(perf.cluster_cache_update_delay_ms as u64));

  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      let _ = inner.wait_with_interrupt(delay).await?;
    }

    if !multiplexer.connections.has_server_connection(server) {
      if let Err(e) = multiplexer.sync_cluster().await {
        _debug!(inner, "Error syncing cluster before ASKING: {:?}", e);
        delay = utils::next_reconnection_delay(inner)?;
        continue;
      }
    }

    let mut command = RedisCommand::new_asking(slot);
    let (tx, rx) = oneshot_channel();
    command.skip_backpressure = true;
    command.response = ResponseKind::Respond(Some(tx));

    if let Err(error) = multiplexer.write_once(command, server).await {
      if error.should_not_reconnect() {
        break;
      } else {
        if let Err(_) = reconnect_once(inner, multiplexer).await {
          delay = utils::next_reconnection_delay(inner)?;
          continue;
        } else {
          delay = Duration::from_millis(0);
          continue;
        }
      }
    } else {
      match rx.await {
        Ok(Err(e)) => {
          // error writing the command
          _debug!(inner, "Reconnect once after error from ASKING: {:?}", e);
          if let Err(_) = reconnect_once(inner, multiplexer).await {
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
          if let Err(_) = reconnect_once(inner, multiplexer).await {
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
pub async fn sync_cluster_with_policy(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  let mut delay = inner.with_perf_config(|config| Duration::from_millis(config.cluster_cache_update_delay_ms as u64));

  // TODO GATs would make this looping a lot easier to express as a util function
  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      let _ = inner.wait_with_interrupt(delay).await?;
    }

    if let Err(e) = multiplexer.sync_cluster().await {
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
