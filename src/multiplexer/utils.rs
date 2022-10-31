use crate::{
  clients::RedisClient,
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  multiplexer::{responses, types::ClusterChange, utils, Backpressure, Connections, Counters, Multiplexer, Written},
  protocol::{
    command::{MultiplexerResponse, RedisCommand, RedisCommandKind},
    connection::{self, CommandBuffer, RedisReader, RedisTransport, RedisWriter, SharedBuffer},
    responders,
    types::*,
    utils as protocol_utils,
  },
  trace,
  types::*,
  utils as client_utils,
};
use arcstr::ArcStr;
use futures::{future::Either, pin_mut, select, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use log::Level;
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::{
  cmp,
  collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
  future::Future,
  mem,
  ops::DerefMut,
  str,
  sync::Arc,
  time::{Duration, Instant},
};
use tokio::{
  self,
  io::{AsyncRead, AsyncWrite},
  sync::{
    broadcast::{channel as broadcast_channel, Receiver as BroadcastReceiver},
    mpsc::UnboundedSender,
    oneshot::Sender as OneshotSender,
    RwLock as AsyncRwLock,
  },
};

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
  force_flush: bool,
) -> Written {
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

  if let Err(e) = writer.write_frame(frame, should_flush).await {
    _debug!(inner, "Error sending command {}: {:?}", command.kind.to_str_debug(), e);
    if command.should_send_write_error(inner) {
      command.respond_to_caller(Err(e.clone()));
      Written::Disconnect((server, None, e))
    } else {
      inner.notifications.broadcast_error(e.clone());
      Written::Disconnect((server, Some(command), e))
    }
  } else {
    writer.push_command(command);
    _trace!(inner, "Successfully sent command {}", command.kind.to_str_debug());
    Written::Sent((server, should_flush))
  }
}

/// Check the shared connection command buffer to see if the oldest command blocks the multiplexer task on a
/// response (not pipelined).
pub fn check_blocked_multiplexer(inner: &Arc<RedisClientInner>, buffer: &SharedBuffer, error: &Option<RedisError>) {
  let mut command = {
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

  let tx = match command.take_multiplexer_channel() {
    Some(tx) => tx,
    None => return,
  };
  let error = error
    .clone()
    .unwrap_or(RedisError::new(RedisErrorKind::IO, "Connection Closed"));

  tx.send(MultiplexerResponse::ConnectionClosed((error, command)));
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
        if command.attempts >= inner.max_command_attempts() {
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

/// Compare server identifiers of the form `<host>|<ip>:<port>` and `:<port>`, using `default_host` if a host/ip is
/// not provided.
// TODO unit test this
pub fn compare_servers(lhs: &str, rhs: &str, default_host: &str) -> bool {
  let lhs_parts: Vec<&str> = lhs.split(":").collect();
  let rhs_parts: Vec<&str> = rhs.split(":").collect();
  if lhs_parts.is_empty() || rhs_parts.is_empty() {
    error!("Invalid server identifier(s): {} == {}", lhs, rhs);
    return false;
  }

  if lhs_parts.len() == 2 && rhs_parts.len() == 2 {
    lhs == rhs
  } else if lhs_parts.len() == 2 && rhs_parts == 1 {
    lhs_parts[0] == default_host && lhs_parts[1] == rhs_parts[0]
  } else if lhs_parts.len() == 1 && rhs_parts.len() == 2 {
    rhs_parts[0] == default_host && rhs_parts[1] == lhs_parts[0]
  } else {
    lhs == rhs
  }
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

/// Run a function on an interval according to the reconnection policy.
pub async fn on_reconnect_interval<F, Fut, T, E>(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  initial_delay: Option<Duration>,
  func: F,
) -> Result<T, RedisError>
where
  E: Into<RedisError>,
  Fut: Future<Output = Result<T, E>>,
  F: FnMut(&Arc<RedisClientInner>, &mut Multiplexer) -> Fut,
{
  let mut delay = initial_delay.unwrap_or_else(|| next_reconnection_delay(inner)?);
  loop {
    if !delay.is_zero() {
      _debug!(inner, "Sleeping for {} ms.", delay.as_millis());
      let _ = inner.wait_with_interrupt(delay).await?;
    }

    match func(inner, multiplexer).await.map_err(|e| e.into()) {
      Ok(result) => {
        inner.reset_reconnection_attempts();
        return Ok(result);
      },
      Err(error) => {
        _warn!(inner, "Error reconnecting: {:?}", error);

        // return the underlying error on the last attempt
        delay = match next_reconnection_delay(inner) {
          Ok(delay) => delay,
          Err(_) => return Err(error),
        };
        continue;
      },
    };
  }
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
    if let Err(e) = multiplexer.retry_buffer().await {
      _debug!(inner, "Failed retrying command buffer: {:?}", e);
      client_utils::set_client_state(&inner.state, ClientState::Disconnected);
      inner.notifications.broadcast_error(e.clone());
      Err(e)
    } else {
      client_utils::set_client_state(&inner.state, ClientState::Connected);
      inner.notifications.broadcast_connect(Ok(()));
      inner.notifications.broadcast_reconnect();
      inner.reset_reconnection_attempts();
      Ok(())
    }
  }
}

/// Reconnect to the server(s) until the max reconnect policy attempts are reached.
pub async fn reconnect_with_policy(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  on_reconnect_interval(inner, multiplexer, None, |inner, multiplexer| async {
    reconnect_once(inner, multiplexer).await
  })
  .await
}

#[cfg(test)]
mod tests {}
