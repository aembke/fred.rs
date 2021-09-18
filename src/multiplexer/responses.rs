use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::utils;
use crate::multiplexer::{Counters, SentCommand, SentCommands};
use crate::protocol::types::RedisCommandKind;
use crate::protocol::types::{ResponseKind, ValueScanInner, ValueScanResult};
use crate::protocol::utils as protocol_utils;
use crate::protocol::utils::{frame_to_error, frame_to_single_result};
use crate::trace;
use crate::types::{HScanResult, KeyspaceEvent, RedisKey, RedisValue, SScanResult, ScanResult, ZScanResult};
use crate::utils as client_utils;
use parking_lot::RwLock;
use redis_protocol::resp2::types::Frame as ProtocolFrame;
use std::cmp;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock as AsyncRwLock;

#[cfg(feature = "custom-reconnect-errors")]
use crate::globals::globals;
#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;

const LAST_CURSOR: &'static str = "0";
const KEYSPACE_PREFIX: &'static str = "__keyspace@";
const KEYEVENT_PREFIX: &'static str = "__keyevent@";

#[derive(Clone, Debug, Eq, PartialEq)]
enum TransactionEnded {
  Exec,
  Discard,
}

#[cfg(feature = "metrics")]
fn sample_latency(latency_stats: &RwLock<MovingStats>, sent: Instant) {
  let dur = Instant::now().duration_since(sent);
  let dur_ms = cmp::max(0, (dur.as_secs() * 1000) + dur.subsec_millis() as u64) as i64;
  latency_stats.write().sample(dur_ms);
}

/// Sample overall and network latency values for a command.
#[cfg(feature = "metrics")]
fn sample_command_latencies(inner: &Arc<RedisClientInner>, command: &mut SentCommand) {
  if let Some(sent) = command.network_start.take() {
    sample_latency(&inner.network_latency_stats, sent);
  }
  sample_latency(&inner.latency_stats, command.command.sent);
}

#[cfg(not(feature = "metrics"))]
fn sample_command_latencies(_: &Arc<RedisClientInner>, _: &mut SentCommand) {}

/// Merge multiple potentially nested frames into one flat array of frames.
fn merge_multiple_frames(frames: &mut VecDeque<ProtocolFrame>) -> ProtocolFrame {
  let inner_len = frames.iter().fold(0, |count, frame| {
    count
      + match frame {
        ProtocolFrame::Array(ref inner) => inner.len(),
        _ => 1,
      }
  });

  let mut out = Vec::with_capacity(inner_len);

  for frame in frames.drain(..) {
    match frame {
      ProtocolFrame::Array(inner) => {
        for inner_frame in inner.into_iter() {
          out.push(inner_frame);
        }
      }
      _ => out.push(frame),
    };
  }

  ProtocolFrame::Array(out)
}

/// Update the SCAN cursor on a command, changing the internal cursor and the arguments array for the next call to SCAN.
fn update_scan_cursor(inner: &Arc<RedisClientInner>, last_command: &mut SentCommand, cursor: String) {
  if last_command.command.kind.is_scan() {
    last_command.command.args[0] = cursor.clone().into();
  } else if last_command.command.kind.is_value_scan() {
    last_command.command.args[1] = cursor.clone().into();
  }

  let old_cursor = match last_command.command.kind {
    RedisCommandKind::Scan(ref mut inner) => &mut inner.cursor,
    RedisCommandKind::Hscan(ref mut inner) => &mut inner.cursor,
    RedisCommandKind::Sscan(ref mut inner) => &mut inner.cursor,
    RedisCommandKind::Zscan(ref mut inner) => &mut inner.cursor,
    _ => {
      _warn!(inner, "Failed to update cursor. Invalid command kind.");
      return;
    }
  };

  *old_cursor = cursor;
}

/// Parse the output of a command that scans keys.
fn handle_key_scan_result(frame: ProtocolFrame) -> Result<(String, Vec<RedisKey>), RedisError> {
  if let ProtocolFrame::Array(mut frames) = frame {
    if frames.len() == 2 {
      let cursor = match frames[0].to_string() {
        Some(s) => s,
        None => {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            "Expected first SCAN result element to be a bulk string.",
          ))
        }
      };

      if let Some(ProtocolFrame::Array(results)) = frames.pop() {
        let mut keys = Vec::with_capacity(results.len());

        for frame in results.into_iter() {
          let key = match frame.to_string() {
            Some(s) => s,
            None => {
              return Err(RedisError::new(
                RedisErrorKind::ProtocolError,
                "Expected an array of strings from second SCAN result.",
              ))
            }
          };

          keys.push(RedisKey::new(key));
        }

        Ok((cursor, keys))
      } else {
        Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected second SCAN result element to be an array.",
        ))
      }
    } else {
      Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected two-element bulk string array from SCAN.",
      ))
    }
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected bulk string array from SCAN.",
    ))
  }
}

/// Parse the output of a command that scans values.
fn handle_value_scan_result(frame: ProtocolFrame) -> Result<(String, Vec<RedisValue>), RedisError> {
  if let ProtocolFrame::Array(mut frames) = frame {
    if frames.len() == 2 {
      let cursor = match frames[0].to_string() {
        Some(s) => s,
        None => {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            "Expected first result element to be a bulk string.",
          ))
        }
      };

      if let Some(ProtocolFrame::Array(results)) = frames.pop() {
        let mut values = Vec::with_capacity(results.len());

        for frame in results.into_iter() {
          values.push(frame_to_single_result(frame)?);
        }

        Ok((cursor, values))
      } else {
        Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected second result element to be an array.",
        ))
      }
    } else {
      Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected two-element bulk string array.",
      ))
    }
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected bulk string array.",
    ))
  }
}

/// Send the output to the caller of a command that scans keys.
fn send_key_scan_result(
  inner: &Arc<RedisClientInner>,
  last_command: SentCommand,
  result: Vec<RedisKey>,
  can_continue: bool,
) -> Result<(), RedisError> {
  if let RedisCommandKind::Scan(scan_state) = last_command.command.kind {
    let tx = scan_state.tx.clone();

    let scan_result = ScanResult {
      can_continue,
      inner: inner.clone(),
      scan_state,
      args: last_command.command.args,
      results: Some(result),
    };

    if let Err(_) = tx.send(Ok(scan_result)) {
      _warn!(inner, "Failed to send SCAN callback result.");
    }

    Ok(())
  } else {
    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Invalid redis command. Expected SCAN.",
    ))
  }
}

/// Send an error to the caller of a command that scans keys.
fn send_key_scan_error(
  inner: &Arc<RedisClientInner>,
  last_command: &SentCommand,
  e: RedisError,
) -> Result<(), RedisError> {
  if let RedisCommandKind::Scan(ref scan_state) = last_command.command.kind {
    if let Err(_) = scan_state.tx.send(Err(e)) {
      _warn!(inner, "Failed to send SCAN callback error.");
    }

    Ok(())
  } else {
    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Invalid redis command. Expected SCAN.",
    ))
  }
}

/// Send the output to the caller of a command that scans values.
fn send_value_scan_result(
  inner: &Arc<RedisClientInner>,
  last_command: SentCommand,
  result: Vec<RedisValue>,
  can_continue: bool,
) -> Result<(), RedisError> {
  let args = last_command.command.args;

  match last_command.command.kind {
    RedisCommandKind::Zscan(scan_state) => {
      let tx = scan_state.tx.clone();
      let results = ValueScanInner::transform_zscan_result(result)?;

      let state = ValueScanResult::ZScan(ZScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state,
        args,
        results: Some(results),
      });

      if let Err(_) = tx.send(Ok(state)) {
        _warn!(inner, "Failed to send ZSCAN result to caller");
      }
    }
    RedisCommandKind::Sscan(scan_state) => {
      let tx = scan_state.tx.clone();

      let state = ValueScanResult::SScan(SScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state,
        args,
        results: Some(result),
      });

      if let Err(_) = tx.send(Ok(state)) {
        _warn!(inner, "Failed to send SSCAN result to caller");
      }
    }
    RedisCommandKind::Hscan(scan_state) => {
      let tx = scan_state.tx.clone();
      let results = ValueScanInner::transform_hscan_result(result)?;

      let state = ValueScanResult::HScan(HScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state,
        args,
        results: Some(results),
      });

      if let Err(_) = tx.send(Ok(state)) {
        _warn!(inner, "Failed to send HSCAN result to caller");
      }
    }
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Invalid redis command. Expected HSCAN, SSCAN, or ZSCAN.",
      ))
    }
  };

  Ok(())
}

/// Send an error to the caller of a command that scans values.
fn send_value_scan_error(
  inner: &Arc<RedisClientInner>,
  last_command: &SentCommand,
  e: RedisError,
) -> Result<(), RedisError> {
  let scan_state = match last_command.command.kind {
    RedisCommandKind::Zscan(ref inner) => inner,
    RedisCommandKind::Sscan(ref inner) => inner,
    RedisCommandKind::Hscan(ref inner) => inner,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Invalid redis command. Expected HSCAN, SSCAN, or ZSCAN.",
      ))
    }
  };

  if let Err(_) = scan_state.tx.send(Err(e)) {
    _warn!(inner, "Failed to respond to caller with value scan result.");
  }

  Ok(())
}

/// Emit `(channel, message)` tuples on the pubsub interface, closing any senders for whom the receiver has been dropped.
fn emit_pubsub_message(inner: &Arc<RedisClientInner>, channel: String, message: RedisValue) {
  let mut to_remove = BTreeSet::new();

  // check for closed senders as we emit messages, and drop them at the end
  {
    for (idx, tx) in inner.message_tx.read().iter().enumerate() {
      if let Err(_) = tx.send((channel.clone(), message.clone())) {
        to_remove.insert(idx);
      }
    }
  }

  if !to_remove.is_empty() {
    _trace!(inner, "Removing {} closed pubsub listeners", to_remove.len());
    let mut message_tx_guard = inner.message_tx.write();
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

/// Emit keyspace events on the keyspace interface, closing any senders for whom the receiver has been dropped.
fn emit_keyspace_event(inner: &Arc<RedisClientInner>, event: KeyspaceEvent) {
  let mut to_remove = BTreeSet::new();

  // check for closed senders as we emit messages, and drop them at the end
  {
    for (idx, tx) in inner.keyspace_tx.read().iter().enumerate() {
      if let Err(_) = tx.send(event.clone()) {
        to_remove.insert(idx);
      }
    }
  }

  if !to_remove.is_empty() {
    _trace!(inner, "Removing {} closed keyspace listeners", to_remove.len());
    let mut message_tx_guard = inner.message_tx.write();
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

/// Respond to the caller with the output of the command.
fn respond_to_caller(inner: &Arc<RedisClientInner>, last_command: SentCommand, frame: ProtocolFrame) {
  _trace!(
    inner,
    "Responding to caller for {}",
    last_command.command.kind.to_str_debug()
  );

  if let Some(tx) = last_command.command.tx {
    if let Err(_) = tx.send(Ok(frame)) {
      _warn!(inner, "Failed to respond to caller.");
    }
  } else {
    _debug!(inner, "Skip writing response to caller for command without response.");
  }
}

/// Respond to the caller with an error.
fn respond_to_caller_error(inner: &Arc<RedisClientInner>, last_command: SentCommand, error: RedisError) {
  _trace!(
    inner,
    "Responding to caller with error for {}",
    last_command.command.kind.to_str_debug()
  );

  if let Some(tx) = last_command.command.tx {
    if let Err(_) = tx.send(Err(error)) {
      _warn!(inner, "Failed to respond to caller.");
    }
  } else {
    _debug!(
      inner,
      "Skip writing response error to caller for command without response."
    );
  }
}

/// Handle a frame that came from a command that was sent to all nodes in the cluster.
async fn handle_all_nodes_response(
  inner: &Arc<RedisClientInner>,
  last_command: SentCommand,
  frame: ProtocolFrame,
) -> Option<SentCommand> {
  if let Some(resp) = last_command.command.kind.all_nodes_response() {
    if frame.is_error() {
      check_command_resp_tx(inner, &last_command).await;

      if let Some(tx) = resp.take_tx() {
        let err = protocol_utils::frame_to_error(&frame).unwrap_or(RedisError::new_canceled());

        if let Err(_) = tx.send(Err(err)) {
          _warn!(inner, "Error sending all nodes response.");
        }
      } else {
        _warn!(inner, "Could not send error to all nodes response sender.");
      }
    } else {
      if resp.decr_num_nodes() == 0 {
        check_command_resp_tx(inner, &last_command).await;

        // take the final response sender off the command and write to that
        if let Some(tx) = resp.take_tx() {
          _trace!(inner, "Sending all nodes response after recv all responses.");
          if let Err(_) = tx.send(Ok(())) {
            _warn!(inner, "Error sending all nodes response.");
          }
        } else {
          _warn!(inner, "Could not send result to all nodes response sender.");
        }
      } else {
        _trace!(
          inner,
          "Waiting on {} more responses to all nodes command",
          resp.num_nodes()
        );
      }
    }
  } else {
    _warn!(inner, "Command with all nodes response has no callback sender.");
  }

  None
}

/// Process the frame in the context of the last (oldest) command sent.
///
/// If the last command has more expected responses it will be returned so it can be put back on the front of the response queue.
async fn process_response(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Counters,
  mut last_command: SentCommand,
  frame: ProtocolFrame,
) -> Result<Option<SentCommand>, RedisError> {
  _trace!(
    inner,
    "Processing response from {} to {} with frame kind {:?}",
    server,
    last_command.command.kind.to_str_debug(),
    frame.kind()
  );

  if last_command.command.kind.has_multiple_response_kind() {
    let frames = match last_command.command.kind.response_kind_mut() {
      Some(kind) => {
        if let ResponseKind::Multiple {
          ref count,
          ref mut buffer,
        } = kind
        {
          buffer.push_back(frame);

          if buffer.len() < *count {
            _trace!(
              inner,
              "Waiting for {} more frames for request with multiple responses.",
              count - buffer.len()
            );
            None
          } else {
            _trace!(inner, "Merging {} frames into one response.", buffer.len());
            Some(merge_multiple_frames(buffer))
          }
        } else {
          _warn!(inner, "Invalid command response kind. Expected multiple responses.");
          return Ok(None);
        }
      }
      None => {
        _warn!(
          inner,
          "Failed to read multiple response kind. Dropping response frame..."
        );
        return Ok(None);
      }
    };

    if let Some(frames) = frames {
      check_command_resp_tx(inner, &last_command).await;
      respond_to_caller(inner, last_command, frames);
    } else {
      // more responses are expected so return the last command to be put back in the queue
      return Ok(Some(last_command));
    }
  } else if last_command.command.kind.is_scan() {
    client_utils::decr_atomic(&counters.in_flight);

    let (next_cursor, keys) = match handle_key_scan_result(frame) {
      Ok(result) => result,
      Err(e) => {
        let _ = send_key_scan_error(inner, &last_command, e);
        check_command_resp_tx(inner, &last_command).await;
        return Ok(None);
      }
    };
    let should_stop = next_cursor.as_str() == LAST_CURSOR;
    update_scan_cursor(inner, &mut last_command, next_cursor);
    check_command_resp_tx(inner, &last_command).await;

    _trace!(inner, "Sending key scan result with {} keys", keys.len());
    if let Err(_) = send_key_scan_result(inner, last_command, keys, !should_stop) {
      _warn!(inner, "Failed to send key scan result");
    }
  } else if last_command.command.kind.is_value_scan() {
    client_utils::decr_atomic(&counters.in_flight);

    let (next_cursor, values) = match handle_value_scan_result(frame) {
      Ok(result) => result,
      Err(e) => {
        let _ = send_value_scan_error(inner, &last_command, e);
        check_command_resp_tx(inner, &last_command).await;
        return Ok(None);
      }
    };
    let should_stop = next_cursor.as_str() == LAST_CURSOR;
    update_scan_cursor(inner, &mut last_command, next_cursor);
    check_command_resp_tx(inner, &last_command).await;

    _trace!(inner, "Sending value scan result with {} values", values.len());
    if let Err(_) = send_value_scan_result(inner, last_command, values, !should_stop) {
      _warn!(inner, "Failed to send value scan result");
    }
  } else if last_command.command.kind.is_all_cluster_nodes() {
    return Ok(handle_all_nodes_response(inner, last_command, frame).await);
  } else {
    client_utils::decr_atomic(&counters.in_flight);
    sample_command_latencies(inner, &mut last_command);

    check_command_resp_tx(inner, &last_command).await;
    respond_to_caller(inner, last_command, frame);
  }

  Ok(None)
}

fn parse_keyspace_notification(channel: String, message: RedisValue) -> Result<KeyspaceEvent, (String, RedisValue)> {
  if channel.starts_with(KEYEVENT_PREFIX) {
    let parts: Vec<&str> = channel.split("@").collect();
    if parts.len() != 2 {
      return Err((channel, message));
    }

    let suffix: Vec<&str> = parts[1].split(":").collect();
    if suffix.len() != 2 {
      return Err((channel, message));
    }

    let db = match suffix[0].replace("__", "").parse::<u8>() {
      Ok(db) => db,
      Err(_) => return Err((channel, message)),
    };
    let operation = suffix[1].to_owned();
    let key = match message.as_string() {
      Some(k) => k,
      None => return Err((channel, message)),
    };

    Ok(KeyspaceEvent { db, key, operation })
  } else if channel.starts_with(KEYSPACE_PREFIX) {
    let parts: Vec<&str> = channel.split("@").collect();
    if parts.len() != 2 {
      return Err((channel, message));
    }

    let suffix: Vec<&str> = parts[1].split(":").collect();
    if suffix.len() != 2 {
      return Err((channel, message));
    }

    let db = match suffix[0].replace("__", "").parse::<u8>() {
      Ok(db) => db,
      Err(_) => return Err((channel, message)),
    };
    let key = suffix[1].to_owned();
    let operation = match message.as_string() {
      Some(k) => k,
      None => return Err((channel, message)),
    };

    Ok(KeyspaceEvent { db, key, operation })
  } else {
    Err((channel, message))
  }
}

/// Check if the frame is part of a pubsub message, and if so route it to any listeners.
///
/// If not then return it to the caller for further processing.
fn check_pubsub_message(inner: &Arc<RedisClientInner>, frame: ProtocolFrame) -> Option<ProtocolFrame> {
  if frame.is_pubsub_message() {
    let span = if inner.should_trace() {
      let span = trace::create_pubsub_span(inner, &frame);
      Some(span)
    } else {
      None
    };

    _trace!(inner, "Processing pubsub message.");
    let parsed_frame = if let Some(ref span) = span {
      let _enter = span.enter();
      protocol_utils::frame_to_pubsub(frame)
    } else {
      protocol_utils::frame_to_pubsub(frame)
    };

    let (channel, message) = match parsed_frame {
      Ok(data) => data,
      Err(err) => {
        _warn!(inner, "Invalid message on pubsub interface: {:?}", err);
        return None;
      }
    };
    if let Some(ref span) = span {
      span.record("channel", &channel.as_str());
    }

    match parse_keyspace_notification(channel, message) {
      Ok(event) => emit_keyspace_event(inner, event),
      Err((channel, message)) => emit_pubsub_message(inner, channel, message),
    };

    None
  } else {
    Some(frame)
  }
}

#[cfg(feature = "reconnect-on-auth-error")]
/// Parse the response frame to see if it's an auth error.
fn parse_redis_auth_error(frame: &ProtocolFrame) -> Option<RedisError> {
  if frame.is_error() {
    match frame_to_single_result(frame.clone()) {
      Ok(_) => None,
      Err(e) => match e.kind() {
        RedisErrorKind::Auth => Some(e),
        _ => None,
      },
    }
  } else {
    None
  }
}

#[cfg(not(feature = "reconnect-on-auth-error"))]
/// Parse the response frame to see if it's an auth error.
fn parse_redis_auth_error(_frame: &ProtocolFrame) -> Option<RedisError> {
  None
}

/// Read the last (oldest) command from the command queue.
async fn last_cluster_command(
  inner: &Arc<RedisClientInner>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
  server: &Arc<String>,
) -> Result<Option<SentCommand>, RedisError> {
  let last_command = match commands.write().await.get_mut(server) {
    Some(commands) => match commands.pop_front() {
      Some(cmd) => cmd,
      None => {
        _warn!(inner, "Recv response without a corresponding command from {}", server);
        return Ok(None);
      }
    },
    None => {
      _error!(inner, "Couldn't find command queue for server {}", server);
      return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command queue."));
    }
  };

  Ok(Some(last_command))
}

/// Push the last command back on the command queue.
async fn add_back_last_cluster_command(
  inner: &Arc<RedisClientInner>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
  server: &Arc<String>,
  command: SentCommand,
) -> Result<(), RedisError> {
  match commands.write().await.get_mut(server) {
    Some(commands) => commands.push_front(command),
    None => {
      _error!(inner, "Couldn't find command queue for server {}", server);
      return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command queue."));
    }
  };

  Ok(())
}

/// Check if the command has a response sender to unblock the multiplexer loop, and if send a message on that channel.
async fn check_command_resp_tx(inner: &Arc<RedisClientInner>, command: &SentCommand) {
  _trace!(inner, "Writing to multiplexer sender to unblock command loop.");
  if command.command.kind.is_blocking() {
    inner.backchannel.write().await.set_unblocked();
  }

  if let Some(tx) = command.command.take_resp_tx() {
    if let Err(e) = tx.send(()) {
      _warn!(inner, "Error sending cmd loop response: {:?}", e);
    }
  }
}

/// Whether or not the most recent command ends a transaction.
async fn last_centralized_command_ends_transaction(
  commands: &Arc<AsyncRwLock<SentCommands>>,
) -> Option<TransactionEnded> {
  commands.read().await.back().and_then(|c| {
    if c.command.kind.is_exec() {
      Some(TransactionEnded::Exec)
    } else if c.command.kind.is_discard() {
      Some(TransactionEnded::Discard)
    } else {
      None
    }
  })
}

/// Whether or not the most recent command ends a transaction.
async fn last_clustered_command_ends_transaction(
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
  server: &Arc<String>,
) -> Option<TransactionEnded> {
  commands.read().await.get(server).and_then(|commands| {
    commands.back().and_then(|c| {
      if c.command.kind.is_exec() {
        Some(TransactionEnded::Exec)
      } else if c.command.kind.is_discard() {
        Some(TransactionEnded::Discard)
      } else {
        None
      }
    })
  })
}

/// Whether or not the response is a QUEUED response to a command within a transaction.
fn response_is_queued(frame: &ProtocolFrame) -> bool {
  match frame {
    ProtocolFrame::SimpleString(ref s) => s == "QUEUED",
    _ => false,
  }
}

/// Read the most recent (newest) command from a centralized command response queue.
async fn take_most_recent_centralized_command(commands: &Arc<AsyncRwLock<SentCommands>>) -> Option<SentCommand> {
  commands.write().await.pop_back()
}

/// Read the most recent (newest) command from a clustered command queue.
async fn take_most_recent_cluster_command(
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
  server: &Arc<String>,
) -> Option<SentCommand> {
  commands
    .write()
    .await
    .get_mut(server)
    .and_then(|commands| commands.pop_back())
}

/// Send a `Canceled` error to all commands in a centralized command response queue.
async fn cancel_centralized_multi_commands(inner: &Arc<RedisClientInner>, commands: &Arc<AsyncRwLock<SentCommands>>) {
  for command in commands.write().await.drain(..) {
    check_command_resp_tx(inner, &command).await;
    respond_to_caller_error(inner, command, RedisError::new_canceled());
  }
}

/// Send a `Canceled` error to all commands in a clustered command queue.
async fn cancel_clustered_multi_commands(
  inner: &Arc<RedisClientInner>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
  server: &Arc<String>,
) {
  if let Some(commands) = commands.write().await.get_mut(server) {
    for command in commands.drain(..) {
      check_command_resp_tx(inner, &command).await;
      respond_to_caller_error(inner, command, RedisError::new_canceled())
    }
  }
}

/// End a transaction on a centralized client instance.
async fn end_centralized_multi_block(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  commands: &Arc<AsyncRwLock<SentCommands>>,
  frame: ProtocolFrame,
  ending_cmd: TransactionEnded,
) -> Result<(), RedisError> {
  if !client_utils::is_locked_some(&inner.multi_block) {
    return Err(RedisError::new(
      RedisErrorKind::InvalidCommand,
      "Expected MULTI block policy.",
    ));
  }
  counters.decr_in_flight();

  if ending_cmd == TransactionEnded::Discard || (ending_cmd == TransactionEnded::Exec && frame.is_null()) {
    // the transaction was discarded or aborted due to a WATCH condition failing
    _trace!(inner, "Ending transaction with discard or null response");
    let recent_cmd = take_most_recent_centralized_command(commands).await;
    cancel_centralized_multi_commands(inner, commands).await;
    let _ = client_utils::take_locked(&inner.multi_block);

    if let Some(mut recent_cmd) = recent_cmd {
      sample_command_latencies(inner, &mut recent_cmd);
      check_command_resp_tx(inner, &recent_cmd).await;
      respond_to_caller(inner, recent_cmd, frame);
      return Ok(());
    } else {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Missing most recent command.",
      ));
    }
  }

  // return the frame to the caller directly, let them sort out the results
  _trace!(inner, "Returning exec result to the caller directly");
  let mut last_command = match commands.write().await.pop_front() {
    Some(cmd) => cmd,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Missing last command from EXEC or DISCARD.",
      ))
    }
  };
  if !last_command.command.kind.ends_transaction() {
    return Err(RedisError::new(
      RedisErrorKind::InvalidCommand,
      "Expected EXEC or DISCARD command.",
    ));
  }

  sample_command_latencies(inner, &mut last_command);
  check_command_resp_tx(inner, &last_command).await;
  respond_to_caller(inner, last_command, frame);

  let _ = client_utils::take_locked(&inner.multi_block);
  Ok(())
}

/// End a transaction on a clustered client instance.
async fn end_clustered_multi_block(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
  frame: ProtocolFrame,
  ending_cmd: TransactionEnded,
) -> Result<(), RedisError> {
  if !client_utils::is_locked_some(&inner.multi_block) {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected MULTI block policy.",
    ));
  }
  if let Some(counters) = counters.read().get(server) {
    counters.decr_in_flight();
  }

  if ending_cmd == TransactionEnded::Discard || (ending_cmd == TransactionEnded::Exec && frame.is_null()) {
    // the transaction was discarded or aborted due to a WATCH condition failing
    _trace!(inner, "Ending transaction with discard or null response.");
    let recent_cmd = take_most_recent_cluster_command(commands, server).await;
    cancel_clustered_multi_commands(inner, commands, server).await;
    let _ = client_utils::take_locked(&inner.multi_block);

    if let Some(mut recent_cmd) = recent_cmd {
      sample_command_latencies(inner, &mut recent_cmd);
      check_command_resp_tx(inner, &recent_cmd).await;
      respond_to_caller(inner, recent_cmd, frame);
      return Ok(());
    } else {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Missing most recent clustered command.",
      ));
    }
  }

  // return the frame to the caller directly, let them sort out the results
  _trace!(inner, "Returning exec result to the caller directly.");
  let mut last_command = match last_cluster_command(inner, commands, server).await? {
    Some(cmd) => cmd,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Missing last command from EXEC or DISCARD.",
      ))
    }
  };
  if !last_command.command.kind.ends_transaction() {
    return Err(RedisError::new(
      RedisErrorKind::InvalidCommand,
      "Expected EXEC or DISCARD command.",
    ));
  }

  sample_command_latencies(inner, &mut last_command);
  check_command_resp_tx(inner, &last_command).await;
  respond_to_caller(inner, last_command, frame);

  let _ = client_utils::take_locked(&inner.multi_block);
  Ok(())
}

/// Handle a QUEUED response to a command on a clustered client instance.
async fn handle_clustered_queued_response(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
  frame: ProtocolFrame,
) -> Result<(), RedisError> {
  let multi_block = match client_utils::read_locked(&inner.multi_block) {
    Some(blk) => blk,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected MULTI block policy with QUEUED response.",
      ))
    }
  };
  if let Some(counters) = counters.read().get(server) {
    counters.decr_in_flight();
  }

  // read the last command and respond with the QUEUED result
  _trace!(inner, "Handle QUEUED response for transaction.");
  let mut last_command = match last_cluster_command(inner, commands, server).await? {
    Some(cmd) => cmd,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Could not find last request.",
      ))
    }
  };

  if frame.is_error() && multi_block.abort_on_error {
    let _ = client_utils::take_locked(&inner.multi_block);
  }

  sample_command_latencies(inner, &mut last_command);
  check_command_resp_tx(inner, &last_command).await;
  respond_to_caller(inner, last_command, frame);

  Ok(())
}

/// Handle a QUEUED response to a command from a centralized client.
async fn handle_centralized_queued_response(
  inner: &Arc<RedisClientInner>,
  counters: &Counters,
  commands: &Arc<AsyncRwLock<SentCommands>>,
  frame: ProtocolFrame,
) -> Result<(), RedisError> {
  let multi_block = match client_utils::read_locked(&inner.multi_block) {
    Some(blk) => blk,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected MULTI block policy with QUEUED response.",
      ))
    }
  };
  counters.decr_in_flight();

  // read the last command and respond with the QUEUED result
  _trace!(inner, "Handle QUEUED response for transaction.");
  let mut last_command = match commands.write().await.pop_front() {
    Some(cmd) => cmd,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Could not find last request.",
      ))
    }
  };

  if frame.is_error() && multi_block.abort_on_error {
    let _ = client_utils::take_locked(&inner.multi_block);
  }

  sample_command_latencies(inner, &mut last_command);
  check_command_resp_tx(inner, &last_command).await;
  respond_to_caller(inner, last_command, frame);

  Ok(())
}

/// Check if the frame represents a MOVED or ASK error.
fn check_redirection_error(inner: &Arc<RedisClientInner>, frame: &ProtocolFrame) -> Option<RedisError> {
  if frame.is_moved_or_ask_error() {
    let error = frame_to_error(frame).unwrap_or(RedisError::new(RedisErrorKind::Cluster, "MOVED or ASK error."));
    utils::emit_error(&inner, &error);
    _debug!(inner, "Recv moved or ask error: {:?}", error);
    Some(error)
  } else {
    None
  }
}

#[cfg(feature = "custom-reconnect-errors")]
fn check_global_reconnect_errors(inner: &Arc<RedisClientInner>, frame: &ProtocolFrame) -> Option<RedisError> {
  if let ProtocolFrame::Error(ref message) = frame {
    for prefix in globals().reconnect_errors.read().iter() {
      if message.starts_with(prefix.to_str()) {
        _warn!(inner, "Found reconnection error: {}", message);
        let error = protocol_utils::pretty_error(message);
        utils::emit_error(inner, &error);
        return Some(error);
      }
    }

    None
  } else {
    None
  }
}

#[cfg(not(feature = "custom-reconnect-errors"))]
fn check_global_reconnect_errors(_: &Arc<RedisClientInner>, _: &ProtocolFrame) -> Option<RedisError> {
  None
}

/// Check for special errors configured by the caller to initiate a reconnection process.
fn check_special_errors(inner: &Arc<RedisClientInner>, frame: &ProtocolFrame) -> Option<RedisError> {
  if let Some(auth_error) = parse_redis_auth_error(frame) {
    // this closes the stream and initiates a reconnect, if applicable
    return Some(auth_error);
  }

  check_global_reconnect_errors(inner, frame)
}

/// Refresh the cluster state and retry the last command.
async fn handle_redirection_error(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, SentCommands>>>,
  error: RedisError,
) -> Result<(), RedisError> {
  let last_command = last_cluster_command(inner, commands, server).await?;

  if let Some(command) = last_command {
    utils::refresh_cluster_state(inner, command, error);
  }
  Ok(())
}

/// Process a frame on a clustered client instance from the provided server.
///
/// Errors in this context are considered fatal and will close the stream.
pub async fn process_clustered_frame(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Arc<RwLock<BTreeMap<Arc<String>, Counters>>>,
  commands: &Arc<AsyncRwLock<BTreeMap<Arc<String>, VecDeque<SentCommand>>>>,
  frame: ProtocolFrame,
) -> Result<(), RedisError> {
  if let Some(error) = check_redirection_error(inner, &frame) {
    handle_redirection_error(inner, server, commands, error).await?;
    return Ok(());
  }
  if let Some(error) = check_special_errors(inner, &frame) {
    // this closes the stream and initiates a reconnect, if configured
    return Err(error);
  }

  if let Some(frame) = check_pubsub_message(inner, frame) {
    if response_is_queued(&frame) {
      let _ = handle_clustered_queued_response(inner, server, counters, commands, frame).await?;
      return Ok(());
    }

    if let Some(trx_ended) = last_clustered_command_ends_transaction(commands, server).await {
      end_clustered_multi_block(inner, server, counters, commands, frame, trx_ended).await
    } else {
      let counters = match counters.read().get(server) {
        Some(counters) => counters.clone(),
        None => {
          _error!(inner, "Couldn't find counters for server {}", server);
          return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command counters."));
        }
      };
      let last_command = match last_cluster_command(inner, commands, server).await? {
        Some(cmd) => cmd,
        None => {
          _error!(
            inner,
            "Missing last command for {:?} frame from {}",
            frame.kind(),
            server
          );
          return Ok(());
        }
      };

      if let Some(last_command) = process_response(inner, server, &counters, last_command, frame).await? {
        add_back_last_cluster_command(inner, commands, server, last_command).await?;
      }
      Ok(())
    }
  } else {
    Ok(())
  }
}

/// Process a frame on a centralized client instance.
///
/// Errors in this context are considered fatal and will close the stream.
pub async fn process_centralized_frame(
  inner: &Arc<RedisClientInner>,
  server: &Arc<String>,
  counters: &Counters,
  commands: &Arc<AsyncRwLock<SentCommands>>,
  frame: ProtocolFrame,
) -> Result<(), RedisError> {
  if let Some(error) = check_special_errors(inner, &frame) {
    // this closes the stream and initiates a reconnect, if configured
    return Err(error);
  }

  if let Some(frame) = check_pubsub_message(inner, frame) {
    if response_is_queued(&frame) {
      let _ = handle_centralized_queued_response(inner, counters, commands, frame).await?;
      return Ok(());
    }

    if let Some(trx_ended) = last_centralized_command_ends_transaction(commands).await {
      end_centralized_multi_block(inner, counters, commands, frame, trx_ended).await
    } else {
      let last_command = match commands.write().await.pop_front() {
        Some(cmd) => cmd,
        None => {
          _error!(
            inner,
            "Missing last command for {:?} frame from {}",
            frame.kind(),
            server
          );
          return Ok(());
        }
      };

      if let Some(last_command) = process_response(inner, server, counters, last_command, frame).await? {
        commands.write().await.push_front(last_command);
      }

      Ok(())
    }
  } else {
    Ok(())
  }
}
