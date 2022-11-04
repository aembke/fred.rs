use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  multiplexer::{utils, Counters},
  protocol::{
    types::{ValueScanInner, ValueScanResult},
    utils as protocol_utils,
    utils::{frame_to_error, frame_to_single_result},
  },
  trace,
  types::{HScanResult, KeyspaceEvent, RedisKey, RedisValue, SScanResult, ScanResult, ZScanResult},
  utils as client_utils,
};
use arcstr::ArcStr;
use bytes_utils::Str;
use parking_lot::{Mutex, RwLock};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  sync::Arc,
};

#[cfg(feature = "custom-reconnect-errors")]
use crate::globals::globals;
#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
use crate::protocol::command::RedisCommand;
#[cfg(feature = "metrics")]
use std::cmp;
#[cfg(feature = "metrics")]
use std::time::Instant;

const KEYSPACE_PREFIX: &'static str = "__keyspace@";
const KEYEVENT_PREFIX: &'static str = "__keyevent@";

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

/// Check for the various pubsub formats for both RESP2 and RESP3.
fn check_pubsub_formats(frame: &Resp3Frame) -> (bool, bool) {
  if frame.is_pubsub_message() {
    return (true, false);
  }

  // otherwise check for RESP2 formats automatically converted to RESP3 by the codec
  let data = match frame {
    Resp3Frame::Array { ref data, .. } => data,
    Resp3Frame::Push { ref data, .. } => data,
    _ => return (false, false),
  };

  // RESP2 and RESP3 differ in that RESP3 contains an additional "pubsub" string frame at the start
  // so here we check the frame contents according to the RESP2 pubsub rules
  (
    false,
    (data.len() == 3 || data.len() == 4)
      && data[0]
        .as_str()
        .map(|s| s == "message" || s == "pmessage")
        .unwrap_or(false),
  )
}

/// Try to parse the frame in either RESP2 or RESP3 pubsub formats.
fn parse_pubsub_message(
  frame: Resp3Frame,
  is_resp3: bool,
  is_resp2: bool,
) -> Result<(String, RedisValue), RedisError> {
  if is_resp3 {
    protocol_utils::frame_to_pubsub(frame)
  } else if is_resp2 {
    // this is safe to do in limited circumstances like this since RESP2 and RESP3 pubsub arrays are similar enough
    protocol_utils::parse_as_resp2_pubsub(frame)
  } else {
    Err(RedisError::new(RedisErrorKind::Protocol, "Invalid pubsub message."))
  }
}

/// Check if the frame is part of a pubsub message, and if so route it to any listeners.
///
/// If not then return it to the caller for further processing.
pub fn check_pubsub_message(inner: &Arc<RedisClientInner>, frame: Resp3Frame) -> Option<Resp3Frame> {
  // in this case using resp3 frames can cause issues, since resp3 push commands are represented
  // differently than resp2 array frames. to fix this we convert back to resp2 here if needed.
  let (is_resp3_pubsub, is_resp2_pubsub) = check_pubsub_formats(&frame);
  if !is_resp3_pubsub && !is_resp2_pubsub {
    return Some(frame);
  }

  let span = if inner.should_trace() {
    let span = trace::create_pubsub_span(inner, &frame);
    Some(span)
  } else {
    None
  };

  _trace!(inner, "Processing pubsub message.");
  let parsed_frame = if let Some(ref span) = span {
    let _enter = span.enter();
    parse_pubsub_message(frame, is_resp3_pubsub, is_resp2_pubsub)
  } else {
    parse_pubsub_message(frame, is_resp3_pubsub, is_resp2_pubsub)
  };

  let (channel, message) = match parsed_frame {
    Ok(data) => data,
    Err(err) => {
      _warn!(inner, "Invalid message on pubsub interface: {:?}", err);
      return None;
    },
  };
  if let Some(ref span) = span {
    span.record("channel", &channel.as_str());
  }

  match parse_keyspace_notification(channel, message) {
    Ok(event) => inner.notifications.broadcast_keyspace(event),
    Err((channel, message)) => inner.notifications.broadcast_pubsub(channel, message),
  };

  None
}

pub async fn check_and_set_unblocked_flag(inner: &Arc<RedisClientInner>, command: &RedisCommand) {
  if command.blocks_connection() {
    inner.backchannel.write().await.set_unblocked();
  }
}

#[cfg(feature = "reconnect-on-auth-error")]
/// Parse the response frame to see if it's an auth error.
fn parse_redis_auth_error(frame: &Resp3Frame) -> Option<RedisError> {
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
fn parse_redis_auth_error(_frame: &Resp3Frame) -> Option<RedisError> {
  None
}

/// Whether or not the response is a QUEUED response to a command within a transaction.
fn response_is_queued(frame: &Resp3Frame) -> bool {
  match frame {
    Resp3Frame::SimpleString { ref data, .. } => data == "QUEUED",
    _ => false,
  }
}

#[cfg(feature = "custom-reconnect-errors")]
fn check_global_reconnect_errors(inner: &Arc<RedisClientInner>, frame: &Resp3Frame) -> Option<RedisError> {
  if let Resp3Frame::SimpleError { ref data, .. } = frame {
    for prefix in globals().reconnect_errors.read().iter() {
      if data.starts_with(prefix.to_str()) {
        _warn!(inner, "Found reconnection error: {}", data);
        let error = protocol_utils::pretty_error(data);
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
fn check_global_reconnect_errors(_: &Arc<RedisClientInner>, _: &Resp3Frame) -> Option<RedisError> {
  None
}

/// Check for special errors configured by the caller to initiate a reconnection process.
pub fn check_special_errors(inner: &Arc<RedisClientInner>, frame: &Resp3Frame) -> Option<RedisError> {
  if let Some(auth_error) = parse_redis_auth_error(frame) {
    return Some(auth_error);
  }

  check_global_reconnect_errors(inner, frame)
}

/// Handle an error in the reader task that should end the connection.
pub fn handle_reader_error(inner: &Arc<RedisClientInner>, server: &ArcStr, error: Option<RedisError>) {
  if inner.should_reconnect() {
    inner.send_reconnect(Some(server.clone()), false, None);
  }
  _debug!(inner, "Ending reader task from {} due to {:?}", server, error);
  inner
    .notifications
    .broadcast_error(error.unwrap_or(RedisError::new_canceled()));
}
