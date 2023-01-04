use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{command::RedisCommand, types::Server, utils as protocol_utils},
  trace,
  types::{ClientState, KeyspaceEvent, Message, RedisKey, RedisValue},
  utils,
};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::{str, sync::Arc};

#[cfg(feature = "custom-reconnect-errors")]
use crate::globals::globals;
use crate::protocol::utils::pretty_error;

const KEYSPACE_PREFIX: &'static str = "__keyspace@";
const KEYEVENT_PREFIX: &'static str = "__keyevent@";

fn parse_keyspace_notification(channel: &str, message: &RedisValue) -> Option<KeyspaceEvent> {
  if channel.starts_with(KEYEVENT_PREFIX) {
    let parts: Vec<&str> = channel.split("@").collect();
    if parts.len() != 2 {
      return None;
    }

    let suffix: Vec<&str> = parts[1].split(":").collect();
    if suffix.len() != 2 {
      return None;
    }

    let db = match suffix[0].replace("__", "").parse::<u8>() {
      Ok(db) => db,
      Err(_) => return None,
    };
    let operation = suffix[1].to_owned();
    let key: RedisKey = match message.clone().try_into() {
      Ok(k) => k,
      Err(_) => return None,
    };

    Some(KeyspaceEvent { db, key, operation })
  } else if channel.starts_with(KEYSPACE_PREFIX) {
    let parts: Vec<&str> = channel.split("@").collect();
    if parts.len() != 2 {
      return None;
    }

    let suffix: Vec<&str> = parts[1].split(":").collect();
    if suffix.len() != 2 {
      return None;
    }

    let db = match suffix[0].replace("__", "").parse::<u8>() {
      Ok(db) => db,
      Err(_) => return None,
    };
    let key: RedisKey = suffix[1].to_owned().into();
    let operation = match message.as_string() {
      Some(k) => k,
      None => return None,
    };

    Some(KeyspaceEvent { db, key, operation })
  } else {
    None
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
  let resp3 = (data.len() == 3 || data.len() == 4)
    && data[0]
      .as_str()
      .map(|s| s == "message" || s == "pmessage" || s == "smessage")
      .unwrap_or(false);

  (false, resp3)
}

/// Try to parse the frame in either RESP2 or RESP3 pubsub formats.
fn parse_pubsub_message(frame: Resp3Frame, is_resp3: bool, is_resp2: bool) -> Result<Message, RedisError> {
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

  let span = trace::create_pubsub_span(inner, &frame);

  _trace!(inner, "Processing pubsub message.");
  let parsed_frame = if let Some(ref span) = span {
    let _enter = span.enter();
    parse_pubsub_message(frame, is_resp3_pubsub, is_resp2_pubsub)
  } else {
    parse_pubsub_message(frame, is_resp3_pubsub, is_resp2_pubsub)
  };

  let message = match parsed_frame {
    Ok(data) => data,
    Err(err) => {
      _warn!(inner, "Invalid message on pubsub interface: {:?}", err);
      return None;
    },
  };
  if let Some(ref span) = span {
    span.record("channel", &&*message.channel);
  }

  if let Some(event) = parse_keyspace_notification(&message.channel, &message.value) {
    inner.notifications.broadcast_keyspace(event);
  } else {
    inner.notifications.broadcast_pubsub(message);
  }

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
    match protocol_utils::frame_to_single_result(frame.clone()) {
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

#[cfg(feature = "custom-reconnect-errors")]
fn check_global_reconnect_errors(inner: &Arc<RedisClientInner>, frame: &Resp3Frame) -> Option<RedisError> {
  if let Resp3Frame::SimpleError { ref data, .. } = frame {
    for prefix in globals().reconnect_errors.read().iter() {
      if data.starts_with(prefix.to_str()) {
        _warn!(inner, "Found reconnection error: {}", data);
        let error = protocol_utils::pretty_error(data);
        inner.notifications.broadcast_error(error.clone());
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

fn is_clusterdown_error(frame: &Resp3Frame) -> Option<&str> {
  match frame {
    Resp3Frame::SimpleError { data, .. } => {
      if data.trim().starts_with("CLUSTERDOWN") {
        Some(&data)
      } else {
        None
      }
    },
    Resp3Frame::BlobError { data, .. } => {
      let parsed = match str::from_utf8(&data) {
        Ok(s) => s,
        Err(_) => return None,
      };

      if parsed.trim().starts_with("CLUSTERDOWN") {
        Some(parsed)
      } else {
        None
      }
    },
    _ => None,
  }
}

/// Check for special errors configured by the caller to initiate a reconnection process.
pub fn check_special_errors(inner: &Arc<RedisClientInner>, frame: &Resp3Frame) -> Option<RedisError> {
  if let Some(auth_error) = parse_redis_auth_error(frame) {
    return Some(auth_error);
  }
  if let Some(error) = is_clusterdown_error(frame) {
    return Some(pretty_error(error));
  }

  check_global_reconnect_errors(inner, frame)
}

/// Handle an error in the reader task that should end the connection.
pub fn handle_reader_error(inner: &Arc<RedisClientInner>, server: &Server, error: Option<RedisError>) {
  _debug!(inner, "Ending reader task from {} due to {:?}", server, error);

  if inner.should_reconnect() {
    inner.send_reconnect(Some(server.clone()), false, None);
  }
  if utils::read_locked(&inner.state) != ClientState::Disconnecting {
    inner
      .notifications
      .broadcast_error(error.unwrap_or(RedisError::new_canceled()));
  }
}
