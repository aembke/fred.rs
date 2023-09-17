use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{command::RedisCommand, types::Server, utils as protocol_utils, utils::pretty_error},
  trace,
  types::{ClientState, KeyspaceEvent, Message, RedisKey, RedisValue},
  utils,
};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::{str, sync::Arc};

#[cfg(feature = "custom-reconnect-errors")]
use crate::globals::globals;
#[cfg(feature = "client-tracking")]
use crate::types::Invalidation;

const KEYSPACE_PREFIX: &'static str = "__keyspace@";
const KEYEVENT_PREFIX: &'static str = "__keyevent@";
#[cfg(feature = "client-tracking")]
const INVALIDATION_CHANNEL: &'static str = "__redis__:invalidate";

fn parse_keyspace_notification(channel: &str, message: &RedisValue) -> Option<KeyspaceEvent> {
  if channel.starts_with(KEYEVENT_PREFIX) {
    let parts: Vec<&str> = channel.splitn(2, "@").collect();
    if parts.len() < 2 {
      return None;
    }

    let suffix: Vec<&str> = parts[1].splitn(2, ":").collect();
    if suffix.len() < 2 {
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
    let parts: Vec<&str> = channel.splitn(2, "@").collect();
    if parts.len() < 2 {
      return None;
    }

    let suffix: Vec<&str> = parts[1].splitn(2, ":").collect();
    if suffix.len() < 2 {
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

fn check_message_prefix(s: &str) -> bool {
  s == "message" || s == "pmessage" || s == "smessage"
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
  let resp3 = (data.len() == 3 || data.len() == 4) && data[0].as_str().map(check_message_prefix).unwrap_or(false);

  (false, resp3)
}

/// Try to parse the frame in either RESP2 or RESP3 pubsub formats.
fn parse_pubsub_message(
  server: &Server,
  frame: Resp3Frame,
  is_resp3: bool,
  is_resp2: bool,
) -> Result<Message, RedisError> {
  if is_resp3 {
    protocol_utils::frame_to_pubsub(server, frame)
  } else if is_resp2 {
    protocol_utils::parse_as_resp2_pubsub(server, frame)
  } else {
    Err(RedisError::new(RedisErrorKind::Protocol, "Invalid pubsub message."))
  }
}

#[cfg(feature = "client-tracking")]
fn broadcast_pubsub_invalidation(inner: &Arc<RedisClientInner>, message: Message, server: &Server) {
  if let Some(invalidation) = Invalidation::from_message(message, server) {
    inner.notifications.broadcast_invalidation(invalidation);
  } else {
    _debug!(
      inner,
      "Dropping pubsub message on invalidation channel that cannot be parsed as an invalidation message."
    );
  }
}

#[cfg(not(feature = "client-tracking"))]
fn broadcast_pubsub_invalidation(_: &Arc<RedisClientInner>, _: Message, _: &Server) {}

#[cfg(feature = "client-tracking")]
fn is_pubsub_invalidation(message: &Message) -> bool {
  message.channel == INVALIDATION_CHANNEL
}

#[cfg(not(feature = "client-tracking"))]
fn is_pubsub_invalidation(_: &Message) -> bool {
  false
}

#[cfg(feature = "client-tracking")]
fn broadcast_resp3_invalidation(inner: &Arc<RedisClientInner>, server: &Server, frame: Resp3Frame) {
  if let Resp3Frame::Push { mut data, .. } = frame {
    if data.len() != 2 {
      return;
    }

    // RESP3 example: Push { data: [BlobString { data: b"invalidate", attributes: None }, Array { data:
    // [BlobString { data: b"foo", attributes: None }], attributes: None }], attributes: None }
    if let Resp3Frame::Array { data, .. } = data[1].take() {
      inner.notifications.broadcast_invalidation(Invalidation {
        keys:   data
          .into_iter()
          .filter_map(|f| f.as_bytes().map(|b| b.into()))
          .collect(),
        server: server.clone(),
      })
    }
  }
}

#[cfg(not(feature = "client-tracking"))]
fn broadcast_resp3_invalidation(_: &Arc<RedisClientInner>, _: &Server, _: Resp3Frame) {}

#[cfg(feature = "client-tracking")]
fn is_resp3_invalidation(frame: &Resp3Frame) -> bool {
  // RESP3 example: Push { data: [BlobString { data: b"invalidate", attributes: None }, Array { data:
  // [BlobString { data: b"foo", attributes: None }], attributes: None }], attributes: None }
  if let Resp3Frame::Push { ref data, .. } = frame {
    data
      .first()
      .and_then(|f| f.as_str())
      .map(|s| s == "invalidate")
      .unwrap_or(false)
  } else {
    false
  }
}

#[cfg(not(feature = "client-tracking"))]
fn is_resp3_invalidation(_: &Resp3Frame) -> bool {
  false
}

/// Check if the frame is part of a pubsub message, and if so route it to any listeners.
///
/// If not then return it to the caller for further processing.
pub fn check_pubsub_message(inner: &Arc<RedisClientInner>, server: &Server, frame: Resp3Frame) -> Option<Resp3Frame> {
  if is_resp3_invalidation(&frame) {
    broadcast_resp3_invalidation(inner, server, frame);
    return None;
  }

  let (is_resp3_pubsub, is_resp2_pubsub) = check_pubsub_formats(&frame);
  if !is_resp3_pubsub && !is_resp2_pubsub {
    return Some(frame);
  }

  let span = trace::create_pubsub_span(inner, &frame);
  _trace!(inner, "Processing pubsub message from {}.", server);
  let parsed_frame = if let Some(ref span) = span {
    let _enter = span.enter();
    parse_pubsub_message(server, frame, is_resp3_pubsub, is_resp2_pubsub)
  } else {
    parse_pubsub_message(server, frame, is_resp3_pubsub, is_resp2_pubsub)
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

  if is_pubsub_invalidation(&message) {
    broadcast_pubsub_invalidation(inner, message, server);
  } else {
    if let Some(event) = parse_keyspace_notification(&message.channel, &message.value) {
      inner.notifications.broadcast_keyspace(event);
    } else {
      inner.notifications.broadcast_pubsub(message);
    }
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
    match protocol_utils::frame_to_results(frame.clone()) {
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
pub fn broadcast_reader_error(inner: &Arc<RedisClientInner>, server: &Server, error: Option<RedisError>) {
  _warn!(inner, "Ending reader task from {} due to {:?}", server, error);

  if inner.should_reconnect() {
    inner.send_reconnect(Some(server.clone()), false, None);
  }
  if utils::read_locked(&inner.state) != ClientState::Disconnecting {
    inner
      .notifications
      .broadcast_error(error.unwrap_or(RedisError::new_canceled()));
  }
}

#[cfg(not(feature = "replicas"))]
pub fn broadcast_replica_error(inner: &Arc<RedisClientInner>, server: &Server, error: Option<RedisError>) {
  broadcast_reader_error(inner, server, error);
}

#[cfg(feature = "replicas")]
pub fn broadcast_replica_error(inner: &Arc<RedisClientInner>, server: &Server, error: Option<RedisError>) {
  _warn!(inner, "Ending replica reader task from {} due to {:?}", server, error);

  if inner.should_reconnect() {
    inner.send_replica_reconnect(server);
  }
  if utils::read_locked(&inner.state) != ClientState::Disconnecting {
    inner
      .notifications
      .broadcast_error(error.unwrap_or(RedisError::new_canceled()));
  }
}
