use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RedisCommandKind},
    connection::OK,
    types::{ProtocolFrame, *},
  },
  types::*,
  utils,
  utils::redis_string_to_f64,
};
use bytes::Bytes;
use bytes_utils::Str;
use redis_protocol::{
  resp2::types::Frame as Resp2Frame,
  resp3::types::{Auth, Frame as Resp3Frame, FrameMap, PUBSUB_PUSH_PREFIX},
};
use semver::Version;
use std::{borrow::Cow, collections::HashMap, convert::TryInto, ops::Deref, str, sync::Arc};

pub fn initial_buffer_size(inner: &Arc<RedisClientInner>) -> usize {
  if inner.performance.load().as_ref().auto_pipeline {
    // TODO make this configurable
    256
  } else {
    1
  }
}

/// Read the major redis version, assuming version 6 if a version is not provided.
#[allow(dead_code)]
pub fn major_redis_version(version: &Option<Version>) -> u8 {
  version.as_ref().map(|v| v.major as u8).unwrap_or(6)
}

pub fn parse_cluster_error(data: &str) -> Result<(ClusterErrorKind, u16, String), RedisError> {
  let parts: Vec<&str> = data.split(" ").collect();
  if parts.len() == 3 {
    let kind: ClusterErrorKind = parts[0].try_into()?;
    let slot: u16 = parts[1].parse()?;
    let server = parts[2].to_string();

    Ok((kind, slot, server))
  } else {
    Err(RedisError::new(RedisErrorKind::Protocol, "Expected cluster error."))
  }
}

pub fn queued_frame() -> Resp3Frame {
  Resp3Frame::SimpleString {
    data:       utils::static_bytes(QUEUED.as_bytes()),
    attributes: None,
  }
}

pub fn frame_is_queued(frame: &Resp3Frame) -> bool {
  match frame {
    Resp3Frame::SimpleString { ref data, .. } | Resp3Frame::BlobString { ref data, .. } => {
      str::from_utf8(data).ok().map(|s| s == QUEUED).unwrap_or(false)
    },
    _ => false,
  }
}

pub fn is_ok(frame: &Resp3Frame) -> bool {
  match frame {
    Resp3Frame::SimpleString { ref data, .. } => data == OK,
    _ => false,
  }
}

/// Whether the provided frame is null.
pub fn is_null(frame: &Resp3Frame) -> bool {
  match frame {
    Resp3Frame::Null => true,
    _ => false,
  }
}

pub fn server_to_parts(server: &str) -> Result<(&str, u16), RedisError> {
  let parts: Vec<&str> = server.split(":").collect();
  if parts.len() < 2 {
    return Err(RedisError::new(RedisErrorKind::IO, "Invalid server."));
  }
  Ok((parts[0], parts[1].parse::<u16>()?))
}

pub fn binary_search(slots: &Vec<SlotRange>, slot: u16) -> Option<usize> {
  if slot > REDIS_CLUSTER_SLOTS {
    return None;
  }

  let (mut low, mut high) = (0, slots.len() - 1);
  while low <= high {
    let mid = (low + high) / 2;

    let curr = match slots.get(mid) {
      Some(slot) => slot,
      None => {
        warn!("Failed to find slot range at index {} for hash slot {}", mid, slot);
        return None;
      },
    };

    if slot < curr.start {
      high = mid - 1;
    } else if slot > curr.end {
      low = mid + 1;
    } else {
      return Some(mid);
    }
  }

  None
}

pub fn pretty_error(resp: &str) -> RedisError {
  let kind = {
    let mut parts = resp.split_whitespace();

    match parts.next().unwrap_or("").as_ref() {
      "" => RedisErrorKind::Unknown,
      "ERR" => RedisErrorKind::Unknown,
      "WRONGTYPE" => RedisErrorKind::InvalidArgument,
      "NOAUTH" | "WRONGPASS" => RedisErrorKind::Auth,
      "MOVED" | "ASK" | "CLUSTERDOWN" => RedisErrorKind::Cluster,
      "Invalid" => match parts.next().unwrap_or("").as_ref() {
        "argument(s)" | "Argument" => RedisErrorKind::InvalidArgument,
        "command" | "Command" => RedisErrorKind::InvalidCommand,
        _ => RedisErrorKind::Unknown,
      },
      _ => RedisErrorKind::Unknown,
    }
  };

  let details = if resp.is_empty() {
    Cow::Borrowed("No response!")
  } else {
    Cow::Owned(resp.to_owned())
  };
  RedisError::new(kind, details)
}

/// Parse the frame as a string, without support for error frames.
pub fn frame_into_string(frame: Resp3Frame) -> Result<String, RedisError> {
  match frame {
    Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::Double { data, .. } => Ok(data.to_string()),
    Resp3Frame::Number { data, .. } => Ok(data.to_string()),
    Resp3Frame::Boolean { data, .. } => Ok(data.to_string()),
    Resp3Frame::VerbatimString { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::BigNumber { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected protocol string.")),
  }
}

/// Parse the frame from a shard pubsub channel.
pub fn parse_shard_pubsub_frame(server: &Server, frame: &Resp3Frame) -> Option<Message> {
  let value = match frame {
    Resp3Frame::Array { ref data, .. } | Resp3Frame::Push { ref data, .. } => {
      if data.len() >= 3 && data.len() <= 5 {
        // check both resp2 and resp3 formats
        let has_either_prefix = (data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == "smessage").unwrap_or(false))
          || (data[0].as_str().map(|s| s == "smessage").unwrap_or(false));

        if has_either_prefix {
          let channel = match frame_to_str(&data[data.len() - 2]) {
            Some(channel) => channel,
            None => return None,
          };
          let message = match frame_to_results(data[data.len() - 1].clone()) {
            Ok(message) => message,
            Err(_) => return None,
          };

          Some((channel, message))
        } else {
          None
        }
      } else {
        None
      }
    },
    _ => None,
  };

  value.map(|(channel, value)| Message {
    channel,
    value,
    kind: MessageKind::SMessage,
    server: server.clone(),
  })
}

/// Parse the kind of pubsub message (pattern, sharded, or regular).
pub fn parse_message_kind(frame: &Resp3Frame) -> Result<MessageKind, RedisError> {
  let frames = match frame {
    Resp3Frame::Array { ref data, .. } => data,
    Resp3Frame::Push { ref data, .. } => data,
    _ => return Err(RedisError::new(RedisErrorKind::Protocol, "Invalid pubsub frame type.")),
  };

  let parsed = if frames.len() == 3 {
    // resp2 format, normal message
    frames[0].as_str().and_then(MessageKind::from_str)
  } else if frames.len() == 4 {
    // resp3 normal message or resp2 pattern/shard message
    frames[1]
      .as_str()
      .and_then(MessageKind::from_str)
      .or(frames[0].as_str().and_then(MessageKind::from_str))
  } else if frames.len() == 5 {
    // resp3 pattern or shard message
    frames[1]
      .as_str()
      .and_then(MessageKind::from_str)
      .or(frames[2].as_str().and_then(MessageKind::from_str))
  } else {
    None
  };

  parsed.ok_or(RedisError::new(
    RedisErrorKind::Protocol,
    "Invalid pubsub message kind.",
  ))
}

/// Parse the channel and value fields from a pubsub frame.
pub fn parse_message_fields(frame: &Resp3Frame) -> Result<(Str, RedisValue), RedisError> {
  let mut frames = match frame.clone() {
    Resp3Frame::Array { data, .. } => data,
    Resp3Frame::Push { data, .. } => data,
    _ => return Err(RedisError::new(RedisErrorKind::Protocol, "Invalid pubsub frame type.")),
  };

  let value = frames
    .pop()
    .ok_or(RedisError::new(RedisErrorKind::Protocol, "Invalid pubsub message."))?;
  let channel = frames
    .pop()
    .ok_or(RedisError::new(RedisErrorKind::Protocol, "Invalid pubsub channel."))?;
  let channel =
    frame_to_str(&channel).ok_or(RedisError::new(RedisErrorKind::Protocol, "Failed to parse channel."))?;
  let value = frame_to_results(value)?;

  Ok((channel, value))
}

/// Parse the frame as a pubsub message.
pub fn frame_to_pubsub(server: &Server, frame: Resp3Frame) -> Result<Message, RedisError> {
  if let Some(message) = parse_shard_pubsub_frame(server, &frame) {
    return Ok(message);
  }

  let kind = parse_message_kind(&frame)?;
  let (channel, value) = parse_message_fields(&frame)?;
  Ok(Message {
    kind,
    channel,
    value,
    server: server.clone(),
  })
}

/// Attempt to parse a RESP3 frame as a pubsub message in the RESP2 format.
///
/// This can be useful in cases where the codec layer automatically upgrades to RESP3,
/// but the contents of the pubsub message still use the RESP2 format.
// TODO move and redo this in redis_protocol
pub fn parse_as_resp2_pubsub(server: &Server, frame: Resp3Frame) -> Result<Message, RedisError> {
  if let Some(message) = parse_shard_pubsub_frame(server, &frame) {
    return Ok(message);
  }

  // resp3 has an added "pubsub" simple string frame at the front
  let mut out = Vec::with_capacity(frame.len() + 1);
  out.push(Resp3Frame::SimpleString {
    data:       PUBSUB_PUSH_PREFIX.into(),
    attributes: None,
  });

  if let Resp3Frame::Push { data, .. } = frame {
    out.extend(data);
    let frame = Resp3Frame::Push {
      data:       out,
      attributes: None,
    };

    frame_to_pubsub(server, frame)
  } else {
    Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid pubsub message. Expected push frame.",
    ))
  }
}

#[cfg(not(feature = "ignore-auth-error"))]
pub fn check_resp2_auth_error(frame: Resp2Frame) -> Resp2Frame {
  frame
}

#[cfg(feature = "ignore-auth-error")]
pub fn check_resp2_auth_error(frame: Resp2Frame) -> Resp2Frame {
  let is_auth_error = match frame {
    Resp2Frame::Error(ref data) => {
      *data == "ERR Client sent AUTH, but no password is set"
        || data.starts_with("ERR AUTH <password> called without any password configured for the default user")
    },
    _ => false,
  };

  if is_auth_error {
    Resp2Frame::SimpleString(OK.into())
  } else {
    frame
  }
}

#[cfg(not(feature = "ignore-auth-error"))]
pub fn check_resp3_auth_error(frame: Resp3Frame) -> Resp3Frame {
  frame
}

#[cfg(feature = "ignore-auth-error")]
pub fn check_resp3_auth_error(frame: Resp3Frame) -> Resp3Frame {
  let is_auth_error = match frame {
    Resp3Frame::SimpleError { ref data, .. } => {
      *data == "ERR Client sent AUTH, but no password is set"
        || data.starts_with("ERR AUTH <password> called without any password configured for the default user")
    },
    _ => false,
  };

  if is_auth_error {
    Resp3Frame::SimpleString {
      data:       "OK".into(),
      attributes: None,
    }
  } else {
    frame
  }
}

/// Try to parse the data as a string, and failing that return a byte slice.
pub fn string_or_bytes(data: Bytes) -> RedisValue {
  if let Some(s) = Str::from_inner(data.clone()).ok() {
    RedisValue::String(s)
  } else {
    RedisValue::Bytes(data)
  }
}

pub fn frame_to_bytes(frame: &Resp3Frame) -> Option<Bytes> {
  match frame {
    Resp3Frame::BigNumber { data, .. } => Some(data.clone()),
    Resp3Frame::VerbatimString { data, .. } => Some(data.clone()),
    Resp3Frame::BlobString { data, .. } => Some(data.clone()),
    Resp3Frame::SimpleString { data, .. } => Some(data.clone()),
    Resp3Frame::BlobError { data, .. } => Some(data.clone()),
    Resp3Frame::SimpleError { data, .. } => Some(data.inner().clone()),
    _ => None,
  }
}

pub fn frame_to_str(frame: &Resp3Frame) -> Option<Str> {
  match frame {
    Resp3Frame::BigNumber { data, .. } => Str::from_inner(data.clone()).ok(),
    Resp3Frame::VerbatimString { data, .. } => Str::from_inner(data.clone()).ok(),
    Resp3Frame::BlobString { data, .. } => Str::from_inner(data.clone()).ok(),
    Resp3Frame::SimpleString { data, .. } => Str::from_inner(data.clone()).ok(),
    Resp3Frame::BlobError { data, .. } => Str::from_inner(data.clone()).ok(),
    Resp3Frame::SimpleError { data, .. } => Some(data.clone()),
    _ => None,
  }
}

fn parse_nested_map(data: FrameMap) -> Result<RedisMap, RedisError> {
  let mut out = HashMap::with_capacity(data.len());

  // maybe make this smarter, but that would require changing the RedisMap type to use potentially non-hashable types
  // as keys...
  for (key, value) in data.into_iter() {
    let key: RedisKey = frame_to_results(key)?.try_into()?;
    let value = frame_to_results(value)?;

    out.insert(key, value);
  }

  Ok(RedisMap { inner: out })
}

/// Convert `nil` responses to a generic `Timeout` error.
pub fn check_null_timeout(frame: &Resp3Frame) -> Result<(), RedisError> {
  if frame.is_null() {
    Err(RedisError::new(RedisErrorKind::Timeout, "Request timed out."))
  } else {
    Ok(())
  }
}

/// Parse the protocol frame into a redis value, with support for arbitrarily nested arrays.
///
/// Unlike `frame_to_results` this will not unwrap single-element arrays.
pub fn frame_to_results(frame: Resp3Frame) -> Result<RedisValue, RedisError> {
  let value = match frame {
    Resp3Frame::Null => RedisValue::Null,
    Resp3Frame::SimpleString { data, .. } => {
      let value = string_or_bytes(data);

      if value.as_str().map(|s| s == QUEUED).unwrap_or(false) {
        RedisValue::Queued
      } else {
        value
      }
    },
    Resp3Frame::SimpleError { data, .. } => return Err(pretty_error(&data)),
    Resp3Frame::BlobString { data, .. } => string_or_bytes(data),
    Resp3Frame::BlobError { data, .. } => {
      let parsed = String::from_utf8_lossy(&data);
      return Err(pretty_error(parsed.as_ref()));
    },
    Resp3Frame::VerbatimString { data, .. } => string_or_bytes(data),
    Resp3Frame::Number { data, .. } => data.into(),
    Resp3Frame::Double { data, .. } => data.into(),
    Resp3Frame::BigNumber { data, .. } => string_or_bytes(data),
    Resp3Frame::Boolean { data, .. } => data.into(),
    Resp3Frame::Array { data, .. } | Resp3Frame::Push { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_to_results(frame)?);
      }

      RedisValue::Array(out)
    },
    Resp3Frame::Set { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_to_results(frame)?);
      }

      RedisValue::Array(out)
    },
    Resp3Frame::Map { data, .. } => {
      let mut out = HashMap::with_capacity(data.len());
      for (key, value) in data.into_iter() {
        let key: RedisKey = frame_to_results(key)?.try_into()?;
        let value = frame_to_results(value)?;

        out.insert(key, value);
      }

      RedisValue::Map(RedisMap { inner: out })
    },
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Invalid response frame type.",
      ))
    },
  };

  Ok(value)
}

/// Flatten a single nested layer of arrays or sets into one array.
pub fn flatten_frame(frame: Resp3Frame) -> Resp3Frame {
  match frame {
    Resp3Frame::Array { data, .. } => {
      let count = data.iter().fold(0, |c, f| {
        c + match f {
          Resp3Frame::Push { ref data, .. } => data.len(),
          Resp3Frame::Array { ref data, .. } => data.len(),
          Resp3Frame::Set { ref data, .. } => data.len(),
          _ => 1,
        }
      });

      let mut out = Vec::with_capacity(count);
      for frame in data.into_iter() {
        match frame {
          Resp3Frame::Push { data, .. } => out.extend(data),
          Resp3Frame::Array { data, .. } => out.extend(data),
          Resp3Frame::Set { data, .. } => out.extend(data),
          _ => out.push(frame),
        };
      }

      Resp3Frame::Array {
        data:       out,
        attributes: None,
      }
    },
    Resp3Frame::Set { data, .. } => {
      let count = data.iter().fold(0, |c, f| {
        c + match f {
          Resp3Frame::Array { ref data, .. } => data.len(),
          Resp3Frame::Set { ref data, .. } => data.len(),
          _ => 1,
        }
      });

      let mut out = Vec::with_capacity(count);
      for frame in data.into_iter() {
        match frame {
          Resp3Frame::Array { data, .. } => out.extend(data),
          Resp3Frame::Set { data, .. } => out.extend(data),
          _ => out.push(frame),
        };
      }

      Resp3Frame::Array {
        data:       out,
        attributes: None,
      }
    },
    _ => frame,
  }
}

/// Convert a frame to a nested RedisMap.
pub fn frame_to_map(frame: Resp3Frame) -> Result<RedisMap, RedisError> {
  match frame {
    Resp3Frame::Array { mut data, .. } => {
      if data.is_empty() {
        return Ok(RedisMap::new());
      }
      if data.len() % 2 != 0 {
        return Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Expected an even number of frames.",
        ));
      }

      let mut inner = HashMap::with_capacity(data.len() / 2);
      while data.len() >= 2 {
        let value = frame_to_results(data.pop().unwrap())?;
        let key = frame_to_results(data.pop().unwrap())?.try_into()?;

        inner.insert(key, value);
      }

      Ok(RedisMap { inner })
    },
    Resp3Frame::Map { data, .. } => parse_nested_map(data),
    Resp3Frame::SimpleError { data, .. } => Err(pretty_error(&data)),
    Resp3Frame::BlobError { data, .. } => {
      let parsed = String::from_utf8_lossy(&data);
      Err(pretty_error(&parsed))
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Expected array or map frames.",
    )),
  }
}

/// Convert a frame to a `RedisError`.
pub fn frame_to_error(frame: &Resp3Frame) -> Option<RedisError> {
  match frame {
    Resp3Frame::SimpleError { ref data, .. } => Some(pretty_error(data)),
    Resp3Frame::BlobError { ref data, .. } => {
      let parsed = String::from_utf8_lossy(data);
      Some(pretty_error(parsed.as_ref()))
    },
    _ => None,
  }
}

pub fn value_to_outgoing_resp2_frame(value: &RedisValue) -> Result<Resp2Frame, RedisError> {
  let frame = match value {
    RedisValue::Double(ref f) => Resp2Frame::BulkString(f.to_string().into()),
    RedisValue::Boolean(ref b) => Resp2Frame::BulkString(b.to_string().into()),
    RedisValue::Integer(ref i) => Resp2Frame::BulkString(i.to_string().into()),
    RedisValue::String(ref s) => Resp2Frame::BulkString(s.inner().clone()),
    RedisValue::Bytes(ref b) => Resp2Frame::BulkString(b.clone()),
    RedisValue::Queued => Resp2Frame::BulkString(Bytes::from_static(QUEUED.as_bytes())),
    RedisValue::Null => Resp2Frame::Null,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument,
        format!("Invalid argument type: {}", value.kind()),
      ))
    },
  };

  Ok(frame)
}

pub fn value_to_outgoing_resp3_frame(value: &RedisValue) -> Result<Resp3Frame, RedisError> {
  let frame = match value {
    RedisValue::Double(ref f) => Resp3Frame::BlobString {
      data:       f.to_string().into(),
      attributes: None,
    },
    RedisValue::Boolean(ref b) => Resp3Frame::BlobString {
      data:       b.to_string().into(),
      attributes: None,
    },
    RedisValue::Integer(ref i) => Resp3Frame::BlobString {
      data:       i.to_string().into(),
      attributes: None,
    },
    RedisValue::String(ref s) => Resp3Frame::BlobString {
      data:       s.inner().clone(),
      attributes: None,
    },
    RedisValue::Bytes(ref b) => Resp3Frame::BlobString {
      data:       b.clone(),
      attributes: None,
    },
    RedisValue::Queued => Resp3Frame::BlobString {
      data:       Bytes::from_static(QUEUED.as_bytes()),
      attributes: None,
    },
    RedisValue::Null => Resp3Frame::Null,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument,
        format!("Invalid argument type: {}", value.kind()),
      ))
    },
  };

  Ok(frame)
}

#[cfg(feature = "mocks")]
pub fn mocked_value_to_frame(value: RedisValue) -> Resp3Frame {
  match value {
    RedisValue::Array(values) => Resp3Frame::Array {
      data:       values.into_iter().map(|v| mocked_value_to_frame(v)).collect(),
      attributes: None,
    },
    RedisValue::Map(values) => Resp3Frame::Map {
      data:       values
        .inner()
        .into_iter()
        .map(|(key, value)| (mocked_value_to_frame(key.into()), mocked_value_to_frame(value)))
        .collect(),
      attributes: None,
    },
    RedisValue::Null => Resp3Frame::Null,
    RedisValue::Queued => Resp3Frame::SimpleString {
      data:       Bytes::from_static(QUEUED.as_bytes()),
      attributes: None,
    },
    RedisValue::Bytes(value) => Resp3Frame::BlobString {
      data:       value,
      attributes: None,
    },
    RedisValue::Boolean(value) => Resp3Frame::Boolean {
      data:       value,
      attributes: None,
    },
    RedisValue::Integer(value) => Resp3Frame::Number {
      data:       value,
      attributes: None,
    },
    RedisValue::Double(value) => Resp3Frame::Double {
      data:       value,
      attributes: None,
    },
    RedisValue::String(value) => Resp3Frame::BlobString {
      data:       value.into_inner(),
      attributes: None,
    },
  }
}

pub fn expect_ok(value: &RedisValue) -> Result<(), RedisError> {
  match *value {
    RedisValue::String(ref resp) => {
      if resp.deref() == OK || resp.deref() == QUEUED {
        Ok(())
      } else {
        Err(RedisError::new(
          RedisErrorKind::Unknown,
          format!("Expected OK, found {}", resp),
        ))
      }
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown,
      format!("Expected OK, found {:?}.", value),
    )),
  }
}

fn parse_u64(val: &Resp3Frame) -> u64 {
  match *val {
    Resp3Frame::Number { ref data, .. } => {
      if *data < 0 {
        0
      } else {
        *data as u64
      }
    },
    Resp3Frame::Double { ref data, .. } => *data as u64,
    Resp3Frame::BlobString { ref data, .. } | Resp3Frame::SimpleString { ref data, .. } => str::from_utf8(data)
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(0),
    _ => 0,
  }
}

fn parse_f64(val: &Resp3Frame) -> f64 {
  match *val {
    Resp3Frame::Number { ref data, .. } => *data as f64,
    Resp3Frame::Double { ref data, .. } => *data,
    Resp3Frame::BlobString { ref data, .. } | Resp3Frame::SimpleString { ref data, .. } => str::from_utf8(data)
      .ok()
      .and_then(|s| redis_string_to_f64(s).ok())
      .unwrap_or(0.0),
    _ => 0.0,
  }
}

fn parse_db_memory_stats(data: &Vec<Resp3Frame>) -> Result<DatabaseMemoryStats, RedisError> {
  if data.len() % 2 != 0 {
    return Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid MEMORY STATS database response. Result must have an even number of frames.",
    ));
  }

  let mut out = DatabaseMemoryStats::default();
  for chunk in data.chunks(2) {
    let key = match chunk[0].as_str() {
      Some(s) => s,
      None => continue,
    };

    match key.as_ref() {
      "overhead.hashtable.main" => out.overhead_hashtable_main = parse_u64(&chunk[1]),
      "overhead.hashtable.expires" => out.overhead_hashtable_expires = parse_u64(&chunk[1]),
      _ => {},
    };
  }

  Ok(out)
}

fn parse_memory_stat_field(stats: &mut MemoryStats, key: &str, value: &Resp3Frame) {
  match key.as_ref() {
    "peak.allocated" => stats.peak_allocated = parse_u64(value),
    "total.allocated" => stats.total_allocated = parse_u64(value),
    "startup.allocated" => stats.startup_allocated = parse_u64(value),
    "replication.backlog" => stats.replication_backlog = parse_u64(value),
    "clients.slaves" => stats.clients_slaves = parse_u64(value),
    "clients.normal" => stats.clients_normal = parse_u64(value),
    "aof.buffer" => stats.aof_buffer = parse_u64(value),
    "lua.caches" => stats.lua_caches = parse_u64(value),
    "overhead.total" => stats.overhead_total = parse_u64(value),
    "keys.count" => stats.keys_count = parse_u64(value),
    "keys.bytes-per-key" => stats.keys_bytes_per_key = parse_u64(value),
    "dataset.bytes" => stats.dataset_bytes = parse_u64(value),
    "dataset.percentage" => stats.dataset_percentage = parse_f64(value),
    "peak.percentage" => stats.peak_percentage = parse_f64(value),
    "allocator.allocated" => stats.allocator_allocated = parse_u64(value),
    "allocator.active" => stats.allocator_active = parse_u64(value),
    "allocator.resident" => stats.allocator_resident = parse_u64(value),
    "allocator-fragmentation.ratio" => stats.allocator_fragmentation_ratio = parse_f64(value),
    "allocator-fragmentation.bytes" => stats.allocator_fragmentation_bytes = parse_u64(value),
    "allocator-rss.ratio" => stats.allocator_rss_ratio = parse_f64(value),
    "allocator-rss.bytes" => stats.allocator_rss_bytes = parse_u64(value),
    "rss-overhead.ratio" => stats.rss_overhead_ratio = parse_f64(value),
    "rss-overhead.bytes" => stats.rss_overhead_bytes = parse_u64(value),
    "fragmentation" => stats.fragmentation = parse_f64(value),
    "fragmentation.bytes" => stats.fragmentation_bytes = parse_u64(value),
    _ => {},
  }
}

pub fn parse_memory_stats(data: &Vec<Resp3Frame>) -> Result<MemoryStats, RedisError> {
  if data.len() % 2 != 0 {
    return Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid MEMORY STATS response. Result must have an even number of frames.",
    ));
  }

  let mut out = MemoryStats::default();
  for chunk in data.chunks(2) {
    let key = match chunk[0].as_str() {
      Some(s) => s,
      None => continue,
    };

    if key.starts_with("db.") {
      let db = match key.split(".").last() {
        Some(db) => match db.parse::<u16>().ok() {
          Some(db) => db,
          None => continue,
        },
        None => continue,
      };

      let inner = match chunk[1] {
        Resp3Frame::Array { ref data, .. } => data,
        _ => continue,
      };
      let parsed = parse_db_memory_stats(inner)?;

      out.db.insert(db, parsed);
    } else {
      parse_memory_stat_field(&mut out, key, &chunk[1]);
    }
  }

  Ok(out)
}

fn parse_acl_getuser_flag(value: &Resp3Frame) -> Result<Vec<AclUserFlag>, RedisError> {
  if let Resp3Frame::Array { ref data, .. } = value {
    let mut out = Vec::with_capacity(data.len());

    for frame in data.iter() {
      let flag = match frame.as_str() {
        Some(s) => match s.as_ref() {
          "on" => AclUserFlag::On,
          "off" => AclUserFlag::Off,
          "allcommands" => AclUserFlag::AllCommands,
          "allkeys" => AclUserFlag::AllKeys,
          "allchannels" => AclUserFlag::AllChannels,
          "nopass" => AclUserFlag::NoPass,
          _ => continue,
        },
        None => continue,
      };

      out.push(flag);
    }

    Ok(out)
  } else {
    Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid ACL user flags. Expected array.",
    ))
  }
}

fn frames_to_strings(frames: &Resp3Frame) -> Result<Vec<String>, RedisError> {
  match frames {
    Resp3Frame::Array { ref data, .. } => {
      let mut out = Vec::with_capacity(data.len());

      for frame in data.iter() {
        let val = match frame.as_str() {
          Some(v) => v.to_owned(),
          None => continue,
        };

        out.push(val);
      }

      Ok(out)
    },
    Resp3Frame::SimpleString { ref data, .. } => Ok(vec![String::from_utf8(data.to_vec())?]),
    Resp3Frame::BlobString { ref data, .. } => Ok(vec![String::from_utf8(data.to_vec())?]),
    _ => Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Expected string or array of frames.",
    )),
  }
}

fn parse_acl_getuser_field(user: &mut AclUser, key: &str, value: &Resp3Frame) -> Result<(), RedisError> {
  match key.as_ref() {
    "passwords" => user.passwords = frames_to_strings(value)?,
    "keys" => user.keys = frames_to_strings(value)?,
    "channels" => user.channels = frames_to_strings(value)?,
    "commands" => {
      if let Some(commands) = value.as_str() {
        user.commands = commands.split(" ").map(|s| s.to_owned()).collect();
      }
    },
    _ => {
      debug!("Skip ACL GETUSER field: {}", key);
    },
  };

  Ok(())
}

pub fn frame_map_or_set_to_nested_array(frame: Resp3Frame) -> Result<Resp3Frame, RedisError> {
  match frame {
    Resp3Frame::Map { data, .. } => {
      let mut out = Vec::with_capacity(data.len() * 2);
      for (key, value) in data.into_iter() {
        out.push(key);
        out.push(frame_map_or_set_to_nested_array(value)?);
      }

      Ok(Resp3Frame::Array {
        data:       out,
        attributes: None,
      })
    },
    Resp3Frame::Set { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_map_or_set_to_nested_array(frame)?);
      }

      Ok(Resp3Frame::Array {
        data:       out,
        attributes: None,
      })
    },
    _ => Ok(frame),
  }
}

pub fn parse_acl_getuser_frames(frames: Vec<Resp3Frame>) -> Result<AclUser, RedisError> {
  if frames.len() % 2 != 0 {
    return Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid number of response frames.",
    ));
  }

  let mut user = AclUser::default();
  for chunk in frames.chunks(2) {
    let key = match chunk[0].as_str() {
      Some(s) => s,
      None => continue,
    };

    if key == "flags" {
      user.flags = parse_acl_getuser_flag(&chunk[1])?;
    } else {
      parse_acl_getuser_field(&mut user, key, &chunk[1])?
    }
  }

  Ok(user)
}

/// Parse the replicas from the ROLE response returned from a master/primary node.
#[cfg(feature = "replicas")]
pub fn parse_master_role_replicas(data: RedisValue) -> Result<Vec<Server>, RedisError> {
  let mut role: Vec<RedisValue> = data.convert()?;

  if role.len() == 3 {
    if role[0].as_str().map(|s| s == "master").unwrap_or(false) {
      let replicas: Vec<RedisValue> = role[2].take().convert()?;

      Ok(
        replicas
          .into_iter()
          .filter_map(|value| {
            value
              .convert::<(String, u16, String)>()
              .ok()
              .map(|(host, port, _)| Server::new(host, port))
          })
          .collect(),
      )
    } else {
      Ok(Vec::new())
    }
  } else {
    // we're talking to a replica or sentinel node
    Ok(Vec::new())
  }
}

pub fn assert_array_len<T>(data: &Vec<T>, len: usize) -> Result<(), RedisError> {
  if data.len() == len {
    Ok(())
  } else {
    Err(RedisError::new(
      RedisErrorKind::Parse,
      format!("Expected {} values.", len),
    ))
  }
}

/// Flatten a nested array of values into one array.
pub fn flatten_redis_value(value: RedisValue) -> RedisValue {
  if let RedisValue::Array(values) = value {
    let mut out = Vec::with_capacity(values.len());
    for value in values.into_iter() {
      let flattened = flatten_redis_value(value);
      if let RedisValue::Array(flattened) = flattened {
        out.extend(flattened);
      } else {
        out.push(flattened);
      }
    }

    RedisValue::Array(out)
  } else {
    value
  }
}

/// Convert a redis value to an array of (value, score) tuples.
pub fn value_to_zset_result(value: RedisValue) -> Result<Vec<(RedisValue, f64)>, RedisError> {
  let value = flatten_redis_value(value);

  if let RedisValue::Array(mut values) = value {
    if values.is_empty() {
      return Ok(Vec::new());
    }
    if values.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Expected an even number of redis values.",
      ));
    }

    let mut out = Vec::with_capacity(values.len() / 2);
    while values.len() >= 2 {
      let score = match values.pop().unwrap().as_f64() {
        Some(f) => f,
        None => {
          return Err(RedisError::new(
            RedisErrorKind::Protocol,
            "Could not convert value to floating point number.",
          ))
        },
      };
      let value = values.pop().unwrap();

      out.push((value, score));
    }

    Ok(out)
  } else {
    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Expected array of redis values.",
    ))
  }
}

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
fn i64_size(i: i64) -> usize {
  if i < 0 {
    1 + redis_protocol::digits_in_number((i * -1) as usize)
  } else {
    redis_protocol::digits_in_number(i as usize)
  }
}

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
pub fn arg_size(value: &RedisValue) -> usize {
  match value {
    // use the RESP2 size
    RedisValue::Boolean(_) => 5,
    // FIXME make this more accurate by casting to an i64 and using `digits_in_number`
    // the tricky part is doing so without allocating and without any loss in precision, but
    // this is only used for logging and tracing
    RedisValue::Double(_) => 10,
    RedisValue::Null => 3,
    RedisValue::Integer(ref i) => i64_size(*i),
    RedisValue::String(ref s) => s.inner().len(),
    RedisValue::Bytes(ref b) => b.len(),
    RedisValue::Array(ref arr) => args_size(arr),
    RedisValue::Map(ref map) => map
      .inner
      .iter()
      .fold(0, |c, (k, v)| c + k.as_bytes().len() + arg_size(v)),
    RedisValue::Queued => 0,
  }
}

#[cfg(feature = "blocking-encoding")]
pub fn resp2_frame_size(frame: &Resp2Frame) -> usize {
  match frame {
    Resp2Frame::Integer(ref i) => i64_size(*i),
    Resp2Frame::Null => 3,
    Resp2Frame::Error(ref s) => s.as_bytes().len(),
    Resp2Frame::SimpleString(ref s) => s.len(),
    Resp2Frame::BulkString(ref b) => b.len(),
    Resp2Frame::Array(ref a) => a.iter().fold(0, |c, f| c + resp2_frame_size(f)),
  }
}

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
pub fn resp3_frame_size(frame: &Resp3Frame) -> usize {
  frame.encode_len().unwrap_or(0)
}

#[cfg(feature = "blocking-encoding")]
pub fn frame_size(frame: &ProtocolFrame) -> usize {
  match frame {
    ProtocolFrame::Resp3(f) => resp3_frame_size(f),
    ProtocolFrame::Resp2(f) => resp2_frame_size(f),
  }
}

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
pub fn args_size(args: &Vec<RedisValue>) -> usize {
  args.iter().fold(0, |c, arg| c + arg_size(arg))
}

fn serialize_hello(command: &RedisCommand, version: &RespVersion) -> Result<Resp3Frame, RedisError> {
  let args = command.args();

  let auth = if args.len() == 2 {
    // has username and password
    let username = match args[0].as_bytes_str() {
      Some(username) => username,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid username. Expected string.",
        ));
      },
    };
    let password = match args[1].as_bytes_str() {
      Some(password) => password,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid password. Expected string.",
        ));
      },
    };

    Some(Auth { username, password })
  } else if args.len() == 1 {
    // just has a password (assume the default user)
    let password = match args[0].as_bytes_str() {
      Some(password) => password,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid password. Expected string.",
        ));
      },
    };

    Some(Auth::from_password(password))
  } else {
    None
  };

  Ok(Resp3Frame::Hello {
    version: version.clone(),
    auth,
  })
}

pub fn command_to_resp3_frame(command: &RedisCommand) -> Result<Resp3Frame, RedisError> {
  let args = command.args();

  match command.kind {
    RedisCommandKind::_Custom(ref kind) => {
      let parts: Vec<&str> = kind.cmd.trim().split(" ").collect();
      let mut bulk_strings = Vec::with_capacity(parts.len() + args.len());

      for part in parts.into_iter() {
        bulk_strings.push(Resp3Frame::BlobString {
          data:       part.as_bytes().to_vec().into(),
          attributes: None,
        });
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp3_frame(value)?);
      }

      Ok(Resp3Frame::Array {
        data:       bulk_strings,
        attributes: None,
      })
    },
    RedisCommandKind::_Hello(ref version) => serialize_hello(command, version),
    _ => {
      let mut bulk_strings = Vec::with_capacity(args.len() + 2);

      bulk_strings.push(Resp3Frame::BlobString {
        data:       command.kind.cmd_str().into_inner(),
        attributes: None,
      });

      if let Some(subcommand) = command.kind.subcommand_str() {
        bulk_strings.push(Resp3Frame::BlobString {
          data:       subcommand.into_inner(),
          attributes: None,
        });
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp3_frame(value)?);
      }

      Ok(Resp3Frame::Array {
        data:       bulk_strings,
        attributes: None,
      })
    },
  }
}

pub fn command_to_resp2_frame(command: &RedisCommand) -> Result<Resp2Frame, RedisError> {
  let args = command.args();

  match command.kind {
    RedisCommandKind::_Custom(ref kind) => {
      let parts: Vec<&str> = kind.cmd.trim().split(" ").collect();
      let mut bulk_strings = Vec::with_capacity(parts.len() + args.len());

      for part in parts.into_iter() {
        bulk_strings.push(Resp2Frame::BulkString(part.as_bytes().to_vec().into()));
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp2_frame(value)?);
      }

      Ok(Resp2Frame::Array(bulk_strings))
    },
    _ => {
      let mut bulk_strings = Vec::with_capacity(args.len() + 2);

      bulk_strings.push(Resp2Frame::BulkString(command.kind.cmd_str().into_inner()));
      if let Some(subcommand) = command.kind.subcommand_str() {
        bulk_strings.push(Resp2Frame::BulkString(subcommand.into_inner()));
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp2_frame(value)?);
      }

      Ok(Resp2Frame::Array(bulk_strings))
    },
  }
}

/// Serialize the command as a protocol frame.
pub fn command_to_frame(command: &RedisCommand, is_resp3: bool) -> Result<ProtocolFrame, RedisError> {
  if is_resp3 || command.kind.is_hello() {
    command_to_resp3_frame(command).map(|c| c.into())
  } else {
    command_to_resp2_frame(command).map(|c| c.into())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::{collections::HashMap, time::Duration};

  fn str_to_f(s: &str) -> Resp3Frame {
    Resp3Frame::SimpleString {
      data:       s.to_owned().into(),
      attributes: None,
    }
  }

  fn str_to_bs(s: &str) -> Resp3Frame {
    Resp3Frame::BlobString {
      data:       s.to_owned().into(),
      attributes: None,
    }
  }

  fn int_to_f(i: i64) -> Resp3Frame {
    Resp3Frame::Number {
      data:       i,
      attributes: None,
    }
  }

  fn string_vec(d: Vec<&str>) -> Vec<String> {
    d.into_iter().map(|s| s.to_owned()).collect()
  }

  #[test]
  fn should_parse_memory_stats() {
    // better from()/into() interfaces for frames coming in the next redis-protocol version...
    let frames: Vec<Resp3Frame> = vec![
      str_to_f("peak.allocated"),
      int_to_f(934192),
      str_to_f("total.allocated"),
      int_to_f(872040),
      str_to_f("startup.allocated"),
      int_to_f(809912),
      str_to_f("replication.backlog"),
      int_to_f(0),
      str_to_f("clients.slaves"),
      int_to_f(0),
      str_to_f("clients.normal"),
      int_to_f(20496),
      str_to_f("aof.buffer"),
      int_to_f(0),
      str_to_f("lua.caches"),
      int_to_f(0),
      str_to_f("db.0"),
      Resp3Frame::Array {
        data:       vec![
          str_to_f("overhead.hashtable.main"),
          int_to_f(72),
          str_to_f("overhead.hashtable.expires"),
          int_to_f(0),
        ],
        attributes: None,
      },
      str_to_f("overhead.total"),
      int_to_f(830480),
      str_to_f("keys.count"),
      int_to_f(1),
      str_to_f("keys.bytes-per-key"),
      int_to_f(62128),
      str_to_f("dataset.bytes"),
      int_to_f(41560),
      str_to_f("dataset.percentage"),
      str_to_f("66.894157409667969"),
      str_to_f("peak.percentage"),
      str_to_f("93.346977233886719"),
      str_to_f("allocator.allocated"),
      int_to_f(1022640),
      str_to_f("allocator.active"),
      int_to_f(1241088),
      str_to_f("allocator.resident"),
      int_to_f(5332992),
      str_to_f("allocator-fragmentation.ratio"),
      str_to_f("1.2136118412017822"),
      str_to_f("allocator-fragmentation.bytes"),
      int_to_f(218448),
      str_to_f("allocator-rss.ratio"),
      str_to_f("4.2970294952392578"),
      str_to_f("allocator-rss.bytes"),
      int_to_f(4091904),
      str_to_f("rss-overhead.ratio"),
      str_to_f("2.0268816947937012"),
      str_to_f("rss-overhead.bytes"),
      int_to_f(5476352),
      str_to_f("fragmentation"),
      str_to_f("13.007383346557617"),
      str_to_f("fragmentation.bytes"),
      int_to_f(9978328),
    ];
    let memory_stats = parse_memory_stats(&frames).unwrap();

    let expected_db_0 = DatabaseMemoryStats {
      overhead_hashtable_expires: 0,
      overhead_hashtable_main:    72,
    };
    let mut expected_db = HashMap::new();
    expected_db.insert(0, expected_db_0);
    let expected = MemoryStats {
      peak_allocated:                934192,
      total_allocated:               872040,
      startup_allocated:             809912,
      replication_backlog:           0,
      clients_slaves:                0,
      clients_normal:                20496,
      aof_buffer:                    0,
      lua_caches:                    0,
      db:                            expected_db,
      overhead_total:                830480,
      keys_count:                    1,
      keys_bytes_per_key:            62128,
      dataset_bytes:                 41560,
      dataset_percentage:            66.894157409667969,
      peak_percentage:               93.346977233886719,
      allocator_allocated:           1022640,
      allocator_active:              1241088,
      allocator_resident:            5332992,
      allocator_fragmentation_ratio: 1.2136118412017822,
      allocator_fragmentation_bytes: 218448,
      allocator_rss_ratio:           4.2970294952392578,
      allocator_rss_bytes:           4091904,
      rss_overhead_ratio:            2.0268816947937012,
      rss_overhead_bytes:            5476352,
      fragmentation:                 13.007383346557617,
      fragmentation_bytes:           9978328,
    };

    assert_eq!(memory_stats, expected);
  }

  #[test]
  fn should_parse_acl_getuser_response() {
    // 127.0.0.1:6379> acl getuser alec
    // 1) "flags"
    // 2) 1) "on"
    // 3) "passwords"
    // 4) 1) "c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5"
    // 2) "39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45"
    // 5) "commands"
    // 6) "-@all +@sortedset +@geo +config|get"
    // 7) "keys"
    // 8) 1) "a"
    // 2) "b"
    // 3) "c"
    // 9) "channels"
    // 10) 1) "c1"
    // 2) "c2"

    let input = vec![
      str_to_bs("flags"),
      Resp3Frame::Array {
        data:       vec![str_to_bs("on")],
        attributes: None,
      },
      str_to_bs("passwords"),
      Resp3Frame::Array {
        data:       vec![
          str_to_bs("c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5"),
          str_to_bs("39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45"),
        ],
        attributes: None,
      },
      str_to_bs("commands"),
      str_to_bs("-@all +@sortedset +@geo +config|get"),
      str_to_bs("keys"),
      Resp3Frame::Array {
        data:       vec![str_to_bs("a"), str_to_bs("b"), str_to_bs("c")],
        attributes: None,
      },
      str_to_bs("channels"),
      Resp3Frame::Array {
        data:       vec![str_to_bs("c1"), str_to_bs("c2")],
        attributes: None,
      },
    ];
    let actual = parse_acl_getuser_frames(input).unwrap();

    let expected = AclUser {
      flags:     vec![AclUserFlag::On],
      passwords: string_vec(vec![
        "c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5",
        "39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45",
      ]),
      commands:  string_vec(vec!["-@all", "+@sortedset", "+@geo", "+config|get"]),
      keys:      string_vec(vec!["a", "b", "c"]),
      channels:  string_vec(vec!["c1", "c2"]),
    };
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_slowlog_entries_redis_3() {
    // redis 127.0.0.1:6379> slowlog get 2
    // 1) 1) (integer) 14
    // 2) (integer) 1309448221
    // 3) (integer) 15
    // 4) 1) "ping"
    // 2) 1) (integer) 13
    // 2) (integer) 1309448128
    // 3) (integer) 30
    // 4) 1) "slowlog"
    // 2) "get"
    // 3) "100"

    let input = frame_to_results(Resp3Frame::Array {
      data:       vec![
        Resp3Frame::Array {
          data:       vec![int_to_f(14), int_to_f(1309448221), int_to_f(15), Resp3Frame::Array {
            data:       vec![str_to_bs("ping")],
            attributes: None,
          }],
          attributes: None,
        },
        Resp3Frame::Array {
          data:       vec![int_to_f(13), int_to_f(1309448128), int_to_f(30), Resp3Frame::Array {
            data:       vec![str_to_bs("slowlog"), str_to_bs("get"), str_to_bs("100")],
            attributes: None,
          }],
          attributes: None,
        },
      ],
      attributes: None,
    })
    .unwrap();
    let actual: Vec<SlowlogEntry> = input.convert().unwrap();

    let expected = vec![
      SlowlogEntry {
        id:        14,
        timestamp: 1309448221,
        duration:  Duration::from_micros(15),
        args:      vec!["ping".into()],
        ip:        None,
        name:      None,
      },
      SlowlogEntry {
        id:        13,
        timestamp: 1309448128,
        duration:  Duration::from_micros(30),
        args:      vec!["slowlog".into(), "get".into(), "100".into()],
        ip:        None,
        name:      None,
      },
    ];

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_slowlog_entries_redis_4() {
    // redis 127.0.0.1:6379> slowlog get 2
    // 1) 1) (integer) 14
    // 2) (integer) 1309448221
    // 3) (integer) 15
    // 4) 1) "ping"
    // 5) "127.0.0.1:58217"
    // 6) "worker-123"
    // 2) 1) (integer) 13
    // 2) (integer) 1309448128
    // 3) (integer) 30
    // 4) 1) "slowlog"
    // 2) "get"
    // 3) "100"
    // 5) "127.0.0.1:58217"
    // 6) "worker-123"

    let input = frame_to_results(Resp3Frame::Array {
      data:       vec![
        Resp3Frame::Array {
          data:       vec![
            int_to_f(14),
            int_to_f(1309448221),
            int_to_f(15),
            Resp3Frame::Array {
              data:       vec![str_to_bs("ping")],
              attributes: None,
            },
            str_to_bs("127.0.0.1:58217"),
            str_to_bs("worker-123"),
          ],
          attributes: None,
        },
        Resp3Frame::Array {
          data:       vec![
            int_to_f(13),
            int_to_f(1309448128),
            int_to_f(30),
            Resp3Frame::Array {
              data:       vec![str_to_bs("slowlog"), str_to_bs("get"), str_to_bs("100")],
              attributes: None,
            },
            str_to_bs("127.0.0.1:58217"),
            str_to_bs("worker-123"),
          ],
          attributes: None,
        },
      ],
      attributes: None,
    })
    .unwrap();
    let actual: Vec<SlowlogEntry> = input.convert().unwrap();

    let expected = vec![
      SlowlogEntry {
        id:        14,
        timestamp: 1309448221,
        duration:  Duration::from_micros(15),
        args:      vec!["ping".into()],
        ip:        Some("127.0.0.1:58217".into()),
        name:      Some("worker-123".into()),
      },
      SlowlogEntry {
        id:        13,
        timestamp: 1309448128,
        duration:  Duration::from_micros(30),
        args:      vec!["slowlog".into(), "get".into(), "100".into()],
        ip:        Some("127.0.0.1:58217".into()),
        name:      Some("worker-123".into()),
      },
    ];

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_info() {
    let input: RedisValue = "cluster_state:fail
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:3
cluster_slots_fail:2
cluster_known_nodes:6
cluster_size:3
cluster_current_epoch:6
cluster_my_epoch:2
cluster_stats_messages_sent:1483972
cluster_stats_messages_received:1483968"
      .into();

    let expected = ClusterInfo {
      cluster_state:                   ClusterState::Fail,
      cluster_slots_assigned:          16384,
      cluster_slots_ok:                16384,
      cluster_slots_fail:              2,
      cluster_slots_pfail:             3,
      cluster_known_nodes:             6,
      cluster_size:                    3,
      cluster_current_epoch:           6,
      cluster_my_epoch:                2,
      cluster_stats_messages_sent:     1483972,
      cluster_stats_messages_received: 1483968,
    };
    let actual: ClusterInfo = input.convert().unwrap();

    assert_eq!(actual, expected);
  }
}
