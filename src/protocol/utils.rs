use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::protocol::connection::OK;
use crate::protocol::types::ProtocolFrame;
use crate::protocol::types::*;
use crate::types::Resolve;
use crate::types::*;
use crate::types::{RedisConfig, ServerConfig, QUEUED};
use crate::utils;
use crate::utils::redis_string_to_f64;
use arcstr::ArcStr;
use bytes::Bytes;
use bytes_utils::Str;
use parking_lot::RwLock;
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp3::types::{Auth, PUBSUB_PUSH_PREFIX};
use redis_protocol::resp3::types::{Frame as Resp3Frame, FrameMap};
use semver::Version;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::ops::Deref;
use std::str;
use std::sync::Arc;

macro_rules! parse_or_zero(
  ($data:ident, $t:ty) => {
    $data.parse::<$t>().ok().unwrap_or(0)
  }
);

pub fn initial_buffer_size(inner: &Arc<RedisClientInner>) -> usize {
  if inner.performance.as_ref().load().pipeline {
    256
  } else {
    1
  }
}

/// Read the major redis version, assuming version 6 if a version is not provided.
pub fn major_redis_version(version: &Option<Version>) -> u8 {
  version.as_ref().map(|v| v.major as u8).unwrap_or(6)
}

pub fn null_frame(is_resp3: bool) -> ProtocolFrame {
  if is_resp3 {
    ProtocolFrame::Resp2(Resp2Frame::Null)
  } else {
    ProtocolFrame::Resp3(Resp3Frame::Null)
  }
}

pub fn is_ok(frame: &ProtocolFrame) -> bool {
  match frame {
    ProtocolFrame::Resp3(ref frame) => match frame {
      Resp3Frame::SimpleString { ref data, .. } => data == OK,
      _ => false,
    },
    ProtocolFrame::Resp2(ref frame) => match frame {
      Resp2Frame::SimpleString(ref data) => data == OK,
      _ => false,
    },
  }
}

/// Whether the provided frame is null.
pub fn is_null(frame: &Resp3Frame) -> bool {
  match frame {
    Resp3Frame::Null => true,
    _ => false,
  }
}

#[cfg(not(feature = "no-client-setname"))]
pub fn is_ok(frame: &ProtocolFrame) -> bool {
  match frame {
    ProtocolFrame::Resp3(ref frame) => match frame {
      Resp3Frame::SimpleString { ref data, .. } => data == OK,
      _ => false,
    },
    ProtocolFrame::Resp2(ref frame) => match frame {
      Resp2Frame::SimpleString(ref data) => data == OK,
      _ => false,
    },
  }
}

#[cfg(not(feature = "enable-native-tls"))]
pub fn uses_tls(_: &Arc<RedisClientInner>) -> bool {
  false
}

pub fn server_to_parts(server: &ArcStr) -> Result<(&str, u16), RedisError> {
  let parts: Vec<&str> = server.split(":").collect();
  if parts.len() < 2 {
    return Err(RedisError::new(RedisErrorKind::IO, "Invalid server."));
  }
  Ok((parts[0], parts[1].parse::<u16>()?))
}

/// Parse a cluster server string to read the (domain, IP address/port).
pub async fn parse_cluster_server(
  inner: &Arc<RedisClientInner>,
  server: &str,
) -> Result<(String, SocketAddr), RedisError> {
  let parts: Vec<&str> = server.trim().split(":").collect();
  if parts.len() != 2 {
    return Err(RedisError::new(
      RedisErrorKind::UrlError,
      "Invalid cluster server name. Expected host:port.",
    ));
  }

  let port = parts[1].parse::<u16>()?;
  let addr = inner.resolver.resolve(parts[0].to_owned(), port).await?;

  Ok((parts[0].to_owned(), addr))
}

/// Server hostnames/IP addresses can have a cport suffix of the form `@1122` that needs to be removed.
fn remove_cport_suffix(server: &str) -> &str {
  if let Some(first) = server.split("@").next() {
    return first;
  }

  server
}

// TODO if None is returned then sync the cluster before returning an error
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
      "MOVED" | "ASK" => RedisErrorKind::Cluster,
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
    _ => Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected protocol string.",
    )),
  }
}

/// Convert the frame to a `(channel, message)` tuple from the pubsub interface.
pub fn frame_to_pubsub(frame: Resp3Frame) -> Result<(String, RedisValue), RedisError> {
  if let Ok((channel, message)) = frame.parse_as_pubsub() {
    let channel = frame_into_string(channel)?;
    let message = frame_to_single_result(message)?;

    Ok((channel, message))
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid pubsub message frame.",
    ))
  }
}

/// Attempt to parse a RESP3 frame as a pubsub message in the RESP2 format.
///
/// This can be useful in cases where the codec layer automatically upgrades to RESP3,
/// but the contents of the pubsub message still use the RESP2 format.
pub fn parse_as_resp2_pubsub(frame: Resp3Frame) -> Result<(String, RedisValue), RedisError> {
  // there's a few ways to do this, but i don't want to re-implement the logic in redis_protocol.
  // the main difference between resp2 and resp3 here is the presence of a "pubsub" string at the
  // beginning of the push array, so we just add that to the front here.

  let mut out = Vec::with_capacity(frame.len() + 1);
  out.push(Resp3Frame::SimpleString {
    data: PUBSUB_PUSH_PREFIX.into(),
    attributes: None,
  });

  if let Resp3Frame::Push { data, .. } = frame {
    out.extend(data);
    let frame = Resp3Frame::Push {
      data: out,
      attributes: None,
    };

    frame_to_pubsub(frame)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
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
    Resp2Frame::Error(ref data) => *data == "ERR Client sent AUTH, but no password is set",
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
    Resp3Frame::SimpleError { ref data, .. } => *data == "ERR Client sent AUTH, but no password is set",
    _ => false,
  };

  if is_auth_error {
    Resp3Frame::SimpleString {
      data: "OK".into(),
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

fn parse_nested_array(data: Vec<Resp3Frame>) -> Result<RedisValue, RedisError> {
  let mut out = Vec::with_capacity(data.len());

  for frame in data.into_iter() {
    out.push(frame_to_results(frame)?);
  }

  if out.len() == 1 {
    Ok(out.pop().unwrap())
  } else {
    Ok(RedisValue::Array(out))
  }
}

fn parse_nested_map(data: FrameMap) -> Result<RedisMap, RedisError> {
  let mut out = HashMap::with_capacity(data.len());

  // maybe make this smarter, but that would require changing the RedisMap type to use potentially non-hashable types as keys...
  for (key, value) in data.into_iter() {
    let key: RedisKey = frame_to_single_result(key)?.try_into()?;
    let value = frame_to_results(value)?;

    out.insert(key, value);
  }

  Ok(RedisMap { inner: out })
}

/// Parse the protocol frame into a redis value, with support for arbitrarily nested arrays.
///
/// If the array contains one element then that element will be returned.
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
      // errors don't have a great way to represent non-utf8 strings...
      let parsed = String::from_utf8_lossy(&data);
      return Err(pretty_error(&parsed));
    },
    Resp3Frame::VerbatimString { data, .. } => string_or_bytes(data),
    Resp3Frame::Number { data, .. } => data.into(),
    Resp3Frame::Double { data, .. } => data.into(),
    Resp3Frame::BigNumber { data, .. } => string_or_bytes(data),
    Resp3Frame::Boolean { data, .. } => data.into(),
    Resp3Frame::Array { data, .. } => parse_nested_array(data)?,
    Resp3Frame::Push { data, .. } => parse_nested_array(data)?,
    Resp3Frame::Set { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_to_results(frame)?);
      }

      RedisValue::Array(out)
    },
    Resp3Frame::Map { data, .. } => RedisValue::Map(parse_nested_map(data)?),
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid response frame type.",
      ))
    },
  };

  Ok(value)
}

/// Parse the protocol frame into a redis value, with support for arbitrarily nested arrays.
///
/// Unlike `frame_to_results` this will not unwrap single-element arrays.
pub fn frame_to_results_raw(frame: Resp3Frame) -> Result<RedisValue, RedisError> {
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
      // errors don't have a great way to represent non-utf8 strings...
      let parsed = String::from_utf8_lossy(&data);
      return Err(pretty_error(&parsed));
    },
    Resp3Frame::VerbatimString { data, .. } => string_or_bytes(data),
    Resp3Frame::Number { data, .. } => data.into(),
    Resp3Frame::Double { data, .. } => data.into(),
    Resp3Frame::BigNumber { data, .. } => string_or_bytes(data),
    Resp3Frame::Boolean { data, .. } => data.into(),
    Resp3Frame::Array { data, .. } | Resp3Frame::Push { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_to_results_raw(frame)?);
      }

      RedisValue::Array(out)
    },
    Resp3Frame::Set { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_to_results_raw(frame)?);
      }

      RedisValue::Array(out)
    },
    Resp3Frame::Map { data, .. } => {
      let mut out = HashMap::with_capacity(data.len());
      for (key, value) in data.into_iter() {
        let key: RedisKey = frame_to_single_result(key)?.try_into()?;
        let value = frame_to_results_raw(value)?;

        out.insert(key, value);
      }

      RedisValue::Map(RedisMap { inner: out })
    },
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid response frame type.",
      ))
    },
  };

  Ok(value)
}

/// Parse the protocol frame into a single redis value, returning an error if the result contains nested arrays, an array with more than one value, or any other aggregate type.
///
/// If the array only contains one value then that value will be returned.
///
/// This function is equivalent to [frame_to_results] but with an added validation layer if the result set is a nested array, aggregate type, etc.
pub fn frame_to_single_result(frame: Resp3Frame) -> Result<RedisValue, RedisError> {
  match frame {
    Resp3Frame::SimpleString { data, .. } => {
      let value = string_or_bytes(data);

      if value.as_str().map(|s| s == QUEUED).unwrap_or(false) {
        Ok(RedisValue::Queued)
      } else {
        Ok(value)
      }
    },
    Resp3Frame::SimpleError { data, .. } => Err(pretty_error(&data)),
    Resp3Frame::Number { data, .. } => Ok(data.into()),
    Resp3Frame::Double { data, .. } => Ok(data.into()),
    Resp3Frame::BigNumber { data, .. } => Ok(string_or_bytes(data)),
    Resp3Frame::Boolean { data, .. } => Ok(data.into()),
    Resp3Frame::VerbatimString { data, .. } => Ok(string_or_bytes(data)),
    Resp3Frame::BlobString { data, .. } => Ok(string_or_bytes(data)),
    Resp3Frame::BlobError { data, .. } => {
      // errors don't have a great way to represent non-utf8 strings...
      let parsed = String::from_utf8_lossy(&data);
      Err(pretty_error(&parsed))
    },
    Resp3Frame::Array { mut data, .. } | Resp3Frame::Push { mut data, .. } => {
      if data.len() > 1 {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Could not convert multiple frames to RedisValue.",
        ));
      } else if data.is_empty() {
        return Ok(RedisValue::Null);
      }

      let first_frame = data.pop().unwrap();
      if first_frame.is_array() || first_frame.is_error() {
        // there shouldn't be errors buried in arrays, nor should there be more than one layer of nested arrays
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Invalid nested array or error.",
        ));
      }

      frame_to_single_result(first_frame)
    },
    Resp3Frame::Map { .. } | Resp3Frame::Set { .. } => Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid aggregate type.",
    )),
    Resp3Frame::Null => Ok(RedisValue::Null),
    _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Unexpected frame kind.")),
  }
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
        data: out,
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
        data: out,
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
          RedisErrorKind::ProtocolError,
          "Expected an even number of frames.",
        ));
      }

      let mut inner = HashMap::with_capacity(data.len() / 2);
      while data.len() >= 2 {
        let value = frame_to_results(data.pop().unwrap())?;
        let key = frame_to_single_result(data.pop().unwrap())?.try_into()?;

        inner.insert(key, value);
      }

      Ok(RedisMap { inner })
    },
    Resp3Frame::Map { data, .. } => parse_nested_map(data),
    _ => Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array or map frames.",
    )),
  }
}

pub fn frame_to_error(frame: &Resp3Frame) -> Option<RedisError> {
  match frame {
    Resp3Frame::SimpleError { ref data, .. } => Some(pretty_error(data)),
    Resp3Frame::BlobError { ref data, .. } => {
      let parsed = String::from_utf8_lossy(data);
      Some(pretty_error(&parsed))
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
      data: f.to_string().into(),
      attributes: None,
    },
    RedisValue::Boolean(ref b) => Resp3Frame::BlobString {
      data: b.to_string().into(),
      attributes: None,
    },
    RedisValue::Integer(ref i) => Resp3Frame::BlobString {
      data: i.to_string().into(),
      attributes: None,
    },
    RedisValue::String(ref s) => Resp3Frame::BlobString {
      data: s.inner().clone(),
      attributes: None,
    },
    RedisValue::Bytes(ref b) => Resp3Frame::BlobString {
      data: b.clone(),
      attributes: None,
    },
    RedisValue::Queued => Resp3Frame::BlobString {
      data: Bytes::from_static(QUEUED.as_bytes()),
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

pub fn expect_ok(value: &RedisValue) -> Result<(), RedisError> {
  match *value {
    RedisValue::String(ref resp) => {
      if resp.deref() == OK {
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
      RedisErrorKind::ProtocolError,
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
      RedisErrorKind::ProtocolError,
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
      RedisErrorKind::ProtocolError,
      "Invalid ACL user flags. Expected array.",
    ))
  }
}

fn frames_to_strings(frames: &Resp3Frame) -> Result<Vec<String>, RedisError> {
  if let Resp3Frame::Array { ref data, .. } = frames {
    let mut out = Vec::with_capacity(data.len());

    for frame in data.iter() {
      let val = match frame.as_str() {
        Some(v) => v.to_owned(),
        None => continue,
      };

      out.push(val);
    }

    Ok(out)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array of frames.",
    ))
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
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        format!("Invalid ACL GETUSER field: {}", key),
      ))
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
        data: out,
        attributes: None,
      })
    },
    Resp3Frame::Set { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(frame_map_or_set_to_nested_array(frame)?);
      }

      Ok(Resp3Frame::Array {
        data: out,
        attributes: None,
      })
    },
    _ => Ok(frame),
  }
}

pub fn parse_acl_getuser_frames(frames: Vec<Resp3Frame>) -> Result<AclUser, RedisError> {
  if frames.len() % 2 != 0 || frames.len() > 10 {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError,
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

fn parse_slowlog_entry(frames: Vec<Resp3Frame>) -> Result<SlowlogEntry, RedisError> {
  if frames.len() < 4 {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected at least 4 response frames.",
    ));
  }

  let id = match frames[0] {
    Resp3Frame::Number { ref data, .. } => *data,
    _ => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected integer ID.")),
  };
  let timestamp = match frames[1] {
    Resp3Frame::Number { ref data, .. } => *data,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected integer timestamp.",
      ))
    },
  };
  let duration = match frames[2] {
    Resp3Frame::Number { ref data, .. } => *data as u64,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected integer duration.",
      ))
    },
  };
  let args = match frames[3] {
    Resp3Frame::Array { ref data, .. } => data
      .iter()
      .filter_map(|frame| frame.as_str().map(|s| s.to_owned()))
      .collect(),
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected arguments array.",
      ))
    },
  };

  let (ip, name) = if frames.len() == 6 {
    let ip = match frames[4].as_str() {
      Some(s) => s.to_owned(),
      None => {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected IP address string.",
        ))
      },
    };
    let name = match frames[5].as_str() {
      Some(s) => s.to_owned(),
      None => {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected client name string.",
        ))
      },
    };

    (Some(ip), Some(name))
  } else {
    (None, None)
  };

  Ok(SlowlogEntry {
    id,
    timestamp,
    duration,
    args,
    ip,
    name,
  })
}

pub fn parse_slowlog_entries(frames: Vec<Resp3Frame>) -> Result<Vec<SlowlogEntry>, RedisError> {
  let mut out = Vec::with_capacity(frames.len());

  for frame in frames.into_iter() {
    if let Resp3Frame::Array { data, .. } = frame {
      out.push(parse_slowlog_entry(data)?);
    } else {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected array of slowlog fields.",
      ));
    }
  }

  Ok(out)
}

fn parse_cluster_info_line(info: &mut ClusterInfo, line: &str) -> Result<(), RedisError> {
  let parts: Vec<&str> = line.split(":").collect();
  if parts.len() != 2 {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected key:value pair.",
    ));
  }
  let (field, val) = (parts[0], parts[1]);

  match field.as_ref() {
    "cluster_state" => match val.as_ref() {
      "ok" => info.cluster_state = ClusterState::Ok,
      "fail" => info.cluster_state = ClusterState::Fail,
      _ => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid cluster state.")),
    },
    "cluster_slots_assigned" => info.cluster_slots_assigned = parse_or_zero!(val, u16),
    "cluster_slots_ok" => info.cluster_slots_ok = parse_or_zero!(val, u16),
    "cluster_slots_pfail" => info.cluster_slots_pfail = parse_or_zero!(val, u16),
    "cluster_slots_fail" => info.cluster_slots_fail = parse_or_zero!(val, u16),
    "cluster_known_nodes" => info.cluster_known_nodes = parse_or_zero!(val, u16),
    "cluster_size" => info.cluster_size = parse_or_zero!(val, u32),
    "cluster_current_epoch" => info.cluster_current_epoch = parse_or_zero!(val, u64),
    "cluster_my_epoch" => info.cluster_my_epoch = parse_or_zero!(val, u64),
    "cluster_stats_messages_sent" => info.cluster_stats_messages_sent = parse_or_zero!(val, u64),
    "cluster_stats_messages_received" => info.cluster_stats_messages_received = parse_or_zero!(val, u64),
    _ => {
      warn!("Invalid cluster info field: {}", line);
    },
  };

  Ok(())
}

pub fn parse_cluster_info(data: Resp3Frame) -> Result<ClusterInfo, RedisError> {
  if let Some(data) = data.as_str() {
    let mut out = ClusterInfo::default();

    for line in data.lines().into_iter() {
      let trimmed = line.trim();
      if !trimmed.is_empty() {
        let _ = parse_cluster_info_line(&mut out, trimmed)?;
      }
    }
    Ok(out)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected string response.",
    ))
  }
}

fn frame_to_f64(frame: &Resp3Frame) -> Result<f64, RedisError> {
  match frame {
    Resp3Frame::Double { ref data, .. } => Ok(*data),
    _ => {
      if let Some(s) = frame.as_str() {
        utils::redis_string_to_f64(s)
      } else {
        Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected bulk string or double.",
        ))
      }
    },
  }
}

pub fn parse_geo_position(frame: &Resp3Frame) -> Result<GeoPosition, RedisError> {
  if let Resp3Frame::Array { ref data, .. } = frame {
    if data.len() == 2 {
      let longitude = frame_to_f64(&data[0])?;
      let latitude = frame_to_f64(&data[1])?;

      Ok(GeoPosition { longitude, latitude })
    } else {
      Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected array with 2 coordinates.",
      ))
    }
  } else {
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected array."))
  }
}

fn assert_frame_len(frames: &Vec<Resp3Frame>, len: usize) -> Result<(), RedisError> {
  if frames.len() != len {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      format!("Expected {} frames", len),
    ))
  } else {
    Ok(())
  }
}

fn parse_geo_member(frame: &Resp3Frame) -> Result<RedisValue, RedisError> {
  frame
    .as_str()
    .ok_or(RedisError::new(RedisErrorKind::ProtocolError, "Expected string"))
    .map(|s| s.into())
}

fn parse_geo_dist(frame: &Resp3Frame) -> Result<f64, RedisError> {
  match frame {
    Resp3Frame::Double { ref data, .. } => Ok(*data),
    _ => frame
      .as_str()
      .ok_or(RedisError::new(RedisErrorKind::ProtocolError, "Expected double."))
      .and_then(|s| utils::redis_string_to_f64(s)),
  }
}

fn parse_geo_hash(frame: &Resp3Frame) -> Result<i64, RedisError> {
  if let Resp3Frame::Number { ref data, .. } = frame {
    Ok(*data)
  } else {
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected integer."))
  }
}

pub fn parse_georadius_info(
  frame: &Resp3Frame,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
) -> Result<GeoRadiusInfo, RedisError> {
  if let Resp3Frame::Array { ref data, .. } = frame {
    let mut out = GeoRadiusInfo::default();

    if withcoord && withdist && withhash {
      // 4 elements: member, dist, hash, position
      let _ = assert_frame_len(data, 4)?;

      out.member = parse_geo_member(&data[0])?;
      out.distance = Some(parse_geo_dist(&data[1])?);
      out.hash = Some(parse_geo_hash(&data[2])?);
      out.position = Some(parse_geo_position(&data[3])?);
    } else if withcoord && withdist {
      // 3 elements: member, dist, position
      let _ = assert_frame_len(data, 3)?;

      out.member = parse_geo_member(&data[0])?;
      out.distance = Some(parse_geo_dist(&data[1])?);
      out.position = Some(parse_geo_position(&data[2])?);
    } else if withcoord && withhash {
      // 3 elements: member, hash, position
      let _ = assert_frame_len(data, 3)?;

      out.member = parse_geo_member(&data[0])?;
      out.hash = Some(parse_geo_hash(&data[1])?);
      out.position = Some(parse_geo_position(&data[2])?);
    } else if withdist && withhash {
      // 3 elements: member, dist, hash
      let _ = assert_frame_len(data, 3)?;

      out.member = parse_geo_member(&data[0])?;
      out.distance = Some(parse_geo_dist(&data[1])?);
      out.hash = Some(parse_geo_hash(&data[2])?);
    } else if withcoord {
      // 2 elements: member, position
      let _ = assert_frame_len(data, 2)?;

      out.member = parse_geo_member(&data[0])?;
      out.position = Some(parse_geo_position(&data[1])?);
    } else if withdist {
      // 2 elements: member, dist
      let _ = assert_frame_len(data, 2)?;

      out.member = parse_geo_member(&data[0])?;
      out.distance = Some(parse_geo_dist(&data[1])?);
    } else if withhash {
      // 2 elements: member, hash
      let _ = assert_frame_len(data, 2)?;

      out.member = parse_geo_member(&data[0])?;
      out.hash = Some(parse_geo_hash(&data[1])?);
    }

    Ok(out)
  } else {
    let member: RedisValue = match frame.as_str() {
      Some(s) => s.into(),
      None => {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected string or array of frames.",
        ))
      },
    };

    Ok(GeoRadiusInfo {
      member,
      ..Default::default()
    })
  }
}

pub fn parse_georadius_result(
  frame: Resp3Frame,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
) -> Result<Vec<GeoRadiusInfo>, RedisError> {
  if let Resp3Frame::Array { data, .. } = frame {
    let mut out = Vec::with_capacity(data.len());

    for frame in data.into_iter() {
      out.push(parse_georadius_info(&frame, withcoord, withdist, withhash)?);
    }

    Ok(out)
  } else {
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected array."))
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
            RedisErrorKind::ProtocolError,
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

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
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

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
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
  let auth = if command.args.len() == 2 {
    // has username and password
    let username = match command.args[0].as_bytes_str() {
      Some(username) => username,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid username. Expected string.",
        ));
      },
    };
    let password = match command.args[1].as_bytes_str() {
      Some(password) => password,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid password. Expected string.",
        ));
      },
    };

    Some(Auth { username, password })
  } else if command.args.len() == 1 {
    // just has a password (assume the default user)
    let password = match command.args[0].as_bytes_str() {
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
  match command.kind {
    RedisCommandKind::_Custom(ref kind) => {
      let parts: Vec<&str> = kind.cmd.trim().split(" ").collect();
      let mut bulk_strings = Vec::with_capacity(parts.len() + command.args.len());

      for part in parts.into_iter() {
        bulk_strings.push(Resp3Frame::BlobString {
          data: part.as_bytes().to_vec().into(),
          attributes: None,
        });
      }
      for value in command.args.iter() {
        bulk_strings.push(value_to_outgoing_resp3_frame(value)?);
      }

      Ok(Resp3Frame::Array {
        data: bulk_strings,
        attributes: None,
      })
    },
    RedisCommandKind::Hello(ref version) => serialize_hello(command, version),
    _ => {
      let mut bulk_strings = Vec::with_capacity(command.args.len() + 2);

      bulk_strings.push(Resp3Frame::BlobString {
        data: command.kind.cmd_str().into_inner(),
        attributes: None,
      });

      if let Some(subcommand) = command.kind.subcommand_str() {
        bulk_strings.push(Resp3Frame::BlobString {
          data: Bytes::from_static(subcommand.as_bytes()),
          attributes: None,
        });
      }
      for value in command.args.iter() {
        bulk_strings.push(value_to_outgoing_resp3_frame(value)?);
      }

      Ok(Resp3Frame::Array {
        data: bulk_strings,
        attributes: None,
      })
    },
  }
}

pub fn command_to_resp2_frame(command: &RedisCommand) -> Result<Resp2Frame, RedisError> {
  match command.kind {
    RedisCommandKind::_Custom(ref kind) => {
      let parts: Vec<&str> = kind.cmd.trim().split(" ").collect();
      let mut bulk_strings = Vec::with_capacity(parts.len() + command.args.len());

      for part in parts.into_iter() {
        bulk_strings.push(Resp2Frame::BulkString(part.as_bytes().to_vec().into()));
      }
      for value in command.args.iter() {
        bulk_strings.push(value_to_outgoing_resp2_frame(value)?);
      }

      Ok(Resp2Frame::Array(bulk_strings))
    },
    _ => {
      let mut bulk_strings = Vec::with_capacity(command.args.len() + 2);

      bulk_strings.push(Resp2Frame::BulkString(command.kind.cmd_str().into_inner()));
      if let Some(subcommand) = command.kind.subcommand_str() {
        bulk_strings.push(Resp2Frame::BulkString(Bytes::from_static(subcommand.as_bytes())));
      }
      for value in command.args.iter() {
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
  use std::collections::HashMap;

  fn str_to_f(s: &str) -> Resp3Frame {
    Resp3Frame::SimpleString {
      data: s.to_owned().into(),
      attributes: None,
    }
  }

  fn str_to_bs(s: &str) -> Resp3Frame {
    Resp3Frame::BlobString {
      data: s.to_owned().into(),
      attributes: None,
    }
  }

  fn int_to_f(i: i64) -> Resp3Frame {
    Resp3Frame::Number {
      data: i,
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
        data: vec![
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
      overhead_hashtable_main: 72,
    };
    let mut expected_db = HashMap::new();
    expected_db.insert(0, expected_db_0);
    let expected = MemoryStats {
      peak_allocated: 934192,
      total_allocated: 872040,
      startup_allocated: 809912,
      replication_backlog: 0,
      clients_slaves: 0,
      clients_normal: 20496,
      aof_buffer: 0,
      lua_caches: 0,
      db: expected_db,
      overhead_total: 830480,
      keys_count: 1,
      keys_bytes_per_key: 62128,
      dataset_bytes: 41560,
      dataset_percentage: 66.894157409667969,
      peak_percentage: 93.346977233886719,
      allocator_allocated: 1022640,
      allocator_active: 1241088,
      allocator_resident: 5332992,
      allocator_fragmentation_ratio: 1.2136118412017822,
      allocator_fragmentation_bytes: 218448,
      allocator_rss_ratio: 4.2970294952392578,
      allocator_rss_bytes: 4091904,
      rss_overhead_ratio: 2.0268816947937012,
      rss_overhead_bytes: 5476352,
      fragmentation: 13.007383346557617,
      fragmentation_bytes: 9978328,
    };

    assert_eq!(memory_stats, expected);
  }

  #[test]
  fn should_parse_acl_getuser_response() {
    /*
        127.0.0.1:6379> acl getuser alec
     1) "flags"
     2) 1) "on"
     3) "passwords"
     4) 1) "c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5"
        2) "39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45"
     5) "commands"
     6) "-@all +@sortedset +@geo +config|get"
     7) "keys"
     8) 1) "a"
        2) "b"
        3) "c"
     9) "channels"
    10) 1) "c1"
        2) "c2"
        */

    let input = vec![
      str_to_bs("flags"),
      Resp3Frame::Array {
        data: vec![str_to_bs("on")],
        attributes: None,
      },
      str_to_bs("passwords"),
      Resp3Frame::Array {
        data: vec![
          str_to_bs("c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5"),
          str_to_bs("39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45"),
        ],
        attributes: None,
      },
      str_to_bs("commands"),
      str_to_bs("-@all +@sortedset +@geo +config|get"),
      str_to_bs("keys"),
      Resp3Frame::Array {
        data: vec![str_to_bs("a"), str_to_bs("b"), str_to_bs("c")],
        attributes: None,
      },
      str_to_bs("channels"),
      Resp3Frame::Array {
        data: vec![str_to_bs("c1"), str_to_bs("c2")],
        attributes: None,
      },
    ];
    let actual = parse_acl_getuser_frames(input).unwrap();

    let expected = AclUser {
      flags: vec![AclUserFlag::On],
      passwords: string_vec(vec![
        "c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5",
        "39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45",
      ]),
      commands: string_vec(vec!["-@all", "+@sortedset", "+@geo", "+config|get"]),
      keys: string_vec(vec!["a", "b", "c"]),
      channels: string_vec(vec!["c1", "c2"]),
    };
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_slowlog_entries_redis_3() {
    /*
        redis 127.0.0.1:6379> slowlog get 2
    1) 1) (integer) 14
       2) (integer) 1309448221
       3) (integer) 15
       4) 1) "ping"
    2) 1) (integer) 13
       2) (integer) 1309448128
       3) (integer) 30
       4) 1) "slowlog"
          2) "get"
          3) "100"
    */

    let input = vec![
      Resp3Frame::Array {
        data: vec![
          int_to_f(14),
          int_to_f(1309448221),
          int_to_f(15),
          Resp3Frame::Array {
            data: vec![str_to_bs("ping")],
            attributes: None,
          },
        ],
        attributes: None,
      },
      Resp3Frame::Array {
        data: vec![
          int_to_f(13),
          int_to_f(1309448128),
          int_to_f(30),
          Resp3Frame::Array {
            data: vec![str_to_bs("slowlog"), str_to_bs("get"), str_to_bs("100")],
            attributes: None,
          },
        ],
        attributes: None,
      },
    ];
    let actual = parse_slowlog_entries(input).unwrap();

    let expected = vec![
      SlowlogEntry {
        id: 14,
        timestamp: 1309448221,
        duration: 15,
        args: vec!["ping".into()],
        ip: None,
        name: None,
      },
      SlowlogEntry {
        id: 13,
        timestamp: 1309448128,
        duration: 30,
        args: vec!["slowlog".into(), "get".into(), "100".into()],
        ip: None,
        name: None,
      },
    ];

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_slowlog_entries_redis_4() {
    /*
        redis 127.0.0.1:6379> slowlog get 2
    1) 1) (integer) 14
       2) (integer) 1309448221
       3) (integer) 15
       4) 1) "ping"
       5) "127.0.0.1:58217"
       6) "worker-123"
    2) 1) (integer) 13
       2) (integer) 1309448128
       3) (integer) 30
       4) 1) "slowlog"
          2) "get"
          3) "100"
       5) "127.0.0.1:58217"
       6) "worker-123"
    */

    let input = vec![
      Resp3Frame::Array {
        data: vec![
          int_to_f(14),
          int_to_f(1309448221),
          int_to_f(15),
          Resp3Frame::Array {
            data: vec![str_to_bs("ping")],
            attributes: None,
          },
          str_to_bs("127.0.0.1:58217"),
          str_to_bs("worker-123"),
        ],
        attributes: None,
      },
      Resp3Frame::Array {
        data: vec![
          int_to_f(13),
          int_to_f(1309448128),
          int_to_f(30),
          Resp3Frame::Array {
            data: vec![str_to_bs("slowlog"), str_to_bs("get"), str_to_bs("100")],
            attributes: None,
          },
          str_to_bs("127.0.0.1:58217"),
          str_to_bs("worker-123"),
        ],
        attributes: None,
      },
    ];
    let actual = parse_slowlog_entries(input).unwrap();

    let expected = vec![
      SlowlogEntry {
        id: 14,
        timestamp: 1309448221,
        duration: 15,
        args: vec!["ping".into()],
        ip: Some("127.0.0.1:58217".into()),
        name: Some("worker-123".into()),
      },
      SlowlogEntry {
        id: 13,
        timestamp: 1309448128,
        duration: 30,
        args: vec!["slowlog".into(), "get".into(), "100".into()],
        ip: Some("127.0.0.1:58217".into()),
        name: Some("worker-123".into()),
      },
    ];

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_info() {
    let input = "cluster_state:fail
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:3
cluster_slots_fail:2
cluster_known_nodes:6
cluster_size:3
cluster_current_epoch:6
cluster_my_epoch:2
cluster_stats_messages_sent:1483972
cluster_stats_messages_received:1483968";

    let expected = ClusterInfo {
      cluster_state: ClusterState::Fail,
      cluster_slots_assigned: 16384,
      cluster_slots_ok: 16384,
      cluster_slots_fail: 2,
      cluster_slots_pfail: 3,
      cluster_known_nodes: 6,
      cluster_size: 3,
      cluster_current_epoch: 6,
      cluster_my_epoch: 2,
      cluster_stats_messages_sent: 1483972,
      cluster_stats_messages_received: 1483968,
    };

    let actual = parse_cluster_info(Resp3Frame::BlobString {
      data: input.as_bytes().into(),
      attributes: None,
    })
    .unwrap();
    assert_eq!(actual, expected);
  }
}
