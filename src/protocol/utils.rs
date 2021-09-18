use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::protocol::connection::OK;
use crate::protocol::types::*;
use crate::types::Resolve;
use crate::types::*;
use crate::types::{RedisConfig, ServerConfig, QUEUED};
use crate::utils;
use parking_lot::RwLock;
use redis_protocol::resp2::types::{Frame as ProtocolFrame, FrameKind as ProtocolFrameKind};
use std::borrow::Cow;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;

macro_rules! parse_or_zero(
  ($data:ident, $t:ty) => {
    $data.parse::<$t>().ok().unwrap_or(0)
  }
);

#[cfg(feature = "enable-tls")]
pub fn uses_tls(inner: &Arc<RedisClientInner>) -> bool {
  inner.config.read().tls.is_some()
}

#[cfg(not(feature = "enable-tls"))]
pub fn uses_tls(_: &Arc<RedisClientInner>) -> bool {
  false
}

pub fn server_to_parts(server: &Arc<String>) -> Result<(&str, u16), RedisError> {
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

pub fn read_clustered_hosts(config: &RwLock<RedisConfig>) -> Result<Vec<(String, u16)>, RedisError> {
  match config.read().server {
    ServerConfig::Clustered { ref hosts, .. } => Ok(hosts.clone()),
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Invalid redis config. Clustered config expected.",
    )),
  }
}

pub fn read_centralized_domain(config: &RwLock<RedisConfig>) -> Result<String, RedisError> {
  if let ServerConfig::Centralized { ref host, .. } = config.read().server {
    Ok(host.to_owned())
  } else {
    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Expected centralized server config.",
    ))
  }
}

pub async fn read_centralized_addr(inner: &Arc<RedisClientInner>) -> Result<SocketAddr, RedisError> {
  let (host, port) = match inner.config.read().server {
    ServerConfig::Centralized { ref host, ref port, .. } => (host.clone(), *port),
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Expected centralized server config.",
      ));
    }
  };

  inner.resolver.resolve(host, port).await
}

/// Server hostnames/IP addresses can have a cport suffix of the form `@1122` that needs to be removed.
fn remove_cport_suffix(server: String) -> String {
  if let Some(first) = server.split("@").next() {
    return first.to_owned();
  }

  server
}

pub fn binary_search(slots: &Vec<Arc<SlotRange>>, slot: u16) -> Option<Arc<SlotRange>> {
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
      }
    };

    if slot < curr.start {
      high = mid - 1;
    } else if slot > curr.end {
      low = mid + 1;
    } else {
      return Some(curr.clone());
    }
  }

  None
}

pub fn parse_cluster_nodes(status: String) -> Result<HashMap<Arc<String>, Vec<SlotRange>>, RedisError> {
  let mut out: HashMap<Arc<String>, Vec<SlotRange>> = HashMap::new();

  // build out the slot ranges for the primary nodes
  for line in status.lines() {
    let parts: Vec<&str> = line.split(" ").collect();

    if parts.len() < 8 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        format!("Invalid cluster node status line {}.", line),
      ));
    }

    let id = Arc::new(parts[0].to_owned());

    if parts[2].contains("master") {
      let mut slots: Vec<SlotRange> = Vec::new();

      let server = Arc::new(remove_cport_suffix(parts[1].to_owned()));
      for slot in parts[8..].iter() {
        let inner_parts: Vec<&str> = slot.split("-").collect();

        if inner_parts.len() == 1 {
          // looking at an individual slot

          slots.push(SlotRange {
            start: inner_parts[0].parse::<u16>()?,
            end: inner_parts[0].parse::<u16>()?,
            server: server.clone(),
            id: id.clone(),
          });
        } else if inner_parts.len() == 2 {
          // looking at a slot range

          slots.push(SlotRange {
            start: inner_parts[0].parse::<u16>()?,
            end: inner_parts[1].parse::<u16>()?,
            server: server.clone(),
            id: id.clone(),
          });
        } else if inner_parts.len() == 3 {
          // looking at a migrating slot
          continue;
        } else {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            format!("Invalid redis hash slot range: {}", slot),
          ));
        }
      }

      out.insert(server.clone(), slots);
    }
  }

  //add_replica_nodes(&mut out, &status)?;
  out.shrink_to_fit();
  Ok(out)
}

pub fn pretty_error(resp: &str) -> RedisError {
  let kind = {
    let mut parts = resp.split_whitespace();

    match parts.next().unwrap_or("").as_ref() {
      "" => RedisErrorKind::Unknown,
      "ERR" => RedisErrorKind::Unknown,
      "WRONGTYPE" => RedisErrorKind::InvalidArgument,
      "NOAUTH" => RedisErrorKind::Auth,
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

pub fn frame_to_pubsub(frame: ProtocolFrame) -> Result<(String, RedisValue), RedisError> {
  if let Ok((channel, message)) = frame.parse_as_pubsub() {
    Ok((channel, RedisValue::String(message)))
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid pubsub message frame.",
    ))
  }
}

#[cfg(not(feature = "ignore-auth-error"))]
pub fn check_auth_error(frame: ProtocolFrame) -> ProtocolFrame {
  frame
}

#[cfg(feature = "ignore-auth-error")]
pub fn check_auth_error(frame: ProtocolFrame) -> ProtocolFrame {
  let is_auth_error = match frame {
    ProtocolFrame::Error(ref s) => s == "ERR Client sent AUTH, but no password is set",
    _ => false,
  };

  if is_auth_error {
    ProtocolFrame::SimpleString("OK".into())
  } else {
    frame
  }
}

/// Parse the protocol frame into a redis value, with support for arbitrarily nested arrays.
///
/// If the array contains one element then that element will be returned.
pub fn frame_to_results(frame: ProtocolFrame) -> Result<RedisValue, RedisError> {
  let value = match frame {
    ProtocolFrame::SimpleString(s) => {
      if s.as_str() == QUEUED {
        RedisValue::Queued
      } else {
        s.into()
      }
    }
    ProtocolFrame::BulkString(b) => {
      if let Some(s) = str::from_utf8(&b).ok() {
        RedisValue::String(s.to_owned())
      } else {
        RedisValue::Bytes(b)
      }
    }
    ProtocolFrame::Integer(i) => i.into(),
    ProtocolFrame::Null => RedisValue::Null,
    ProtocolFrame::Array(frames) => {
      let mut out = Vec::with_capacity(frames.len());

      for frame in frames.into_iter() {
        out.push(frame_to_results(frame)?);
      }

      if out.len() == 1 {
        out.pop().unwrap()
      } else {
        RedisValue::Array(out)
      }
    }
    ProtocolFrame::Error(s) => return Err(pretty_error(&s)),
  };

  Ok(value)
}

/// Parse the protocol frame into a single redis value, returning an error if the result contains nested arrays or an array with more than one value.
///
/// If the array only contains one value then that value will be returned.
///
/// This function is equivalent to [frame_to_results] but with an added validation layer if the result set is a nested array, etc.
pub fn frame_to_single_result(frame: ProtocolFrame) -> Result<RedisValue, RedisError> {
  match frame {
    ProtocolFrame::SimpleString(s) => {
      if s.as_str() == QUEUED {
        Ok(RedisValue::Queued)
      } else {
        Ok(s.into())
      }
    }
    ProtocolFrame::Integer(i) => Ok(i.into()),
    ProtocolFrame::BulkString(b) => {
      if let Some(s) = str::from_utf8(&b).ok() {
        Ok(RedisValue::String(s.to_owned()))
      } else {
        Ok(RedisValue::Bytes(b))
      }
    }
    ProtocolFrame::Array(mut frames) => {
      if frames.len() > 1 {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Could not convert multiple frames to RedisValue.",
        ));
      } else if frames.is_empty() {
        return Ok(RedisValue::Null);
      }

      let first_frame = frames.pop().unwrap();
      if first_frame.kind() == ProtocolFrameKind::Array || first_frame.kind() == ProtocolFrameKind::Error {
        // there shouldn't be errors buried in arrays, nor should there be more than one layer of nested arrays
        return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid nested array."));
      }

      frame_to_single_result(first_frame)
    }
    ProtocolFrame::Null => Ok(RedisValue::Null),
    ProtocolFrame::Error(s) => Err(pretty_error(&s)),
  }
}

/// Convert a frame to a nested RedisMap.
pub fn frame_to_map(frame: ProtocolFrame) -> Result<RedisMap, RedisError> {
  if let ProtocolFrame::Array(mut frames) = frame {
    if frames.is_empty() {
      return Ok(RedisMap::new());
    }
    if frames.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected an even number of frames.",
      ));
    }

    let mut inner = utils::new_map(frames.len() / 2);
    while frames.len() >= 2 {
      let value = frames.pop().unwrap();
      let key = match frames.pop().unwrap().as_str() {
        Some(k) => k.to_owned(),
        None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected string key.")),
      };
      let value = frame_to_single_result(value)?;

      inner.insert(key, value);
    }

    Ok(RedisMap { inner })
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array of frames.",
    ))
  }
}

/// Convert a redis array value to a redis map.
#[allow(dead_code)]
pub fn array_to_map(data: RedisValue) -> Result<RedisMap, RedisError> {
  if let RedisValue::Array(mut values) = data {
    if values.is_empty() {
      return Ok(RedisMap::new());
    }
    if values.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected an even number of array frames.",
      ));
    }

    let mut inner = utils::new_map(values.len() / 2);
    while values.len() >= 2 {
      let value = values.pop().unwrap();
      let key = match values.pop().unwrap().into_string() {
        Some(k) => k,
        None => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected string key.")),
      };

      inner.insert(key, value);
    }

    Ok(RedisMap { inner })
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array of frames.",
    ))
  }
}

pub fn frame_to_error(frame: &ProtocolFrame) -> Option<RedisError> {
  match frame {
    ProtocolFrame::Error(ref s) => Some(pretty_error(s)),
    _ => None,
  }
}

pub fn value_to_outgoing_frame(value: &RedisValue) -> Result<ProtocolFrame, RedisError> {
  let frame = match value {
    RedisValue::Integer(ref i) => ProtocolFrame::BulkString(i.to_string().into_bytes()),
    RedisValue::String(ref s) => ProtocolFrame::BulkString(s.as_bytes().to_vec()),
    RedisValue::Bytes(ref b) => ProtocolFrame::BulkString(b.to_vec()),
    RedisValue::Queued => ProtocolFrame::BulkString(QUEUED.as_bytes().to_vec()),
    RedisValue::Null => ProtocolFrame::Null,
    // TODO implement when RESP3 support is in redis-protocol
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument,
        format!("Invalid argument type: {}", value.kind()),
      ))
    }
  };

  Ok(frame)
}

pub fn expect_ok(value: &RedisValue) -> Result<(), RedisError> {
  match *value {
    RedisValue::String(ref resp) => {
      if resp == OK {
        Ok(())
      } else {
        Err(RedisError::new(
          RedisErrorKind::Unknown,
          format!("Expected OK, found {}", resp),
        ))
      }
    }
    _ => Err(RedisError::new(
      RedisErrorKind::Unknown,
      format!("Expected OK, found {:?}.", value),
    )),
  }
}

fn parse_u64(val: &ProtocolFrame) -> u64 {
  match *val {
    ProtocolFrame::Integer(i) => {
      if i < 0 {
        0
      } else {
        i as u64
      }
    }
    ProtocolFrame::SimpleString(ref s) => s.parse::<u64>().ok().unwrap_or(0),
    ProtocolFrame::BulkString(ref s) => str::from_utf8(s).ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0),
    _ => 0,
  }
}

fn parse_f64(val: &ProtocolFrame) -> f64 {
  match *val {
    ProtocolFrame::Integer(i) => i as f64,
    ProtocolFrame::SimpleString(ref s) => s.parse::<f64>().ok().unwrap_or(0.0),
    ProtocolFrame::BulkString(ref s) => str::from_utf8(s)
      .ok()
      .and_then(|s| s.parse::<f64>().ok())
      .unwrap_or(0.0),
    _ => 0.0,
  }
}

fn parse_db_memory_stats(data: &Vec<ProtocolFrame>) -> Result<DatabaseMemoryStats, RedisError> {
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
      _ => {}
    };
  }

  Ok(out)
}

fn parse_memory_stat_field(stats: &mut MemoryStats, key: &str, value: &ProtocolFrame) {
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
    _ => {}
  }
}

pub fn parse_memory_stats(data: &Vec<ProtocolFrame>) -> Result<MemoryStats, RedisError> {
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
        ProtocolFrame::Array(ref inner) => inner,
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

fn parse_acl_getuser_flag(value: &ProtocolFrame) -> Result<Vec<AclUserFlag>, RedisError> {
  if let ProtocolFrame::Array(ref frames) = value {
    let mut out = Vec::with_capacity(frames.len());

    for frame in frames.iter() {
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

fn frames_to_strings(frames: &ProtocolFrame) -> Result<Vec<String>, RedisError> {
  if let ProtocolFrame::Array(ref frames) = frames {
    let mut out = Vec::with_capacity(frames.len());

    for frame in frames.iter() {
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

fn parse_acl_getuser_field(user: &mut AclUser, key: &str, value: &ProtocolFrame) -> Result<(), RedisError> {
  match key.as_ref() {
    "passwords" => user.passwords = frames_to_strings(value)?,
    "keys" => user.keys = frames_to_strings(value)?,
    "channels" => user.channels = frames_to_strings(value)?,
    "commands" => {
      if let Some(commands) = value.as_str() {
        user.commands = commands.split(" ").map(|s| s.to_owned()).collect();
      }
    }
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        format!("Invalid ACL GETUSER field: {}", key),
      ))
    }
  };

  Ok(())
}

pub fn parse_acl_getuser_frames(frames: Vec<ProtocolFrame>) -> Result<AclUser, RedisError> {
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

fn parse_slowlog_entry(frames: Vec<ProtocolFrame>) -> Result<SlowlogEntry, RedisError> {
  if frames.len() < 4 {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected at least 4 response frames.",
    ));
  }

  let id = match frames[0] {
    ProtocolFrame::Integer(ref i) => *i,
    _ => return Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected integer ID.")),
  };
  let timestamp = match frames[1] {
    ProtocolFrame::Integer(ref i) => *i,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected integer timestamp.",
      ))
    }
  };
  let duration = match frames[2] {
    ProtocolFrame::Integer(ref i) => *i as u64,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected integer duration.",
      ))
    }
  };
  let args = match frames[3] {
    ProtocolFrame::Array(ref args) => args.iter().filter_map(|a| a.as_str().map(|s| s.to_owned())).collect(),
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected arguments array.",
      ))
    }
  };

  let (ip, name) = if frames.len() == 6 {
    let ip = match frames[4].as_str() {
      Some(s) => s.to_owned(),
      None => {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected IP address string.",
        ))
      }
    };
    let name = match frames[5].as_str() {
      Some(s) => s.to_owned(),
      None => {
        return Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected client name string.",
        ))
      }
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

pub fn parse_slowlog_entries(frames: Vec<ProtocolFrame>) -> Result<Vec<SlowlogEntry>, RedisError> {
  let mut out = Vec::with_capacity(frames.len());

  for frame in frames.into_iter() {
    if let ProtocolFrame::Array(frames) = frame {
      out.push(parse_slowlog_entry(frames)?);
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
    }
  };

  Ok(())
}

pub fn parse_cluster_info(data: ProtocolFrame) -> Result<ClusterInfo, RedisError> {
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

fn frame_to_f64(frame: &ProtocolFrame) -> Result<f64, RedisError> {
  if let Some(s) = frame.as_str() {
    utils::redis_string_to_f64(s)
  } else {
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected bulk string."))
  }
}

pub fn parse_geo_position(frame: &ProtocolFrame) -> Result<GeoPosition, RedisError> {
  if let ProtocolFrame::Array(ref frames) = frame {
    if frames.len() == 2 {
      let longitude = frame_to_f64(&frames[0])?;
      let latitude = frame_to_f64(&frames[1])?;

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

fn assert_frame_len(frames: &Vec<ProtocolFrame>, len: usize) -> Result<(), RedisError> {
  if frames.len() != len {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      format!("Expected {} frames", len),
    ))
  } else {
    Ok(())
  }
}

fn parse_geo_member(frame: &ProtocolFrame) -> Result<RedisValue, RedisError> {
  frame
    .as_str()
    .ok_or(RedisError::new(RedisErrorKind::ProtocolError, "Expected string"))
    .map(|s| s.into())
}

fn parse_geo_dist(frame: &ProtocolFrame) -> Result<f64, RedisError> {
  frame
    .as_str()
    .ok_or(RedisError::new(RedisErrorKind::ProtocolError, "Expected double."))
    .and_then(|s| utils::redis_string_to_f64(s))
}

fn parse_geo_hash(frame: &ProtocolFrame) -> Result<i64, RedisError> {
  if let ProtocolFrame::Integer(ref i) = frame {
    Ok(*i)
  } else {
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected integer."))
  }
}

pub fn parse_georadius_info(
  frame: &ProtocolFrame,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
) -> Result<GeoRadiusInfo, RedisError> {
  if let ProtocolFrame::Array(ref frames) = frame {
    let mut out = GeoRadiusInfo::default();

    if withcoord && withdist && withhash {
      // 4 elements: member, dist, hash, position
      let _ = assert_frame_len(frames, 4)?;

      out.member = parse_geo_member(&frames[0])?;
      out.distance = Some(parse_geo_dist(&frames[1])?);
      out.hash = Some(parse_geo_hash(&frames[2])?);
      out.position = Some(parse_geo_position(&frames[3])?);
    } else if withcoord && withdist {
      // 3 elements: member, dist, position
      let _ = assert_frame_len(frames, 3)?;

      out.member = parse_geo_member(&frames[0])?;
      out.distance = Some(parse_geo_dist(&frames[1])?);
      out.position = Some(parse_geo_position(&frames[2])?);
    } else if withcoord && withhash {
      // 3 elements: member, hash, position
      let _ = assert_frame_len(frames, 3)?;

      out.member = parse_geo_member(&frames[0])?;
      out.hash = Some(parse_geo_hash(&frames[1])?);
      out.position = Some(parse_geo_position(&frames[2])?);
    } else if withdist && withhash {
      // 3 elements: member, dist, hash
      let _ = assert_frame_len(frames, 3)?;

      out.member = parse_geo_member(&frames[0])?;
      out.distance = Some(parse_geo_dist(&frames[1])?);
      out.hash = Some(parse_geo_hash(&frames[2])?);
    } else if withcoord {
      // 2 elements: member, position
      let _ = assert_frame_len(frames, 2)?;

      out.member = parse_geo_member(&frames[0])?;
      out.position = Some(parse_geo_position(&frames[1])?);
    } else if withdist {
      // 2 elements: member, dist
      let _ = assert_frame_len(frames, 2)?;

      out.member = parse_geo_member(&frames[0])?;
      out.distance = Some(parse_geo_dist(&frames[1])?);
    } else if withhash {
      // 2 elements: member, hash
      let _ = assert_frame_len(frames, 2)?;

      out.member = parse_geo_member(&frames[0])?;
      out.hash = Some(parse_geo_hash(&frames[1])?);
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
      }
    };

    Ok(GeoRadiusInfo {
      member,
      ..Default::default()
    })
  }
}

pub fn parse_georadius_result(
  frame: ProtocolFrame,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
) -> Result<Vec<GeoRadiusInfo>, RedisError> {
  if let ProtocolFrame::Array(frames) = frame {
    let mut out = Vec::with_capacity(frames.len());

    for frame in frames.into_iter() {
      out.push(parse_georadius_info(&frame, withcoord, withdist, withhash)?);
    }

    Ok(out)
  } else {
    Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected array."))
  }
}

/// Convert a redis value to an array of (value, score) tuples.
pub fn value_to_zset_result(value: RedisValue) -> Result<Vec<(RedisValue, f64)>, RedisError> {
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
        }
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
    RedisValue::Null => 3,
    RedisValue::Integer(ref i) => i64_size(*i),
    RedisValue::String(ref s) => s.as_bytes().len(),
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
pub fn frame_size(frame: &Frame) -> usize {
  match frame {
    Frame::Integer(ref i) => i64_size(*i),
    Frame::Null => 3,
    Frame::Error(ref s) => s.as_bytes().len(),
    Frame::SimpleString(ref s) => s.as_bytes().len(),
    Frame::BulkString(ref b) => b.len(),
    Frame::Array(ref a) => a.iter().fold(0, |c, f| c + frame_size(f)),
  }
}

#[cfg(any(feature = "blocking-encoding", feature = "partial-tracing", feature = "full-tracing"))]
pub fn args_size(args: &Vec<RedisValue>) -> usize {
  args.iter().fold(0, |c, arg| c + arg_size(arg))
}

pub fn command_to_frame(command: &RedisCommand) -> Result<ProtocolFrame, RedisError> {
  if let RedisCommandKind::_Custom(ref kind) = command.kind {
    let parts: Vec<&str> = kind.cmd.trim().split(" ").collect();
    let mut bulk_strings = Vec::with_capacity(parts.len() + command.args.len());

    for part in parts.into_iter() {
      bulk_strings.push(ProtocolFrame::BulkString(part.as_bytes().to_vec()));
    }
    for value in command.args.iter() {
      bulk_strings.push(value_to_outgoing_frame(value)?);
    }

    Ok(ProtocolFrame::Array(bulk_strings))
  } else {
    let mut bulk_strings = Vec::with_capacity(command.args.len() + 2);

    let cmd = command.kind.cmd_str().as_bytes();
    bulk_strings.push(ProtocolFrame::BulkString(cmd.to_vec()));

    if let Some(subcommand) = command.kind.subcommand_str() {
      bulk_strings.push(ProtocolFrame::BulkString(subcommand.as_bytes().to_vec()));
    }
    for value in command.args.iter() {
      bulk_strings.push(value_to_outgoing_frame(value)?);
    }

    Ok(ProtocolFrame::Array(bulk_strings))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::HashMap;

  fn str_to_f(s: &str) -> ProtocolFrame {
    ProtocolFrame::SimpleString(s.to_owned())
  }

  fn str_to_bs(s: &str) -> ProtocolFrame {
    ProtocolFrame::BulkString(s.as_bytes().to_vec())
  }

  fn int_to_f(i: i64) -> ProtocolFrame {
    ProtocolFrame::Integer(i)
  }

  fn string_vec(d: Vec<&str>) -> Vec<String> {
    d.into_iter().map(|s| s.to_owned()).collect()
  }

  #[test]
  fn should_parse_memory_stats() {
    // better from()/into() interfaces for frames coming in the next redis-protocol version...
    let frames: Vec<ProtocolFrame> = vec![
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
      ProtocolFrame::Array(vec![
        str_to_f("overhead.hashtable.main"),
        int_to_f(72),
        str_to_f("overhead.hashtable.expires"),
        int_to_f(0),
      ]),
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
      ProtocolFrame::Array(vec![str_to_bs("on")]),
      str_to_bs("passwords"),
      ProtocolFrame::Array(vec![
        str_to_bs("c56e8629954a900e993e84ed3d4b134b9450da1b411a711d047d547808c3ece5"),
        str_to_bs("39b039a94deaa548cf6382282c4591eccdc648706f9d608eceb687d452a31a45"),
      ]),
      str_to_bs("commands"),
      str_to_bs("-@all +@sortedset +@geo +config|get"),
      str_to_bs("keys"),
      ProtocolFrame::Array(vec![str_to_bs("a"), str_to_bs("b"), str_to_bs("c")]),
      str_to_bs("channels"),
      ProtocolFrame::Array(vec![str_to_bs("c1"), str_to_bs("c2")]),
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
      ProtocolFrame::Array(vec![
        int_to_f(14),
        int_to_f(1309448221),
        int_to_f(15),
        ProtocolFrame::Array(vec![str_to_bs("ping")]),
      ]),
      ProtocolFrame::Array(vec![
        int_to_f(13),
        int_to_f(1309448128),
        int_to_f(30),
        ProtocolFrame::Array(vec![str_to_bs("slowlog"), str_to_bs("get"), str_to_bs("100")]),
      ]),
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
      ProtocolFrame::Array(vec![
        int_to_f(14),
        int_to_f(1309448221),
        int_to_f(15),
        ProtocolFrame::Array(vec![str_to_bs("ping")]),
        str_to_bs("127.0.0.1:58217"),
        str_to_bs("worker-123"),
      ]),
      ProtocolFrame::Array(vec![
        int_to_f(13),
        int_to_f(1309448128),
        int_to_f(30),
        ProtocolFrame::Array(vec![str_to_bs("slowlog"), str_to_bs("get"), str_to_bs("100")]),
        str_to_bs("127.0.0.1:58217"),
        str_to_bs("worker-123"),
      ]),
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

    let actual = parse_cluster_info(ProtocolFrame::BulkString(input.as_bytes().to_vec())).unwrap();
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_node_status_individual_slot() {
    let status = "2edc9a62355eacff9376c4e09643e2c932b0356a foo.use2.cache.amazonaws.com:6379@1122 master - 0 1565908731456 2950 connected 1242-1696 8195-8245 8247-8423 10923-12287
db2fd89f83daa5fe49110ef760794f9ccee07d06 bar.use2.cache.amazonaws.com:6379@1122 master - 0 1565908731000 2952 connected 332-1241 8152-8194 8424-8439 9203-10112 12288-12346 12576-12685
d9aeabb1525e5656c98545a0ed42c8c99bbacae1 baz.use2.cache.amazonaws.com:6379@1122 master - 0 1565908729402 2956 connected 1697 1815-2291 3657-4089 5861-6770 7531-7713 13154-13197
5671f02def98d0279224f717aba0f95874e5fb89 wibble.use2.cache.amazonaws.com:6379@1122 master - 0 1565908728391 2953 connected 7900-8125 12427 13198-13760 15126-16383
0b1923e386f6f6f3adc1b6deb250ef08f937e9b5 wobble.use2.cache.amazonaws.com:6379@1122 master - 0 1565908731000 2954 connected 5462-5860 6771-7382 8133-8151 10113-10922 12686-12893
1c5d99e3d6fca2090d0903d61d4e51594f6dcc05 qux.use2.cache.amazonaws.com:6379@1122 master - 0 1565908732462 2949 connected 2292-3656 7383-7530 8896-9202 12347-12426 12428-12575
b8553a4fae8ae99fca716d423b14875ebb10fefe quux.use2.cache.amazonaws.com:6379@1122 master - 0 1565908730439 2951 connected 8246 8440-8895 12919-13144 13761-15125
4a58ba550f37208c9a9909986ce808cdb058e31f quuz.use2.cache.amazonaws.com:6379@1122 myself,master - 0 1565908730000 2955 connected 0-331 1698-1814 4090-5461 7714-7899 8126-8132 12894-12918 13145-13153";

    let mut expected: HashMap<Arc<String>, Vec<SlotRange>> = HashMap::new();
    expected.insert(
      Arc::new("foo.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 1242,
          end: 1696,
          server: Arc::new("foo.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("2edc9a62355eacff9376c4e09643e2c932b0356a".into()),
        },
        SlotRange {
          start: 8195,
          end: 8245,
          server: Arc::new("foo.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("2edc9a62355eacff9376c4e09643e2c932b0356a".into()),
        },
        SlotRange {
          start: 8247,
          end: 8423,
          server: Arc::new("foo.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("2edc9a62355eacff9376c4e09643e2c932b0356a".into()),
        },
        SlotRange {
          start: 10923,
          end: 12287,
          server: Arc::new("foo.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("2edc9a62355eacff9376c4e09643e2c932b0356a".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 332,
          end: 1241,
          server: Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("db2fd89f83daa5fe49110ef760794f9ccee07d06".into()),
        },
        SlotRange {
          start: 8152,
          end: 8194,
          server: Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("db2fd89f83daa5fe49110ef760794f9ccee07d06".into()),
        },
        SlotRange {
          start: 8424,
          end: 8439,
          server: Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("db2fd89f83daa5fe49110ef760794f9ccee07d06".into()),
        },
        SlotRange {
          start: 9203,
          end: 10112,
          server: Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("db2fd89f83daa5fe49110ef760794f9ccee07d06".into()),
        },
        SlotRange {
          start: 12288,
          end: 12346,
          server: Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("db2fd89f83daa5fe49110ef760794f9ccee07d06".into()),
        },
        SlotRange {
          start: 12576,
          end: 12685,
          server: Arc::new("bar.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("db2fd89f83daa5fe49110ef760794f9ccee07d06".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 1697,
          end: 1697,
          server: Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into()),
        },
        SlotRange {
          start: 1815,
          end: 2291,
          server: Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into()),
        },
        SlotRange {
          start: 3657,
          end: 4089,
          server: Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into()),
        },
        SlotRange {
          start: 5861,
          end: 6770,
          server: Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into()),
        },
        SlotRange {
          start: 7531,
          end: 7713,
          server: Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into()),
        },
        SlotRange {
          start: 13154,
          end: 13197,
          server: Arc::new("baz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("d9aeabb1525e5656c98545a0ed42c8c99bbacae1".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("wibble.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 7900,
          end: 8125,
          server: Arc::new("wibble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("5671f02def98d0279224f717aba0f95874e5fb89".into()),
        },
        SlotRange {
          start: 12427,
          end: 12427,
          server: Arc::new("wibble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("5671f02def98d0279224f717aba0f95874e5fb89".into()),
        },
        SlotRange {
          start: 13198,
          end: 13760,
          server: Arc::new("wibble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("5671f02def98d0279224f717aba0f95874e5fb89".into()),
        },
        SlotRange {
          start: 15126,
          end: 16383,
          server: Arc::new("wibble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("5671f02def98d0279224f717aba0f95874e5fb89".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("wobble.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 5462,
          end: 5860,
          server: Arc::new("wobble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into()),
        },
        SlotRange {
          start: 6771,
          end: 7382,
          server: Arc::new("wobble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into()),
        },
        SlotRange {
          start: 8133,
          end: 8151,
          server: Arc::new("wobble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into()),
        },
        SlotRange {
          start: 10113,
          end: 10922,
          server: Arc::new("wobble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into()),
        },
        SlotRange {
          start: 12686,
          end: 12893,
          server: Arc::new("wobble.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("0b1923e386f6f6f3adc1b6deb250ef08f937e9b5".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("qux.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 2292,
          end: 3656,
          server: Arc::new("qux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into()),
        },
        SlotRange {
          start: 7383,
          end: 7530,
          server: Arc::new("qux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into()),
        },
        SlotRange {
          start: 8896,
          end: 9202,
          server: Arc::new("qux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into()),
        },
        SlotRange {
          start: 12347,
          end: 12426,
          server: Arc::new("qux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into()),
        },
        SlotRange {
          start: 12428,
          end: 12575,
          server: Arc::new("qux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("1c5d99e3d6fca2090d0903d61d4e51594f6dcc05".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("quux.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 8246,
          end: 8246,
          server: Arc::new("quux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("b8553a4fae8ae99fca716d423b14875ebb10fefe".into()),
        },
        SlotRange {
          start: 8440,
          end: 8895,
          server: Arc::new("quux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("b8553a4fae8ae99fca716d423b14875ebb10fefe".into()),
        },
        SlotRange {
          start: 12919,
          end: 13144,
          server: Arc::new("quux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("b8553a4fae8ae99fca716d423b14875ebb10fefe".into()),
        },
        SlotRange {
          start: 13761,
          end: 15125,
          server: Arc::new("quux.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("b8553a4fae8ae99fca716d423b14875ebb10fefe".into()),
        },
      ],
    );
    expected.insert(
      Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
      vec![
        SlotRange {
          start: 0,
          end: 331,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
        SlotRange {
          start: 1698,
          end: 1814,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
        SlotRange {
          start: 4090,
          end: 5461,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
        SlotRange {
          start: 7714,
          end: 7899,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
        SlotRange {
          start: 8126,
          end: 8132,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
        SlotRange {
          start: 12894,
          end: 12918,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
        SlotRange {
          start: 13145,
          end: 13153,
          server: Arc::new("quuz.use2.cache.amazonaws.com:6379".into()),
          id: Arc::new("4a58ba550f37208c9a9909986ce808cdb058e31f".into()),
        },
      ],
    );

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e),
    };
    assert_eq!(actual, expected);

    let cache = ClusterKeyCache::new(Some(status.to_owned())).expect("Failed to build cluster cache");
    let slot = cache.get_server(8246).unwrap();
    assert_eq!(slot.server.as_str(), "quux.use2.cache.amazonaws.com:6379");
    let slot = cache.get_server(1697).unwrap();
    assert_eq!(slot.server.as_str(), "baz.use2.cache.amazonaws.com:6379");
    let slot = cache.get_server(12427).unwrap();
    assert_eq!(slot.server.as_str(), "wibble.use2.cache.amazonaws.com:6379");
    let slot = cache.get_server(8445).unwrap();
    assert_eq!(slot.server.as_str(), "quux.use2.cache.amazonaws.com:6379");
  }

  #[test]
  fn should_parse_cluster_node_status() {
    let status = "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460";

    let mut expected: HashMap<Arc<String>, Vec<SlotRange>> = HashMap::new();
    expected.insert(
      Arc::new("127.0.0.1:30002".to_owned()),
      vec![SlotRange {
        start: 5461,
        end: 10922,
        server: Arc::new("127.0.0.1:30002".to_owned()),
        id: Arc::new("67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1".to_owned()),
      }],
    );
    expected.insert(
      Arc::new("127.0.0.1:30003".to_owned()),
      vec![SlotRange {
        start: 10923,
        end: 16383,
        server: Arc::new("127.0.0.1:30003".to_owned()),
        id: Arc::new("292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f".to_owned()),
      }],
    );
    expected.insert(
      Arc::new("127.0.0.1:30001".to_owned()),
      vec![SlotRange {
        start: 0,
        end: 5460,
        server: Arc::new("127.0.0.1:30001".to_owned()),
        id: Arc::new("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca".to_owned()),
      }],
    );

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e),
    };
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_elasticache_cluster_node_status() {
    let status =
      "eec2b077ee95c590279115aac13e7eefdce61dba foo.cache.amazonaws.com:6379@1122 master - 0 1530900241038 0 connected 5462-10922
b4fa5337b58e02673f961e22c9557e81dda4b559 bar.cache.amazonaws.com:6379@1122 myself,master - 0 1530900240000 1 connected 0-5461
29d37b842d1bb097ba491be8f1cb00648620d4bd baz.cache.amazonaws.com:6379@1122 master - 0 1530900242042 2 connected 10923-16383";

    let mut expected: HashMap<Arc<String>, Vec<SlotRange>> = HashMap::new();
    expected.insert(
      Arc::new("foo.cache.amazonaws.com:6379".to_owned()),
      vec![SlotRange {
        start: 5462,
        end: 10922,
        server: Arc::new("foo.cache.amazonaws.com:6379".to_owned()),
        id: Arc::new("eec2b077ee95c590279115aac13e7eefdce61dba".to_owned()),
      }],
    );
    expected.insert(
      Arc::new("bar.cache.amazonaws.com:6379".to_owned()),
      vec![SlotRange {
        start: 0,
        end: 5461,
        server: Arc::new("bar.cache.amazonaws.com:6379".to_owned()),
        id: Arc::new("b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned()),
      }],
    );
    expected.insert(
      Arc::new("baz.cache.amazonaws.com:6379".to_owned()),
      vec![SlotRange {
        start: 10923,
        end: 16383,
        server: Arc::new("baz.cache.amazonaws.com:6379".to_owned()),
        id: Arc::new("29d37b842d1bb097ba491be8f1cb00648620d4bd".to_owned()),
      }],
    );

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e),
    };
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_migrating_cluster_nodes() {
    let status = "eec2b077ee95c590279115aac13e7eefdce61dba foo.cache.amazonaws.com:6379@1122 master - 0 1530900241038 0 connected 5462-10921 [10922->-b4fa5337b58e02673f961e22c9557e81dda4b559]
b4fa5337b58e02673f961e22c9557e81dda4b559 bar.cache.amazonaws.com:6379@1122 myself,master - 0 1530900240000 1 connected 0-5461 10922 [10922-<-eec2b077ee95c590279115aac13e7eefdce61dba]
29d37b842d1bb097ba491be8f1cb00648620d4bd baz.cache.amazonaws.com:6379@1122 master - 0 1530900242042 2 connected 10923-16383";

    let mut expected: HashMap<Arc<String>, Vec<SlotRange>> = HashMap::new();
    expected.insert(
      Arc::new("foo.cache.amazonaws.com:6379".to_owned()),
      vec![SlotRange {
        start: 5462,
        end: 10921,
        server: Arc::new("foo.cache.amazonaws.com:6379".to_owned()),
        id: Arc::new("eec2b077ee95c590279115aac13e7eefdce61dba".to_owned()),
      }],
    );
    expected.insert(
      Arc::new("bar.cache.amazonaws.com:6379".to_owned()),
      vec![
        SlotRange {
          start: 0,
          end: 5461,
          server: Arc::new("bar.cache.amazonaws.com:6379".to_owned()),
          id: Arc::new("b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned()),
        },
        SlotRange {
          start: 10922,
          end: 10922,
          server: Arc::new("bar.cache.amazonaws.com:6379".to_owned()),
          id: Arc::new("b4fa5337b58e02673f961e22c9557e81dda4b559".to_owned()),
        },
      ],
    );
    expected.insert(
      Arc::new("baz.cache.amazonaws.com:6379".to_owned()),
      vec![SlotRange {
        start: 10923,
        end: 16383,
        server: Arc::new("baz.cache.amazonaws.com:6379".to_owned()),
        id: Arc::new("29d37b842d1bb097ba491be8f1cb00648620d4bd".to_owned()),
      }],
    );

    let actual = match parse_cluster_nodes(status.to_owned()) {
      Ok(h) => h,
      Err(e) => panic!("{}", e),
    };
    assert_eq!(actual, expected);
  }
}
