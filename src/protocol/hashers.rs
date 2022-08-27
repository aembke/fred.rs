use crate::error::{RedisError, RedisErrorKind};
use crate::types::RedisValue;
use nom::AsBytes;
use redis_protocol::redis_keyslot;

fn hash_value(value: &RedisValue) -> Option<u16> {
  Some(match value {
    RedisValue::String(s) => redis_keyslot(s.as_bytes()),
    RedisValue::Bytes(b) => redis_keyslot(b.as_bytes()),
    RedisValue::Integer(i) => redis_keyslot(i.to_string().as_bytes()),
    RedisValue::Double(f) => redis_keyslot(f.to_string().as_bytes()),
    RedisValue::Null => redis_keyslot(b"nil"),
    RedisValue::Boolean(b) => redis_keyslot(b.to_string().as_bytes()),
    _ => return None,
  })
}

pub fn read_redis_key(value: &RedisValue) -> Option<&[u8]> {
  match value {
    RedisValue::String(s) => Some(s.as_bytes()),
    RedisValue::Bytes(b) => Some(b.as_bytes()),
    _ => None,
  }
}

fn hash_key(value: &RedisValue) -> Option<u16> {
  read_redis_key(value).map(|k| redis_keyslot(k))
}

/// A cluster hashing policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterHash {
  /// Hash the first string or bytes value in the arguments. (Default)
  FirstKey,
  /// Hash the first argument regardless of type.
  FirstValue,
  /// Use a random node in the cluster.
  Random,
  /// Hash the value with the provided offset in the arguments array.
  Offset(usize),
  /// Provide a custom hash slot value.
  Custom(u16),
}

impl Default for ClusterHash {
  fn default() -> Self {
    ClusterHash::FirstKey
  }
}

impl From<Option<u16>> for ClusterHash {
  fn from(hash_slot: Option<u16>) -> Self {
    match hash_slot {
      Some(slot) => ClusterHash::Custom(slot),
      None => ClusterHash::Random,
    }
  }
}

impl ClusterHash {
  /// Hash the provided arguments.
  pub fn hash(&self, args: &[RedisValue]) -> Option<u16> {
    match self {
      ClusterHash::FirstValue => args.get(0).and_then(|v| hash_value(v)),
      ClusterHash::FirstKey => args.iter().find_map(|v| hash_key(v)),
      ClusterHash::Random => None,
      ClusterHash::Offset(idx) => args.get(idx).and_then(|v| hash_value(v)),
      ClusterHash::Custom(val) => Some(*val),
    }
  }

  /// Find the key to hash with the provided arguments.
  pub fn find_key<'a>(&self, args: &'a [RedisValue]) -> Option<&'a [u8]> {
    match self {
      ClusterHash::FirstValue => args.get(0).and_then(|v| read_redis_key(v)),
      ClusterHash::FirstKey => args.iter().find_map(|v| read_redis_key(v)),
      ClusterHash::Offset(idx) => args.get(idx).and_then(|v| read_redis_key(v)),
      ClusterHash::Random | ClusterHash::Custom(_) => None,
    }
  }
}
