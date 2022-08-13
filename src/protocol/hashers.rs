use crate::error::{RedisError, RedisErrorKind};
use crate::types::RedisValue;
use nom::AsBytes;
use redis_protocol::redis_keyslot;
use std::sync::Arc;

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

fn hash_key(value: &RedisValue) -> Option<u16> {
  Some(match value {
    RedisValue::String(s) => redis_keyslot(s.as_bytes()),
    RedisValue::Bytes(b) => redis_keyslot(b.as_bytes()),
    _ => return None,
  })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterHash {
  /// Hash the first argument regardless of type.
  FirstValue,
  /// Hash the second argument regardless of type.
  SecondValue,
  /// Hash the first string or bytes value in the arguments.
  FirstKey,
  /// Use a random node in the cluster.
  None,
  /// Hash the value with the provided offset in the arguments array.
  Offset(usize),
  /// Provide a custom hash slot value.
  Custom(Option<u16>),
}

impl ClusterHash {
  pub fn hash(&self, args: &[RedisValue]) -> Option<u16> {
    match self {
      ClusterHash::FirstValue => args.get(0).map(|v| hash_value(v)),
      ClusterHash::SecondValue => args.get(1).map(|v| hash_value(v)),
      ClusterHash::FirstKey => args.iter().find_map(|v| hash_key(v)),
      ClusterHash::None => None,
      ClusterHash::Offset(idx) => args.get(idx).map(|v| hash_value(v)),
      ClusterHash::Custom(val) => val.clone(),
    }
  }
}
