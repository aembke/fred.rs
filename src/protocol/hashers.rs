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
      ClusterHash::FirstValue => args.get(0).and_then(|v| hash_value(v)),
      ClusterHash::SecondValue => args.get(1).and_then(|v| hash_value(v)),
      ClusterHash::FirstKey => args.iter().find_map(|v| hash_key(v)),
      ClusterHash::None => None,
      ClusterHash::Offset(idx) => args.get(idx).and_then(|v| hash_value(v)),
      ClusterHash::Custom(val) => val.clone(),
    }
  }

  pub fn find_key<'a>(&self, args: &'a [RedisValue]) -> Option<&'a [u8]> {
    match self {
      ClusterHash::FirstValue => args.get(0).and_then(|v| read_redis_key(v)),
      ClusterHash::SecondValue => args.get(1).and_then(|v| read_redis_key(v)),
      ClusterHash::FirstKey => args.iter().find_map(|v| read_redis_key(v)),
      ClusterHash::Offset(idx) => args.get(idx).and_then(|v| read_redis_key(v)),
      ClusterHash::None | ClusterHash::Custom(_) => None,
    }
  }
}
