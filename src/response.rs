use crate::error::{RedisError, RedisErrorKind};
use crate::types::RedisValue;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::str::FromStr;

pub trait RedisResponse: Sized {
  fn from_value(value: RedisValue) -> Result<Self, RedisError>;

  #[doc(hidden)]
  // FIXME if/when specialization is stable
  fn from_bytes(_: Vec<u8>) -> Option<Vec<Self>> {
    None
  }
}

impl RedisResponse for RedisValue {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    Ok(value)
  }
}

impl RedisResponse for () {
  fn from_value(_: RedisValue) -> Result<Self, RedisError> {
    Ok(())
  }
}

impl_signed_number!(i8);
impl_signed_number!(i16);
impl_signed_number!(i32);
impl_signed_number!(i64);
impl_signed_number!(i128);
impl_signed_number!(isize);

impl RedisResponse for u8 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    to_unsigned_number!(u8, value)
  }

  fn from_bytes(v: Vec<u8>) -> Option<Vec<u8>> {
    Some(v)
  }
}

impl_unsigned_number!(u16);
impl_unsigned_number!(u32);
impl_unsigned_number!(u64);
impl_unsigned_number!(u128);
impl_unsigned_number!(usize);

impl RedisResponse for String {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value
      .into_string()
      .ok_or(RedisError::new_parse("Could not convert to string."))
  }
}

impl RedisResponse for f64 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value
      .as_f64()
      .ok_or(RedisError::new_parse("Could not convert to double."))
  }
}

impl RedisResponse for f32 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value
      .as_f64()
      .map(|f| f as f32)
      .ok_or(RedisError::new_parse("Could not convert to float."))
  }
}

impl RedisResponse for bool {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value
      .as_bool()
      .ok_or(RedisError::new_parse("Could not convert to bool."))
  }
}

impl<T> RedisResponse for Option<T>
where
  T: RedisResponse,
{
  fn from_value(value: RedisValue) -> Result<Option<T>, RedisError> {
    if value.is_null() {
      Ok(None)
    } else {
      Ok(Some(T::from_value(value)?))
    }
  }
}

impl<T> RedisResponse for Vec<T>
where
  T: RedisResponse,
{
  fn from_value(value: RedisValue) -> Result<Vec<T>, RedisError> {
    match value {
      RedisValue::Bytes(b) => T::from_bytes(b).ok_or(RedisError::new_parse("Cannot convert to bytes")),
      RedisValue::Array(values) => values.into_iter().map(|v| T::from_value(v)).collect(),
      RedisValue::Null => Ok(vec![]),
      _ => Err(RedisError::new_parse("Cannot convert to array.")),
    }
  }
}

impl<K, V, S> RedisResponse for HashMap<K, V, S>
where
  K: FromStr + Eq + Hash,
  V: RedisResponse,
  S: BuildHasher + Default,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    let as_map = if value.is_array() || value.is_map() {
      value
        .into_map()
        .map_err(|_| RedisError::new_parse("Cannot convert to map."))?
    } else {
      return Err(RedisError::new_parse("Cannot convert to map."));
    };

    as_map
      .inner()
      .into_iter()
      .map(|(k, v)| {
        Ok((
          k.parse::<K>()
            .map_err(|_| RedisError::new_parse("Cannot convert key."))?,
          V::from_value(v)?,
        ))
      })
      .collect()
  }
}

impl<V, S> RedisResponse for HashSet<V, S>
where
  V: RedisResponse + Hash + Eq,
  S: BuildHasher + Default,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value.into_array().into_iter().map(|v| V::from_value(v)).collect()
  }
}

impl<K, V> RedisResponse for BTreeMap<K, V>
where
  K: FromStr + Ord,
  V: RedisResponse,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    let as_map = if value.is_array() || value.is_map() {
      value
        .into_map()
        .map_err(|_| RedisError::new_parse("Cannot convert to map."))?
    } else {
      return Err(RedisError::new_parse("Cannot convert to map."));
    };

    as_map
      .inner()
      .into_iter()
      .map(|(k, v)| {
        Ok((
          k.parse::<K>()
            .map_err(|_| RedisError::new_parse("Cannot convert key."))?,
          V::from_value(v)?,
        ))
      })
      .collect()
  }
}

impl<V> RedisResponse for BTreeSet<V>
where
  V: RedisResponse + Ord,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value.into_array().into_iter().map(|v| V::from_value(v)).collect()
  }
}

#[cfg(test)]
mod tests {

  #[test]
  fn should_convert_signed_numeric_types() {}

  #[test]
  fn should_convert_unsigned_numeric_types() {}

  #[test]
  fn should_convert_strings() {}

  #[test]
  fn should_convert_bools() {}

  #[test]
  fn should_convert_bytes() {}

  #[test]
  fn should_convert_arrays() {}
}
