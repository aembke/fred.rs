use crate::error::RedisError;
use crate::types::RedisValue;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::str::FromStr;

macro_rules! to_signed_number(
  ($t:ty, $v:expr) => {
    match $v {
      RedisValue::Integer(i) => Ok(i as $t),
      RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
      RedisValue::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          RedisValue::Integer(i) => Ok(i as $t),
          RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
          _ => Err(RedisError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisError::new_parse("Cannot convert to number."))
      }
      _ => Err(RedisError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! to_unsigned_number(
  ($t:ty, $v:expr) => {
    match $v {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new_parse("Cannot convert from negative number."))
      }else{
        Ok(i as $t)
      },
      RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
      RedisValue::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          RedisValue::Integer(i) => if i < 0 {
            Err(RedisError::new_parse("Cannot convert from negative number."))
          }else{
            Ok(i as $t)
          },
          RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
          _ => Err(RedisError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisError::new_parse("Cannot convert to number."))
      }
      _ => Err(RedisError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl RedisResponse for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        to_signed_number!($t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl RedisResponse for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        to_unsigned_number!($t, value)
      }
    }
  }
);

pub trait RedisResponse: Sized {
  fn from_value(value: RedisValue) -> Result<Self, RedisError>;

  #[doc(hidden)]
  fn from_values(values: Vec<RedisValue>) -> Result<Vec<Self>, RedisError> {
    values.into_iter().map(|v| Self::from_value(v)).collect()
  }

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
      RedisValue::Array(values) => T::from_values(values),
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

// adapted from mitsuhiko
// this seems much better than making callers deal with hlists
macro_rules! impl_redis_response_tuple {
  () => ();
  ($($name:ident,)+) => (
    impl<$($name: RedisResponse),*> RedisResponse for ($($name,)*) {
      #[allow(non_snake_case, unused_variables)]
      fn from_value(v: RedisValue) -> Result<($($name,)*), RedisError> {
        if let RedisValue::Array(mut values) = v {
          let mut n = 0;
          $(let $name = (); n += 1;)*
          if values.len() != n {
            return Err(RedisError::new_parse("Invalid tuple dimension."));
          }

          // since we have ownership over the values we have some freedom in how to implement this
          values.reverse();
          Ok(($({let $name = (); values
            .pop()
            .ok_or(RedisError::new_parse("Expected value, found none."))?
            .convert()?
          },)*))
        }else{
          Err(RedisError::new_parse("Could not convert to tuple."))
        }
      }

      #[allow(non_snake_case, unused_variables)]
      fn from_values(mut values: Vec<RedisValue>) -> Result<Vec<($($name,)*)>, RedisError> {
        let mut n = 0;
        $(let $name = (); n += 1;)*
        if values.len() % n != 0 {
          return Err(RedisError::new_parse("Invalid tuple dimension."))
        }

        let mut out = Vec::with_capacity(values.len() / n);
        // this would be cleaner if there were an owned `chunks` variant
        for chunk in values.chunks_exact_mut(n) {
          match chunk {
            [$($name),*] => out.push(($($name.take().convert()?),*),),
             _ => unreachable!(),
          }
        }

        Ok(out)
      }
    }
    impl_redis_response_peel!($($name,)*);
  )
}

macro_rules! impl_redis_response_peel {
  ($name:ident, $($other:ident,)*) => (impl_redis_response_tuple!($($other,)*);)
}

impl_redis_response_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

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

  #[test]
  fn should_convert_tuples() {}
}
