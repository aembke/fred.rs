use crate::error::{RedisError, RedisErrorKind};
use crate::types::{RedisKey, RedisValue, NIL, QUEUED};
use bytes::Bytes;
use bytes_utils::Str;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::str::FromStr;

macro_rules! to_signed_number(
  ($t:ty, $v:expr) => {
    match $v {
      RedisValue::Double(f) => Ok(f as $t),
      RedisValue::Integer(i) => Ok(i as $t),
      RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
      RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
      RedisValue::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          RedisValue::Integer(i) => Ok(i as $t),
          RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
          RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
          _ => Err(RedisError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisError::new_parse("Cannot convert array to number."))
      }
      _ => Err(RedisError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! to_unsigned_number(
  ($t:ty, $v:expr) => {
    match $v {
      RedisValue::Double(f) => if f.is_sign_negative() {
        Err(RedisError::new_parse("Cannot convert from negative number."))
      }else{
        Ok(f as $t)
      },
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
          RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
          RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
          _ => Err(RedisError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisError::new_parse("Cannot convert array to number."))
      },
      RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
      _ => Err(RedisError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl FromRedis for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        to_signed_number!($t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl FromRedis for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        to_unsigned_number!($t, value)
      }
    }
  }
);

/// A trait used to [convert](crate::types::RedisValue::convert) various forms of [RedisValue](crate::types::RedisValue) into different types.
///
/// Note: if callers use [Str](https://docs.rs/bytes-utils/latest/bytes_utils/string/type.Str.html) or [Bytes](https://docs.rs/bytes/latest/bytes/struct.Bytes.html) the underlying data will not be copied or moved, if possible.
pub trait FromRedis: Sized {
  fn from_value(value: RedisValue) -> Result<Self, RedisError>;

  #[doc(hidden)]
  fn from_values(values: Vec<RedisValue>) -> Result<Vec<Self>, RedisError> {
    values.into_iter().map(|v| Self::from_value(v)).collect()
  }

  #[doc(hidden)]
  // FIXME if/when specialization is stable
  fn from_owned_bytes(_: Vec<u8>) -> Option<Vec<Self>> {
    None
  }
  #[doc(hidden)]
  fn is_tuple() -> bool {
    false
  }
}

impl FromRedis for RedisValue {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    Ok(value)
  }
}

impl FromRedis for () {
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

impl FromRedis for u8 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    to_unsigned_number!(u8, value)
  }

  fn from_owned_bytes(d: Vec<u8>) -> Option<Vec<Self>> {
    Some(d)
  }
}

impl_unsigned_number!(u16);
impl_unsigned_number!(u32);
impl_unsigned_number!(u64);
impl_unsigned_number!(u128);
impl_unsigned_number!(usize);

impl FromRedis for String {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    if value.is_null() {
      Err(RedisError::new(
        RedisErrorKind::NotFound,
        "Cannot convert nil response to string.",
      ))
    } else {
      value
        .into_string()
        .ok_or(RedisError::new_parse("Could not convert to string."))
    }
  }
}

impl FromRedis for Str {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    if value.is_null() {
      Err(RedisError::new(
        RedisErrorKind::NotFound,
        "Cannot convert nil response to string.",
      ))
    } else {
      value
        .into_bytes_str()
        .ok_or(RedisError::new_parse("Could not convert to string."))
    }
  }
}

impl FromRedis for f64 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    if value.is_null() {
      Err(RedisError::new(
        RedisErrorKind::NotFound,
        "Cannot convert nil response to double.",
      ))
    } else {
      value
        .as_f64()
        .ok_or(RedisError::new_parse("Could not convert to double."))
    }
  }
}

impl FromRedis for f32 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    if value.is_null() {
      Err(RedisError::new(
        RedisErrorKind::NotFound,
        "Cannot convert nil response to float.",
      ))
    } else {
      value
        .as_f64()
        .map(|f| f as f32)
        .ok_or(RedisError::new_parse("Could not convert to float."))
    }
  }
}

impl FromRedis for bool {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    if value.is_null() {
      Err(RedisError::new(
        RedisErrorKind::NotFound,
        "Cannot convert nil response to bool.",
      ))
    } else {
      value
        .as_bool()
        .ok_or(RedisError::new_parse("Could not convert to bool."))
    }
  }
}

impl<T> FromRedis for Option<T>
where
  T: FromRedis,
{
  fn from_value(value: RedisValue) -> Result<Option<T>, RedisError> {
    if value.is_null() {
      Ok(None)
    } else {
      Ok(Some(T::from_value(value)?))
    }
  }
}

impl FromRedis for Bytes {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value
      .into_bytes()
      .ok_or(RedisError::new_parse("Cannot parse into bytes."))
  }
}

impl<T> FromRedis for Vec<T>
where
  T: FromRedis,
{
  fn from_value(value: RedisValue) -> Result<Vec<T>, RedisError> {
    match value {
      RedisValue::Bytes(bytes) => {
        T::from_owned_bytes(bytes.to_vec()).ok_or(RedisError::new_parse("Cannot convert from bytes"))
      }
      RedisValue::String(string) => {
        // hacky way to check if T is bytes without consuming `string`
        if T::from_owned_bytes(vec![]).is_some() {
          T::from_owned_bytes(string.into_inner().to_vec())
            .ok_or(RedisError::new_parse("Could not convert string to bytes."))
        } else {
          Ok(vec![T::from_value(RedisValue::String(string))?])
        }
      }
      RedisValue::Array(values) => T::from_values(values),
      RedisValue::Map(map) => {
        // not being able to use collect() here is unfortunate
        let out = Vec::with_capacity(map.len() * 2);
        map.inner().into_iter().fold(Ok(out), |out, (key, value)| {
          out.and_then(|mut out| {
            if T::is_tuple() {
              // try to convert to a 2-element tuple since that's a common use case from `HGETALL`, etc
              out.push(T::from_value(RedisValue::Array(vec![key.into(), value]))?);
            } else {
              out.push(T::from_value(key.into())?);
              out.push(T::from_value(value)?);
            }

            Ok(out)
          })
        })
      }
      RedisValue::Null => Ok(vec![]),
      RedisValue::Integer(i) => Ok(vec![T::from_value(RedisValue::Integer(i))?]),
      RedisValue::Double(f) => Ok(vec![T::from_value(RedisValue::Double(f))?]),
      RedisValue::Boolean(b) => Ok(vec![T::from_value(RedisValue::Boolean(b))?]),
      RedisValue::Queued => Ok(vec![T::from_value(RedisValue::from_static_str(QUEUED))?]),
    }
  }
}

impl<K, V, S> FromRedis for HashMap<K, V, S>
where
  K: FromRedisKey + Eq + Hash,
  V: FromRedis,
  S: BuildHasher + Default,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    if value.is_null() {
      return Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to map."));
    }

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
      .map(|(k, v)| Ok((K::from_key(k)?, V::from_value(v)?)))
      .collect()
  }
}

impl<V, S> FromRedis for HashSet<V, S>
where
  V: FromRedis + Hash + Eq,
  S: BuildHasher + Default,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value.into_array().into_iter().map(|v| V::from_value(v)).collect()
  }
}

impl<K, V> FromRedis for BTreeMap<K, V>
where
  K: FromRedisKey + Ord,
  V: FromRedis,
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
      .map(|(k, v)| Ok((K::from_key(k)?, V::from_value(v)?)))
      .collect()
  }
}

impl<V> FromRedis for BTreeSet<V>
where
  V: FromRedis + Ord,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    value.into_array().into_iter().map(|v| V::from_value(v)).collect()
  }
}

// adapted from mitsuhiko
macro_rules! impl_from_redis_tuple {
  () => ();
  ($($name:ident,)+) => (
    #[doc(hidden)]
    impl<$($name: FromRedis),*> FromRedis for ($($name,)*) {
      fn is_tuple() -> bool {
        true
      }

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
    impl_from_redis_peel!($($name,)*);
  )
}

macro_rules! impl_from_redis_peel {
  ($name:ident, $($other:ident,)*) => (impl_from_redis_tuple!($($other,)*);)
}

impl_from_redis_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

macro_rules! impl_from_str_from_redis_key (
  ($t:ty) => {
    impl FromRedisKey for $t {
      fn from_key(value: RedisKey) -> Result<$t, RedisError> {
        value
          .as_str()
          .and_then(|k| k.parse::<$t>().ok())
          .ok_or(RedisError::new_parse("Cannot parse key from bytes."))
      }
    }
  }
);

/// A trait used to convert `RedisKey` values to various types.
pub trait FromRedisKey: Sized {
  fn from_key(value: RedisKey) -> Result<Self, RedisError>;
}

impl_from_str_from_redis_key!(u8);
impl_from_str_from_redis_key!(u16);
impl_from_str_from_redis_key!(u32);
impl_from_str_from_redis_key!(u64);
impl_from_str_from_redis_key!(u128);
impl_from_str_from_redis_key!(usize);
impl_from_str_from_redis_key!(i8);
impl_from_str_from_redis_key!(i16);
impl_from_str_from_redis_key!(i32);
impl_from_str_from_redis_key!(i64);
impl_from_str_from_redis_key!(i128);
impl_from_str_from_redis_key!(isize);
impl_from_str_from_redis_key!(f32);
impl_from_str_from_redis_key!(f64);

impl FromRedisKey for String {
  fn from_key(value: RedisKey) -> Result<Self, RedisError> {
    value
      .into_string()
      .ok_or(RedisError::new_parse("Cannot parse key as string."))
  }
}

impl FromRedisKey for Str {
  fn from_key(value: RedisKey) -> Result<Self, RedisError> {
    Ok(Str::from_inner(value.into_bytes())?)
  }
}

impl FromRedisKey for Vec<u8> {
  fn from_key(value: RedisKey) -> Result<Self, RedisError> {
    Ok(value.into_bytes().to_vec())
  }
}

impl FromRedisKey for Bytes {
  fn from_key(value: RedisKey) -> Result<Self, RedisError> {
    Ok(value.into_bytes())
  }
}
// TODO write tests for this ^^

#[cfg(test)]
mod tests {
  use crate::error::RedisError;
  use crate::types::RedisValue;
  use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

  #[test]
  fn should_convert_null() {
    let _foo: () = RedisValue::Null.convert().unwrap();
  }

  #[test]
  fn should_convert_signed_numeric_types() {
    let _foo: i8 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i8 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i16 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i16 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i32 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i32 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i64 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i64 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i128 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: i128 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: isize = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: isize = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: f32 = RedisValue::String("123.5".into()).convert().unwrap();
    assert_eq!(_foo, 123.5);
    let _foo: f64 = RedisValue::String("123.5".into()).convert().unwrap();
    assert_eq!(_foo, 123.5);
  }

  #[test]
  fn should_convert_unsigned_numeric_types() {
    let _foo: u8 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u8 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u16 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u16 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u32 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u32 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u64 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u64 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u128 = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: u128 = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: usize = RedisValue::String("123".into()).convert().unwrap();
    assert_eq!(_foo, 123);
    let _foo: usize = RedisValue::Integer(123).convert().unwrap();
    assert_eq!(_foo, 123);
  }

  #[test]
  fn should_return_not_found_with_null_scalar_values() {
    let result: Result<u8, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<u16, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<u32, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<u64, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<u128, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<usize, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<i8, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<i16, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<i32, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<i64, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<i128, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<isize, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
  }

  #[test]
  fn should_return_not_found_with_null_strings_and_bools() {
    let result: Result<String, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
    let result: Result<bool, RedisError> = RedisValue::Null.convert();
    assert!(result.unwrap_err().is_not_found());
  }

  #[test]
  fn should_convert_strings() {
    let _foo: String = RedisValue::String("foo".into()).convert().unwrap();
    assert_eq!(_foo, "foo".to_owned());
  }

  #[test]
  fn should_convert_bools() {
    let _foo: bool = RedisValue::Integer(0).convert().unwrap();
    assert_eq!(_foo, false);
    let _foo: bool = RedisValue::Integer(1).convert().unwrap();
    assert_eq!(_foo, true);
    let _foo: bool = RedisValue::String("0".into()).convert().unwrap();
    assert_eq!(_foo, false);
    let _foo: bool = RedisValue::String("1".into()).convert().unwrap();
    assert_eq!(_foo, true);
  }

  #[test]
  fn should_convert_bytes() {
    let _foo: Vec<u8> = RedisValue::Bytes("foo".as_bytes().to_vec().into()).convert().unwrap();
    assert_eq!(_foo, "foo".as_bytes().to_vec());
    let _foo: Vec<u8> = RedisValue::String("foo".into()).convert().unwrap();
    assert_eq!(_foo, "foo".as_bytes().to_vec());
    let _foo: Vec<u8> = RedisValue::Array(vec![102.into(), 111.into(), 111.into()])
      .convert()
      .unwrap();
    assert_eq!(_foo, "foo".as_bytes().to_vec());
  }

  #[test]
  fn should_convert_arrays() {
    let foo: Vec<String> = RedisValue::Array(vec!["a".into(), "b".into()]).convert().unwrap();
    assert_eq!(foo, vec!["a".to_owned(), "b".to_owned()]);
  }

  #[test]
  fn should_convert_hash_maps() {
    let foo: HashMap<String, u16> = RedisValue::Array(vec!["a".into(), 1.into(), "b".into(), 2.into()])
      .convert()
      .unwrap();

    let mut expected = HashMap::new();
    expected.insert("a".to_owned(), 1);
    expected.insert("b".to_owned(), 2);
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_hash_sets() {
    let foo: HashSet<String> = RedisValue::Array(vec!["a".into(), "b".into()]).convert().unwrap();

    let mut expected = HashSet::new();
    expected.insert("a".to_owned());
    expected.insert("b".to_owned());
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_btree_maps() {
    let foo: BTreeMap<String, u16> = RedisValue::Array(vec!["a".into(), 1.into(), "b".into(), 2.into()])
      .convert()
      .unwrap();

    let mut expected = BTreeMap::new();
    expected.insert("a".to_owned(), 1);
    expected.insert("b".to_owned(), 2);
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_btree_sets() {
    let foo: BTreeSet<String> = RedisValue::Array(vec!["a".into(), "b".into()]).convert().unwrap();

    let mut expected = BTreeSet::new();
    expected.insert("a".to_owned());
    expected.insert("b".to_owned());
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_tuples() {
    let foo: (String, i64) = RedisValue::Array(vec!["a".into(), 1.into()]).convert().unwrap();
    assert_eq!(foo, ("a".to_owned(), 1));
  }

  #[test]
  fn should_convert_array_tuples() {
    let foo: Vec<(String, i64)> = RedisValue::Array(vec!["a".into(), 1.into(), "b".into(), 2.into()])
      .convert()
      .unwrap();
    assert_eq!(foo, vec![("a".to_owned(), 1), ("b".to_owned(), 2)]);
  }
}
