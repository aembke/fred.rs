use crate::{
  error::{RedisError, RedisErrorKind},
  types::{GeoPosition, RedisKey, RedisValue, QUEUED},
};
use bytes::Bytes;
use bytes_utils::Str;
use std::{
  collections::{BTreeMap, BTreeSet, HashMap, HashSet},
  hash::{BuildHasher, Hash},
};

#[allow(unused_imports)]
use std::any::type_name;

#[cfg(feature = "default-nil-types")]
use crate::types::NIL;
#[cfg(any(feature = "default-nil-types", feature = "serde-json"))]
use crate::utils;
#[cfg(feature = "serde-json")]
use serde_json::{Map, Value};

macro_rules! debug_type(
  ($($arg:tt)*) => {
    #[cfg(feature="network-logs")]
    log::trace!($($arg)*);
  }
);

macro_rules! check_loose_nil (
  ($v:expr, $o:expr) => {
    #[cfg(feature = "default-nil-types")]
    {
      if $v.is_null() {
        return Ok($o);
      }
    }
  }
);

macro_rules! check_single_bulk_reply(
  ($v:expr) => {
    if $v.is_single_element_vec() {
      return Self::from_value($v.pop_or_take());
    }
  };
  ($t:ty, $v:expr) => {
    if $v.is_single_element_vec() {
      return $t::from_value($v.pop_or_take());
    }
  }
);

macro_rules! to_signed_number(
  ($t:ty, $v:expr) => {
    match $v {
      RedisValue::Double(f) => Ok(f as $t),
      RedisValue::Integer(i) => Ok(i as $t),
      RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
      RedisValue::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          RedisValue::Integer(i) => Ok(i as $t),
          RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
          #[cfg(feature = "default-nil-types")]
          RedisValue::Null => Ok(0),
          #[cfg(not(feature = "default-nil-types"))]
          RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
          _ => Err(RedisError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisError::new_parse("Cannot convert array to number."))
      }
      #[cfg(feature = "default-nil-types")]
      RedisValue::Null => Ok(0),
      #[cfg(not(feature = "default-nil-types"))]
      RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
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
          #[cfg(feature = "default-nil-types")]
          RedisValue::Null => Ok(0),
          #[cfg(not(feature = "default-nil-types"))]
          RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
          RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
          _ => Err(RedisError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisError::new_parse("Cannot convert array to number."))
      },
      #[cfg(feature = "default-nil-types")]
      RedisValue::Null => Ok(0),
      #[cfg(not(feature = "default-nil-types"))]
      RedisValue::Null => Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to number.")),
      _ => Err(RedisError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl FromRedis for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        check_single_bulk_reply!(value);
        to_signed_number!($t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl FromRedis for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        check_single_bulk_reply!(value);
        to_unsigned_number!($t, value)
      }
    }
  }
);

/// A trait used to [convert](RedisValue::convert) various forms of [RedisValue](RedisValue) into different types.
///
/// These type conversion patterns can be used interchangeably:
///
/// ```rust
/// # use fred::prelude::*;
/// // this option is by far the most common
/// let foo: i64 = RedisValue::Integer(42).convert()?;
/// let foo = RedisValue::Integer(42).convert::<i64>()?;
/// let foo = i64::from_value(RedisValue::Integer(42))?;
/// ```
///
/// ## Examples
///
/// ```rust
/// # use fred::types::RedisValue;
/// # use std::collections::HashMap;
/// let foo: usize = RedisValue::String("123".into()).convert()?;
/// let foo: i64 = RedisValue::String("123".into()).convert()?;
/// let foo: String = RedisValue::String("123".into()).convert()?;
/// let foo: Vec<u8> = RedisValue::Bytes(vec![102, 111, 111].into()).convert()?;
/// let foo: Vec<u8> = RedisValue::String("foo".into()).convert()?;
/// let foo: Vec<String> = RedisValue::Array(vec!["a".into(), "b".into()]).convert()?;
/// let foo: HashMap<String, u16> =
///   RedisValue::Array(vec!["a".into(), 1.into(), "b".into(), 2.into()]).convert()?;
/// let foo: (String, i64) = RedisValue::Array(vec!["a".into(), 1.into()]).convert()?;
/// let foo: Vec<(String, i64)> =
///   RedisValue::Array(vec!["a".into(), 1.into(), "b".into(), 2.into()]).convert()?;
/// // ...
/// ```
///
/// ## Bulk Values
///
/// This interface can also convert single-element vectors to scalar values in certain scenarios. This is often
/// useful with commands that conditionally return bulk values, or where the number of elements in the response
/// depends on the number of arguments (`MGET`, etc).
///
/// For example:
///
/// ```rust
/// # use fred::types::RedisValue;
/// let _: String = RedisValue::Array(vec![]).convert()?; // does not work
/// let _: String = RedisValue::Array(vec!["a".into()]).convert()?; // "a"
/// let _: String = RedisValue::Array(vec!["a".into(), "b".into()]).convert()?; // does not work
/// let _: Option<String> = RedisValue::Array(vec![]).convert()?; // None
/// let _: Option<String> = RedisValue::Array(vec!["a".into()]).convert()?; // Some("a")
/// let _: Option<String> = RedisValue::Array(vec!["a".into(), "b".into()]).convert()?; // does not work
/// ```
///
/// ## The `default-nil-types` Feature Flag
///
/// By default a `nil` value cannot be converted directly into any of the scalar types (`u8`, `String`, `Bytes`,
/// etc). In practice this often requires callers to use an `Option` or `Vec` container with commands that can return
/// `nil`.
///
/// The `default-nil-types` feature flag can enable some further type conversion branches that treat `nil` values as
/// default values for the relevant type. For `RedisValue::Null` these include:
///
/// * `impl FromRedis` for `String` or `Str` returns an empty string.
/// * `impl FromRedis` for `Bytes` or `Vec<T>` return an empty array.
/// * `impl FromRedis` for any integer or float type returns `0`
/// * `impl FromRedis` for `bool` returns `false`
/// * `impl FromRedis` for map or set types return an empty map or set.
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
    check_single_bulk_reply!(value);
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
    debug_type!("FromRedis(String): {:?}", value);
    check_single_bulk_reply!(value);
    check_loose_nil!(value, NIL.to_owned());

    value
      .into_string()
      .ok_or(RedisError::new_parse("Could not convert to string."))
  }
}

impl FromRedis for Str {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    debug_type!("FromRedis(Str): {:?}", value);
    check_single_bulk_reply!(value);
    check_loose_nil!(value, utils::static_str(NIL));

    value
      .into_bytes_str()
      .ok_or(RedisError::new_parse("Could not convert to string."))
  }
}

impl FromRedis for f64 {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    debug_type!("FromRedis(f64): {:?}", value);
    check_single_bulk_reply!(value);
    check_loose_nil!(value, 0.0);

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
    debug_type!("FromRedis(f32): {:?}", value);
    check_single_bulk_reply!(value);
    check_loose_nil!(value, 0.0);

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
    debug_type!("FromRedis(bool): {:?}", value);
    check_single_bulk_reply!(value);
    check_loose_nil!(value, false);

    if let Some(val) = value.as_bool() {
      Ok(val)
    } else {
      // it's not obvious how to convert the value to a bool in this block, so we go with a
      // tried and true approach that i'm sure we'll never regret - JS semantics
      Ok(match value {
        RedisValue::String(s) => !s.is_empty(),
        RedisValue::Bytes(b) => !b.is_empty(),
        // everything else should be covered by `as_bool` above
        _ => return Err(RedisError::new_parse("Could not convert to bool.")),
      })
    }
  }
}

impl<T> FromRedis for Option<T>
where
  T: FromRedis,
{
  fn from_value(value: RedisValue) -> Result<Option<T>, RedisError> {
    debug_type!("FromRedis(Option<{}>): {:?}", type_name::<T>(), value);

    if let Some(0) = value.array_len() {
      Ok(None)
    } else if value.is_null() {
      Ok(None)
    } else {
      Ok(Some(T::from_value(value)?))
    }
  }
}

impl FromRedis for Bytes {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    debug_type!("FromRedis(Bytes): {:?}", value);
    check_single_bulk_reply!(value);
    check_loose_nil!(value, NIL.into());

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
    debug_type!("FromRedis(Vec<{}>): {:?}", type_name::<T>(), value);

    match value {
      RedisValue::Bytes(bytes) => {
        T::from_owned_bytes(bytes.to_vec()).ok_or(RedisError::new_parse("Cannot convert from bytes"))
      },
      RedisValue::String(string) => {
        // hacky way to check if T is bytes without consuming `string`
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(string.into_inner().to_vec())
            .ok_or(RedisError::new_parse("Could not convert string to bytes."))
        } else {
          Ok(vec![T::from_value(RedisValue::String(string))?])
        }
      },
      RedisValue::Array(values) => {
        if values.len() > 0 {
          if let RedisValue::Array(_) = &values[0] {
            values.into_iter().map(|x| T::from_value(x)).collect()
          } else {
            T::from_values(values)
          }
        } else {
          Ok(vec![])
        }
      },
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
      },
      RedisValue::Integer(i) => Ok(vec![T::from_value(RedisValue::Integer(i))?]),
      RedisValue::Double(f) => Ok(vec![T::from_value(RedisValue::Double(f))?]),
      RedisValue::Boolean(b) => Ok(vec![T::from_value(RedisValue::Boolean(b))?]),
      RedisValue::Queued => Ok(vec![T::from_value(RedisValue::from_static_str(QUEUED))?]),
      #[cfg(feature = "default-nil-types")]
      RedisValue::Null => {
        // specialize Vec<u8> so we don't punish callers that defer string parsing, but still want consistent behavior
        // with the other `nil` -> string type conversion branches
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(NIL.as_bytes().to_vec()).ok_or(RedisError::new_parse("Could not convert null to vec."))
        } else {
          Ok(Vec::new())
        }
      },
      #[cfg(not(feature = "default-nil-types"))]
      RedisValue::Null => Ok(Vec::new()),
    }
  }
}

impl<T, const N: usize> FromRedis for [T; N]
where
  T: FromRedis,
{
  fn from_value(value: RedisValue) -> Result<[T; N], RedisError> {
    debug_type!("FromRedis([{}; {}]): {:?}", type_name::<T>(), N, value);
    // use the `from_value` impl for Vec<T>
    value
      .convert::<Vec<T>>()?
      .try_into()
      .map_err(|_| RedisError::new_parse("Failed to convert to array."))
  }
}

impl<K, V, S> FromRedis for HashMap<K, V, S>
where
  K: FromRedisKey + Eq + Hash,
  V: FromRedis,
  S: BuildHasher + Default,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    debug_type!(
      "FromRedis(HashMap<{}, {}>): {:?}",
      type_name::<K>(),
      type_name::<V>(),
      value
    );

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
    debug_type!("FromRedis(HashSet<{}>): {:?}", type_name::<V>(), value);
    value.into_array().into_iter().map(|v| V::from_value(v)).collect()
  }
}

impl<K, V> FromRedis for BTreeMap<K, V>
where
  K: FromRedisKey + Ord,
  V: FromRedis,
{
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    debug_type!(
      "FromRedis(BTreeMap<{}, {}>): {:?}",
      type_name::<K>(),
      type_name::<V>(),
      value
    );
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
    debug_type!("FromRedis(BTreeSet<{}>): {:?}", type_name::<V>(), value);
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
          debug_type!("FromRedis({}-tuple): {:?}", n, values);
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
        debug_type!("FromRedis({}-tuple): {:?}", n, values);
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

#[cfg(feature = "serde-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde-json")))]
impl FromRedis for Value {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    let value = match value {
      RedisValue::Null => Value::Null,
      RedisValue::Queued => QUEUED.into(),
      RedisValue::String(s) => {
        if let Some(parsed) = utils::parse_nested_json(&s) {
          parsed
        } else {
          s.to_string().into()
        }
      },
      RedisValue::Bytes(b) => String::from_utf8(b.to_vec())?.into(),
      RedisValue::Integer(i) => i.into(),
      RedisValue::Double(f) => f.into(),
      RedisValue::Boolean(b) => b.into(),
      RedisValue::Array(v) => {
        let mut out = Vec::with_capacity(v.len());
        for value in v.into_iter() {
          out.push(Self::from_value(value)?);
        }
        Value::Array(out)
      },
      RedisValue::Map(v) => {
        let mut out = Map::with_capacity(v.len());
        for (key, value) in v.inner().into_iter() {
          let key = key
            .into_string()
            .ok_or(RedisError::new_parse("Cannot convert key to string."))?;
          let value = Self::from_value(value)?;

          out.insert(key, value);
        }
        Value::Object(out)
      },
    };

    Ok(value)
  }
}

impl FromRedis for GeoPosition {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    GeoPosition::try_from(value)
  }
}

impl FromRedis for RedisKey {
  fn from_value(value: RedisValue) -> Result<Self, RedisError> {
    let key = match value {
      RedisValue::Boolean(b) => b.into(),
      RedisValue::Integer(i) => i.into(),
      RedisValue::Double(f) => f.into(),
      RedisValue::String(s) => s.into(),
      RedisValue::Bytes(b) => b.into(),
      RedisValue::Queued => RedisKey::from_static_str(QUEUED),
      RedisValue::Map(_) | RedisValue::Array(_) => {
        return Err(RedisError::new_parse("Cannot convert aggregate type to key."))
      },
      RedisValue::Null => return Err(RedisError::new(RedisErrorKind::NotFound, "Cannot convert nil to key.")),
    };

    Ok(key)
  }
}

/// A trait used to convert [RedisKey](crate::types::RedisKey) values to various types.
///
/// See the [convert](crate::types::RedisKey::convert) documentation for more information.
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

impl FromRedisKey for () {
  fn from_key(_: RedisKey) -> Result<Self, RedisError> {
    Ok(())
  }
}

impl FromRedisKey for RedisValue {
  fn from_key(value: RedisKey) -> Result<Self, RedisError> {
    Ok(RedisValue::Bytes(value.into_bytes()))
  }
}

impl FromRedisKey for RedisKey {
  fn from_key(value: RedisKey) -> Result<Self, RedisError> {
    Ok(value)
  }
}

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

#[cfg(test)]
mod tests {
  use crate::types::RedisValue;
  use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

  #[cfg(not(feature = "default-nil-types"))]
  use crate::error::RedisError;

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
  #[cfg(not(feature = "default-nil-types"))]
  fn should_return_not_found_with_null_number_types() {
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
  #[cfg(feature = "default-nil-types")]
  fn should_return_zero_with_null_number_types() {
    assert_eq!(0, RedisValue::Null.convert::<u8>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<u16>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<u32>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<u64>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<u128>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<usize>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<i8>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<i16>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<i32>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<i64>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<i128>().unwrap());
    assert_eq!(0, RedisValue::Null.convert::<isize>().unwrap());
    assert_eq!(0.0, RedisValue::Null.convert::<f32>().unwrap());
    assert_eq!(0.0, RedisValue::Null.convert::<f64>().unwrap());
  }

  #[test]
  fn should_convert_null_to_false() {
    assert!(!RedisValue::Null.convert::<bool>().unwrap());
  }

  #[test]
  fn should_convert_strings() {
    let _foo: String = RedisValue::String("foo".into()).convert().unwrap();
    assert_eq!(_foo, "foo".to_owned());
  }

  #[test]
  fn should_convert_numbers_to_bools() {
    let foo: bool = RedisValue::Integer(0).convert().unwrap();
    assert_eq!(foo, false);
    let foo: bool = RedisValue::Integer(1).convert().unwrap();
    assert_eq!(foo, true);
    let foo: bool = RedisValue::String("0".into()).convert().unwrap();
    assert_eq!(foo, false);
    let foo: bool = RedisValue::String("1".into()).convert().unwrap();
    assert_eq!(foo, true);
  }

  #[test]
  fn should_convert_bytes() {
    let foo: Vec<u8> = RedisValue::Bytes("foo".as_bytes().to_vec().into()).convert().unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
    let foo: Vec<u8> = RedisValue::String("foo".into()).convert().unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
    let foo: Vec<u8> = RedisValue::Array(vec![102.into(), 111.into(), 111.into()])
      .convert()
      .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
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

  #[test]
  fn should_handle_single_element_vector_to_scalar() {
    assert!(RedisValue::Array(vec![]).convert::<String>().is_err());
    assert_eq!(
      RedisValue::Array(vec!["foo".into()]).convert::<String>(),
      Ok("foo".into())
    );
    assert!(RedisValue::Array(vec!["foo".into(), "bar".into()])
      .convert::<String>()
      .is_err());

    assert_eq!(RedisValue::Array(vec![]).convert::<Option<String>>(), Ok(None));
    assert_eq!(
      RedisValue::Array(vec!["foo".into()]).convert::<Option<String>>(),
      Ok(Some("foo".into()))
    );
    assert!(RedisValue::Array(vec!["foo".into(), "bar".into()])
      .convert::<Option<String>>()
      .is_err());
  }

  #[test]
  #[cfg(not(feature = "default-nil-types"))]
  fn should_not_specialize_nil_with_byte_vec() {
    assert_eq!(Vec::<String>::new(), RedisValue::Null.convert::<Vec<String>>().unwrap());
    assert_eq!(Vec::<u8>::new(), RedisValue::Null.convert::<Vec<u8>>().unwrap());
  }

  #[test]
  #[cfg(feature = "default-nil-types")]
  fn should_specialize_loose_nil_with_byte_vec() {
    assert_eq!(b"nil".to_vec(), RedisValue::Null.convert::<Vec<u8>>().unwrap());
    assert_eq!(Vec::<String>::new(), RedisValue::Null.convert::<Vec<String>>().unwrap());
  }

  #[test]
  fn should_convert_to_fixed_arrays() {
    let foo: [i64; 2] = RedisValue::Array(vec![1.into(), 2.into()]).convert().unwrap();
    assert_eq!(foo, [1, 2]);

    assert!(RedisValue::Array(vec![1.into(), 2.into()])
      .convert::<[i64; 3]>()
      .is_err());
    assert!(RedisValue::Array(vec![]).convert::<[i64; 3]>().is_err());
  }
}
