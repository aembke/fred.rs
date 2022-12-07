use crate::{
  error::RedisError,
  types::{RedisKey, RedisValue},
};
use std::{
  collections::VecDeque,
  convert::{TryFrom, TryInto},
  iter::FromIterator,
};

/// Convenience struct for commands that take 1 or more keys.
///
/// **Note: this can also be used to represent an empty array of keys by passing `None` to any function that takes
/// `Into<MultipleKeys>`.** This is mostly useful for `EVAL` and `EVALSHA`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipleKeys {
  keys: Vec<RedisKey>,
}

impl MultipleKeys {
  pub fn new() -> MultipleKeys {
    MultipleKeys { keys: Vec::new() }
  }

  pub fn inner(self) -> Vec<RedisKey> {
    self.keys
  }

  pub fn into_values(self) -> Vec<RedisValue> {
    self.keys.into_iter().map(|k| k.into()).collect()
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }
}

impl From<Option<RedisKey>> for MultipleKeys {
  fn from(key: Option<RedisKey>) -> Self {
    let keys = if let Some(key) = key { vec![key] } else { vec![] };
    MultipleKeys { keys }
  }
}

impl<T> From<T> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from(d: T) -> Self {
    MultipleKeys { keys: vec![d.into()] }
  }
}

impl<T> FromIterator<T> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    MultipleKeys {
      keys: iter.into_iter().map(|k| k.into()).collect(),
    }
  }
}

impl<T> From<Vec<T>> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from(d: Vec<T>) -> Self {
    MultipleKeys {
      keys: d.into_iter().map(|k| k.into()).collect(),
    }
  }
}

impl<T> From<VecDeque<T>> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from(d: VecDeque<T>) -> Self {
    MultipleKeys {
      keys: d.into_iter().map(|k| k.into()).collect(),
    }
  }
}

/// Convenience struct for commands that take 1 or more strings.
pub type MultipleStrings = MultipleKeys;

/// Convenience struct for commands that take 1 or more values.
///
/// **Note: this can be used to represent an empty set of values by using `None` for any function that takes
/// `Into<MultipleValues>`.** This is most useful for `EVAL` and `EVALSHA`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipleValues {
  values: Vec<RedisValue>,
}

impl MultipleValues {
  pub fn inner(self) -> Vec<RedisValue> {
    self.values
  }

  pub fn len(&self) -> usize {
    self.values.len()
  }

  /// Convert this a nested `RedisValue`.
  pub fn into_values(self) -> RedisValue {
    RedisValue::Array(self.values)
  }
}

impl From<Option<RedisValue>> for MultipleValues {
  fn from(val: Option<RedisValue>) -> Self {
    let values = if let Some(val) = val { vec![val] } else { vec![] };
    MultipleValues { values }
  }
}

// https://github.com/rust-lang/rust/issues/50133
// FIXME there has to be a way around this issue?
// impl<T> TryFrom<T> for MultipleValues
// where
// T: TryInto<RedisValue>,
// T::Error: Into<RedisError>,
// {
// type Error = RedisError;
//
// fn try_from(d: T) -> Result<Self, Self::Error> {
// Ok(MultipleValues { values: vec![to!(d)?] })
// }
// }

// TODO consider supporting conversion from tuples with a reasonable size

impl<T> From<T> for MultipleValues
where
  T: Into<RedisValue>,
{
  fn from(d: T) -> Self {
    MultipleValues { values: vec![d.into()] }
  }
}

impl<T> FromIterator<T> for MultipleValues
where
  T: Into<RedisValue>,
{
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    MultipleValues {
      values: iter.into_iter().map(|v| v.into()).collect(),
    }
  }
}

impl<T> TryFrom<Vec<T>> for MultipleValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: Vec<T>) -> Result<Self, Self::Error> {
    let mut values = Vec::with_capacity(d.len());
    for value in d.into_iter() {
      values.push(to!(value)?);
    }

    Ok(MultipleValues { values })
  }
}

impl<T> TryFrom<VecDeque<T>> for MultipleValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: VecDeque<T>) -> Result<Self, Self::Error> {
    let mut values = Vec::with_capacity(d.len());
    for value in d.into_iter() {
      values.push(to!(value)?);
    }

    Ok(MultipleValues { values })
  }
}

/// A convenience struct for functions that take one or more hash slot values.
pub struct MultipleHashSlots {
  inner: Vec<u16>,
}

impl MultipleHashSlots {
  pub fn inner(self) -> Vec<u16> {
    self.inner
  }

  pub fn len(&self) -> usize {
    self.inner.len()
  }
}

impl From<u16> for MultipleHashSlots {
  fn from(d: u16) -> Self {
    MultipleHashSlots { inner: vec![d] }
  }
}

impl From<Vec<u16>> for MultipleHashSlots {
  fn from(d: Vec<u16>) -> Self {
    MultipleHashSlots { inner: d }
  }
}

impl<'a> From<&'a [u16]> for MultipleHashSlots {
  fn from(d: &'a [u16]) -> Self {
    MultipleHashSlots { inner: d.to_vec() }
  }
}

impl From<VecDeque<u16>> for MultipleHashSlots {
  fn from(d: VecDeque<u16>) -> Self {
    MultipleHashSlots {
      inner: d.into_iter().collect(),
    }
  }
}

impl FromIterator<u16> for MultipleHashSlots {
  fn from_iter<I: IntoIterator<Item = u16>>(iter: I) -> Self {
    MultipleHashSlots {
      inner: iter.into_iter().collect(),
    }
  }
}
