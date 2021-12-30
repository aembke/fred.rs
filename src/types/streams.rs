use crate::commands::{MAXLEN, MINID};
use crate::error::{RedisError, RedisErrorKind};
use crate::types::{LimitCount, RedisKey, RedisValue};
use std::collections::VecDeque;
use std::convert::{TryFrom, TryInto};

/// Representation for the "=" or "~" operator in `XADD`, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XCapTrim {
  Exact,
  AlmostExact,
}

impl XCapTrim {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      XCapTrim::Exact => "=",
      XCapTrim::AlmostExact => "~",
    }
  }
}

impl<'a> TryFrom<&'a str> for XCapTrim {
  type Error = RedisError;

  fn try_from(s: &'a str) -> Result<Self, Self::Error> {
    Ok(match s.as_ref() {
      "=" => XCapTrim::Exact,
      "~" => XCapTrim::AlmostExact,
      _ => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid XADD trim value.",
        ))
      }
    })
  }
}

/// One or more ordered key-value pairs, typically used as an argument for `XADD`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipleOrderedPairs {
  values: Vec<(RedisKey, RedisValue)>,
}

impl MultipleOrderedPairs {
  pub fn len(&self) -> usize {
    self.values.len()
  }

  pub fn inner(self) -> Vec<(RedisKey, RedisValue)> {
    self.values
  }
}

impl<K, V> TryFrom<(K, V)> for MultipleOrderedPairs
where
  K: Into<RedisKey>,
  V: TryInto<RedisValue>,
  V::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from((key, value): (K, V)) -> Result<Self, Self::Error> {
    Ok(MultipleOrderedPairs {
      values: vec![(key.into(), to!(value)?)],
    })
  }
}

impl<K, V> TryFrom<Vec<(K, V)>> for MultipleOrderedPairs
where
  K: Into<RedisKey>,
  V: TryInto<RedisValue>,
  V::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(values: Vec<(K, V)>) -> Result<Self, Self::Error> {
    Ok(MultipleOrderedPairs {
      values: values
        .into_iter()
        .map(|(key, value)| Ok((key.into(), to!(value)?)))
        .collect::<Result<Vec<(RedisKey, RedisValue)>, RedisError>>()?,
    })
  }
}

impl<K, V> TryFrom<VecDeque<(K, V)>> for MultipleOrderedPairs
where
  K: Into<RedisKey>,
  V: TryInto<RedisValue>,
  V::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(values: VecDeque<(K, V)>) -> Result<Self, Self::Error> {
    Ok(MultipleOrderedPairs {
      values: values
        .into_iter()
        .map(|(key, value)| Ok((key.into(), to!(value)?)))
        .collect::<Result<Vec<(RedisKey, RedisValue)>, RedisError>>()?,
    })
  }
}

/// One or more IDs for elements in a stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipleIDs {
  inner: Vec<XID>,
}

impl MultipleIDs {
  pub fn len(&self) -> usize {
    self.inner.len()
  }

  pub fn inner(self) -> Vec<XID> {
    self.inner
  }
}

impl<T> From<T> for MultipleIDs
where
  T: Into<XID>,
{
  fn from(value: T) -> Self {
    MultipleIDs {
      inner: vec![value.into()],
    }
  }
}

impl<T> From<Vec<T>> for MultipleIDs
where
  T: Into<XID>,
{
  fn from(value: Vec<T>) -> Self {
    MultipleIDs {
      inner: value.into_iter().map(|value| value.into()).collect(),
    }
  }
}

impl<T> From<VecDeque<T>> for MultipleIDs
where
  T: Into<XID>,
{
  fn from(value: VecDeque<T>) -> Self {
    MultipleIDs {
      inner: value.into_iter().map(|value| value.into()).collect(),
    }
  }
}

/// The MAXLEN or MINID argument for a stream cap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XCapKind {
  MaxLen,
  MinID,
}

impl XCapKind {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      XCapKind::MaxLen => MAXLEN,
      XCapKind::MinID => MINID,
    }
  }
}

impl<'a> TryFrom<&'a str> for XCapKind {
  type Error = RedisError;

  fn try_from(value: &'a str) -> Result<Self, Self::Error> {
    Ok(match value.as_ref() {
      "MAXLEN" => XCapKind::MaxLen,
      "MINID" => XCapKind::MinID,
      _ => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Expected MAXLEN or MINID,",
        ))
      }
    })
  }
}

/// Stream cap arguments for `XADD`, `XTRIM`, etc.
///
/// Equivalent to `[MAXLEN|MINID [=|~] threshold [LIMIT count]]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XCap {
  inner: Option<(XCapKind, XCapTrim, String, LimitCount)>,
}

impl XCap {
  pub(crate) fn into_parts(self) -> Option<(XCapKind, XCapTrim, String, LimitCount)> {
    self.inner
  }
}

impl From<Option<()>> for XCap {
  fn from(_: Option<()>) -> Self {
    XCap { inner: None }
  }
}

impl<K, T, S> TryFrom<(K, T, S, Option<i64>)> for XCap
where
  K: TryInto<XCapKind>,
  K::Error: Into<RedisError>,
  T: TryInto<XCapTrim>,
  T::Error: Into<RedisError>,
  S: Into<String>,
{
  type Error = RedisError;

  fn try_from((kind, trim, threshold, limit): (K, T, S, Option<i64>)) -> Result<Self, Self::Error> {
    let (kind, trim) = (to!(kind)?, to!(trim)?);
    Ok(XCap {
      inner: Some((kind, trim, threshold.into(), limit)),
    })
  }
}

impl<K, T, S> TryFrom<(K, T, S)> for XCap
where
  K: TryInto<XCapKind>,
  K::Error: Into<RedisError>,
  T: TryInto<XCapTrim>,
  T::Error: Into<RedisError>,
  S: Into<String>,
{
  type Error = RedisError;

  fn try_from((kind, trim, threshold): (K, T, S)) -> Result<Self, Self::Error> {
    let (kind, trim) = (to!(kind)?, to!(trim)?);
    Ok(XCap {
      inner: Some((kind, trim, threshold.into(), None)),
    })
  }
}

impl<K, S> TryFrom<(K, S)> for XCap
where
  K: TryInto<XCapKind>,
  K::Error: Into<RedisError>,
  S: Into<String>,
{
  type Error = RedisError;

  fn try_from((kind, threshold): (K, S)) -> Result<Self, Self::Error> {
    let kind = to!(kind)?;
    Ok(XCap {
      inner: Some((kind, XCapTrim::Exact, threshold.into(), None)),
    })
  }
}

/// Stream ID arguments for `XADD`, `XREAD`, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XID {
  /// The auto-generated key symbol "*".
  Auto,
  /// An ID specified by the user such as "12345-0".
  Manual(String),
  /// The highest ID in a stream ("$").
  Max,
}

impl XID {
  pub(crate) fn into_string(self) -> String {
    match self {
      XID::Auto => "*".to_owned(),
      XID::Max => "$".to_owned(),
      XID::Manual(s) => s,
    }
  }
}

impl<'a> From<&'a str> for XID {
  fn from(value: &'a str) -> Self {
    match value.as_ref() {
      "*" => XID::Auto,
      "$" => XID::Max,
      _ => XID::Manual(value.to_owned()),
    }
  }
}

impl From<String> for XID {
  fn from(value: String) -> Self {
    match value.as_ref() {
      "*" => XID::Auto,
      "$" => XID::Max,
      _ => XID::Manual(value),
    }
  }
}
