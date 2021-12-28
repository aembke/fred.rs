use crate::error::{RedisError, RedisErrorKind};
use crate::types::{LimitCount, RedisKey, RedisValue};
use std::collections::VecDeque;
use std::convert::{TryFrom, TryInto};

/// Representation for the "=" or "~" operator in `XADD`, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XaddCapTrim {
  Exact,
  AlmostExact,
}

impl XaddCapTrim {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      XaddCapTrim::Exact => "=",
      XaddCapTrim::AlmostExact => "~",
    }
  }
}

impl<'a> TryFrom<&'a str> for XaddCapTrim {
  type Error = RedisError;

  fn try_from(s: &'a str) -> Result<Self, Self::Error> {
    Ok(match s.as_ref() {
      "=" => XaddCapTrim::Exact,
      "~" => XaddCapTrim::AlmostExact,
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

/// Stream cap arguments for `XADD`, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XaddCap {
  /// The MAXLEN argument option.
  MaxLen,
  /// The MINID argument option with the trim argument (=|~), the threshold, and LIMIT option, respectively.
  MinID((XaddCapTrim, String, LimitCount)),
}

/// Stream ID arguments for `XADD`, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum XaddID {
  Auto,
  Manual(String),
}

impl XaddID {
  pub(crate) fn to_string(self) -> String {
    match self {
      XaddID::Auto => "*".to_owned(),
      XaddID::Manual(s) => s,
    }
  }
}

impl<'a> From<&'a str> for XaddID {
  fn from(value: &'a str) -> Self {
    match value.as_ref() {
      "*" => XaddID::Auto,
      _ => XaddID::Manual(value.to_owned()),
    }
  }
}

impl From<String> for XaddID {
  fn from(value: String) -> Self {
    match value.as_ref() {
      "*" => XaddID::Auto,
      _ => XaddID::Manual(value),
    }
  }
}
