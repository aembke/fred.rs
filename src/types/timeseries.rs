use crate::{
  error::{RedisError, RedisErrorKind},
  types::RedisValue,
  utils,
};
use bytes_utils::Str;

/// TODO docs
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Encoding {
  Compressed,
  Uncompressed,
}

impl Encoding {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      Encoding::Compressed => "COMPRESSED",
      Encoding::Uncompressed => "UNCOMPRESSED",
    })
  }
}

/// TODO docs
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DuplicatePolicy {
  Block,
  First,
  Last,
  Min,
  Max,
  Sum,
}

impl DuplicatePolicy {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      DuplicatePolicy::Block => "BLOCK",
      DuplicatePolicy::First => "FIRST",
      DuplicatePolicy::Last => "LAST",
      DuplicatePolicy::Min => "MIN",
      DuplicatePolicy::Max => "MAX",
      DuplicatePolicy::Sum => "SUM",
    })
  }
}

/// TODO
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Timestamp {
  /// Unix time (milliseconds since epoch).
  Custom(i64),
  /// The server's current time, equivalent to "*".
  Now,
}

impl Default for Timestamp {
  fn default() -> Self {
    Timestamp::Now
  }
}

impl Timestamp {
  pub(crate) fn to_value(&self) -> RedisValue {
    match *self {
      Timestamp::Now => RedisValue::String(utils::static_str("*")),
      Timestamp::Custom(v) => RedisValue::Integer(v),
    }
  }

  pub(crate) fn from_str(value: &str) -> Result<Self, RedisError> {
    match value.as_ref() {
      "*" => Ok(Timestamp::Now),
      _ => Ok(Timestamp::Custom(value.parse::<i64>()?)),
    }
  }
}

impl From<i64> for Timestamp {
  fn from(value: i64) -> Self {
    Timestamp::Custom(value)
  }
}

impl TryFrom<&str> for Timestamp {
  type Error = RedisError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    Self::from_str(value)
  }
}

impl TryFrom<Str> for Timestamp {
  type Error = RedisError;

  fn try_from(value: Str) -> Result<Self, Self::Error> {
    Self::from_str(&value)
  }
}

impl TryFrom<String> for Timestamp {
  type Error = RedisError;

  fn try_from(value: String) -> Result<Self, Self::Error> {
    Self::from_str(&value)
  }
}

/// TODO
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Aggregator {
  Avg,
  Sum,
  Min,
  Max,
  Range,
  Count,
  First,
  Last,
  StdP,
  StdS,
  VarP,
  VarS,
  TWA,
}

impl Aggregator {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      Aggregator::Avg => "avg",
      Aggregator::Sum => "sum",
      Aggregator::Min => "min",
      Aggregator::Max => "max",
      Aggregator::Range => "range",
      Aggregator::Count => "count",
      Aggregator::First => "first",
      Aggregator::Last => "last",
      Aggregator::StdP => "std.p",
      Aggregator::StdS => "std.s",
      Aggregator::VarP => "var.p",
      Aggregator::VarS => "var.s",
      Aggregator::TWA => "twa",
    })
  }
}

/// Arguments equivalent to `WITHLABELS | SELECTED_LABELS label...` in various time series GET functions.
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetLabels {
  WithLabels,
  SelectedLabels(Vec<Str>),
}

impl GetLabels {
  pub(crate) fn args_len(&self) -> usize {
    match *self {
      GetLabels::WithLabels => 1,
      GetLabels::SelectedLabels(ref s) => 1 + s.len(),
    }
  }
}

impl<S> FromIterator<S> for GetLabels
where
  S: Into<Str>,
{
  fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
    GetLabels::SelectedLabels(iter.into_iter().map(|v| v.into()).collect())
  }
}

impl<S, const N: usize> From<[S; N]> for GetLabels
where
  S: Into<Str>,
{
  fn from(value: [S; N]) -> Self {
    GetLabels::SelectedLabels(value.into_iter().map(|v| v.into()).collect())
  }
}

impl<S> From<Vec<S>> for GetLabels
where
  S: Into<Str>,
{
  fn from(value: Vec<S>) -> Self {
    GetLabels::SelectedLabels(value.into_iter().map(|v| v.into()).collect())
  }
}

/// A timestamp query used in commands such as `TS.MRANGE`.
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetTimestamp {
  /// Equivalent to `-`.
  Earliest,
  /// Equivalent to `+`
  Latest,
  Custom(i64),
}

impl GetTimestamp {
  pub(crate) fn to_value(&self) -> RedisValue {
    match *self {
      GetTimestamp::Earliest => static_val!("-"),
      GetTimestamp::Latest => static_val!("+"),
      GetTimestamp::Custom(i) => i.into(),
    }
  }
}

impl TryFrom<&str> for GetTimestamp {
  type Error = RedisError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    Ok(match value.as_ref() {
      "-" => GetTimestamp::Earliest,
      "+" => GetTimestamp::Latest,
      _ => GetTimestamp::Custom(value.parse::<i64>()?),
    })
  }
}

impl From<i64> for GetTimestamp {
  fn from(value: i64) -> Self {
    GetTimestamp::Custom(value)
  }
}

/// A struct representing `[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]` in
/// commands such as `TS.MRANGE`.
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeAggregation {
  pub align:            Option<GetTimestamp>,
  pub aggregation:      Aggregator,
  pub bucket_duration:  u64,
  pub bucket_timestamp: Option<BucketTimestamp>,
  pub empty:            bool,
}

impl From<(Aggregator, u64)> for RangeAggregation {
  fn from((aggregation, duration): (Aggregator, u64)) -> Self {
    RangeAggregation {
      aggregation,
      bucket_duration: duration,
      align: None,
      bucket_timestamp: None,
      empty: false,
    }
  }
}

/// A `REDUCER` argument in commands such as `TS.MRANGE`.
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reducer {
  Avg,
  Sum,
  Min,
  Max,
  Range,
  Count,
  StdP,
  StdS,
  VarP,
  VarS,
}

impl Reducer {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      Reducer::Avg => "avg",
      Reducer::Sum => "sum",
      Reducer::Min => "min",
      Reducer::Max => "max",
      Reducer::Range => "range",
      Reducer::Count => "count",
      Reducer::StdP => "std.p",
      Reducer::StdS => "std.s",
      Reducer::VarP => "var.p",
      Reducer::VarS => "var.s",
    })
  }
}

/// A struct representing `GROUPBY label REDUCE reducer` in commands such as `TS.MRANGE`.
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupBy {
  pub groupby: Str,
  pub reduce:  Reducer,
}

impl<S: Into<Str>> From<(S, Reducer)> for GroupBy {
  fn from((groupby, reduce): (S, Reducer)) -> Self {
    GroupBy {
      groupby: groupby.into(),
      reduce,
    }
  }
}

/// A `BUCKETTIMESTAMP` argument in commands such as `TS.MRANGE`.
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BucketTimestamp {
  Start,
  End,
  Mid,
}

impl TryFrom<&str> for BucketTimestamp {
  type Error = RedisError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    Ok(match value.as_ref() {
      "-" | "start" => BucketTimestamp::Start,
      "+" | "end" => BucketTimestamp::End,
      "~" | "mid" => BucketTimestamp::Mid,
      _ => {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid bucket timestamp.",
        ))
      },
    })
  }
}

impl BucketTimestamp {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      BucketTimestamp::Start => "-",
      BucketTimestamp::End => "+",
      BucketTimestamp::Mid => "~",
    })
  }
}
