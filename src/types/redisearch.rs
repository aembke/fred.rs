use crate::types::SortOrder;
use bytes_utils::Str;

/// `GROUPBY` reducer functions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReducerFunc {
  Count,
  CountDistinct,
  CountDistinctIsh,
  Sum,
  Min,
  Max,
  Avg,
  StdDev,
  Quantile,
  ToList,
  FirstValue,
  RandomSample,
  Custom(&'static str),
}

/// `REDUCE` arguments in `FT.AGGREGATE`.
///
/// Equivalent to `function nargs arg [arg ...] [AS name]`
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reducer {
  pub func: ReducerFunc,
  pub args: Vec<Str>,
  pub name: Option<Str>,
}

/// Fields loaded via the `LOAD` parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadField {
  pub identifier: Str,
  pub property:   Option<Str>,
}

/// Arguments to `LOAD` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Load {
  All,
  Some(Vec<LoadField>),
}

/// Arguments to `SORTBY` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortByProperty {
  pub properties: Vec<(Str, SortOrder)>,
  pub max:        Option<u64>,
}

/// Arguments to `APPLY` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Apply {
  pub expression: Str,
  pub name:       Str,
}

/// Arguments for `WITHCURSOR` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WithCursor {
  pub count:    u64,
  pub max_idle: u64,
}

/// Arguments for `PARAMS` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parameter {
  pub name:  Str,
  pub value: Str,
}
