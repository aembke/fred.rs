use crate::types::{GeoPosition, GeoUnit, Limit, RedisKey, RedisValue, SortOrder, ZRange};
use bytes::Bytes;
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

impl ReducerFunc {
  pub(crate) fn to_str(&self) -> &'static str {
    use ReducerFunc::*;

    match self {
      Count => "COUNT",
      CountDistinct => "COUNT_DISTINCT",
      CountDistinctIsh => "COUNT_DISTINCTISH",
      Sum => "SUM",
      Min => "MIN",
      Max => "MAX",
      Avg => "AVG",
      StdDev => "STDDEV",
      Quantile => "QUANTILE",
      ToList => "TOLIST",
      FirstValue => "FIRST_VALUE",
      RandomSample => "RANDOM_SAMPLE",
      Custom(v) => v,
    }
  }
}

/// `REDUCE` arguments in `FT.AGGREGATE`.
///
/// Equivalent to `function nargs arg [arg ...] [AS name]`
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchReducer {
  pub func: ReducerFunc,
  pub args: Vec<Str>,
  pub name: Option<Str>,
}

impl SearchReducer {
  pub(crate) fn num_args(&self) -> usize {
    3 + self.args.len() + self.name.as_ref().map(|_| 2).unwrap_or(0)
  }
}

/// A search field with an optional property.
///
/// Typically equivalent to `identifier [AS property]` in `FT.AGGREGATE`, `FT.SEARCH`, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchField {
  pub identifier: Str,
  pub property:   Option<Str>,
}

impl SearchField {
  pub(crate) fn num_args(&self) -> usize {
    1 + self.property.as_ref().map(|_| 2).unwrap_or(0)
  }
}

/// Arguments to `LOAD` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Load {
  All,
  Some(Vec<SearchField>),
}

/// Arguments for `WITHCURSOR` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WithCursor {
  pub count:    Option<u64>,
  pub max_idle: Option<u64>,
}

/// Arguments for `PARAMS` in `FT.AGGREGATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchParameter {
  pub name:  Str,
  pub value: Str,
}

/// An aggregation operation used in `FT.AGGREGATE`.
///
/// <https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/aggregations/>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateOperation {
  Filter {
    expression: Str,
  },
  GroupBy {
    /// An empty array is equivalent to `GROUPBY 0`
    fields:   Vec<Str>,
    reducers: Vec<SearchReducer>,
  },
  Apply {
    expression: Str,
    name:       Str,
  },
  SortBy {
    properties: Vec<(Str, SortOrder)>,
    max:        Option<u64>,
  },
  Limit {
    offset: u64,
    num:    u64,
  },
}

impl AggregateOperation {
  pub(crate) fn num_args(&self) -> usize {
    match self {
      AggregateOperation::Filter { .. } => 2,
      AggregateOperation::Limit { .. } => 3,
      AggregateOperation::Apply { .. } => 4,
      AggregateOperation::SortBy { max, properties, .. } => {
        2 + (properties.len() * 2) + max.as_ref().map(|_| 2).unwrap_or(0)
      },
      AggregateOperation::GroupBy { fields, reducers } => {
        2 + fields.len() + reducers.iter().fold(0, |m, r| m + r.num_args())
      },
    }
  }
}

/// Arguments to the `FT.AGGREGATE` command.
///
/// <https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/aggregations/>
#[derive(Clone, Debug, Default)]
pub struct FtAggregateOptions {
  pub verbatim: bool,
  pub load:     Option<Load>,
  pub timeout:  Option<i64>,
  pub pipeline: Vec<AggregateOperation>,
  pub cursor:   Option<WithCursor>,
  pub params:   Vec<SearchParameter>,
  pub dialect:  Option<i64>,
}

impl FtAggregateOptions {
  pub(crate) fn num_args(&self) -> usize {
    let mut count = 0;
    if self.verbatim {
      count += 1;
    }
    if let Some(ref load) = self.load {
      count += 1
        + match load {
          Load::All => 1,
          Load::Some(ref v) => 1 + v.iter().fold(0, |m, f| m + f.num_args()),
        };
    }
    if self.timeout.is_some() {
      count += 2;
    }
    count += self.pipeline.iter().fold(0, |m, op| m + op.num_args());
    if let Some(ref cursor) = self.cursor {
      count += 1;
      if cursor.count.is_some() {
        count += 2;
      }
      if cursor.max_idle.is_some() {
        count += 2;
      }
    }
    if !self.params.is_empty() {
      count += 2 + self.params.len() * 2;
    }
    if self.dialect.is_some() {
      count += 2;
    }

    count
  }
}

/// Arguments for `FILTER` in `FT.SEARCH`.
///
/// Callers should use the `*Score*` variants on any provided [ZRange](crate::types::ZRange) values.
#[derive(Clone, Debug)]
pub struct SearchFilter {
  pub attribute: Str,
  pub min:       ZRange,
  pub max:       ZRange,
}

/// Arguments for `GEOFILTER` in `FT.SEARCH`.
#[derive(Clone, Debug)]
pub struct SearchGeoFilter {
  pub attribute: Str,
  pub position:  GeoPosition,
  pub radius:    RedisValue,
  pub units:     GeoUnit,
}

/// Arguments used in `SUMMARIZE` values.
///
/// <https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/highlight/>
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct SearchSummarize {
  pub fields:    Vec<Str>,
  pub frags:     Option<u64>,
  pub len:       Option<u64>,
  pub separator: Option<Str>,
}

/// Arguments used in `HIGHLIGHT` values.
///
/// <https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/highlight/>
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct SearchHighlight {
  pub fields: Vec<Str>,
  pub tags:   Option<(Str, Str)>,
}

/// Arguments for `SORTBY` in `FT.SEARCH`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchSortBy {
  pub attribute: Str,
  pub order:     Option<SortOrder>,
  pub withcount: bool,
}

/// Arguments to `FT.SEARCH`.
#[derive(Clone, Debug, Default)]
pub struct FtSearchOptions {
  pub nocontent:    bool,
  pub verbatim:     bool,
  pub nostopwords:  bool,
  pub withscores:   bool,
  pub withpayloads: bool,
  pub withsortkeys: bool,
  pub filters:      Vec<SearchFilter>,
  pub geofilters:   Vec<SearchGeoFilter>,
  pub inkeys:       Vec<RedisKey>,
  pub infields:     Vec<Str>,
  pub r#return:     Vec<SearchField>,
  pub summarize:    Option<SearchSummarize>,
  pub highlight:    Option<SearchHighlight>,
  pub slop:         Option<i64>,
  pub timeout:      Option<i64>,
  pub inorder:      bool,
  pub language:     Option<Str>,
  pub expander:     Option<Str>,
  pub scorer:       Option<Str>,
  pub explainscore: bool,
  pub payload:      Option<Bytes>,
  pub sortby:       Option<SearchSortBy>,
  pub limit:        Option<Limit>,
  pub params:       Vec<SearchParameter>,
  pub dialect:      Option<i64>,
}

impl FtSearchOptions {
  pub(crate) fn num_args(&self) -> usize {
    let mut count = 0;
    if self.nocontent {
      count += 1;
    }
    if self.verbatim {
      count += 1;
    }
    if self.nostopwords {
      count += 1;
    }
    if self.withscores {
      count += 1;
    }
    if self.withpayloads {
      count += 1;
    }
    if self.withsortkeys {
      count += 1;
    }
    count += self.filters.len() * 4;
    count += self.geofilters.len() * 6;
    if !self.inkeys.is_empty() {
      count += 2 + self.inkeys.len();
    }
    if !self.infields.is_empty() {
      count += 2 + self.infields.len();
    }
    if !self.r#return.is_empty() {
      count += 2;
      for val in self.r#return.iter() {
        count += if val.property.is_some() { 3 } else { 1 };
      }
    }
    if let Some(ref summarize) = self.summarize {
      count += 1;
      if !summarize.fields.is_empty() {
        count += 2 + summarize.fields.len();
      }
      if summarize.frags.is_some() {
        count += 2;
      }
      if summarize.len.is_some() {
        count += 2;
      }
      if summarize.separator.is_some() {
        count += 2;
      }
    }
    if let Some(ref highlight) = self.highlight {
      count += 1;
      if !highlight.fields.is_empty() {
        count += 2 + highlight.fields.len();
      }
      if highlight.tags.is_some() {
        count += 3;
      }
    }
    if self.slop.is_some() {
      count += 2;
    }
    if self.timeout.is_some() {
      count += 2;
    }
    if self.inorder {
      count += 1;
    }
    if self.language.is_some() {
      count += 2;
    }
    if self.expander.is_some() {
      count += 2;
    }
    if self.scorer.is_some() {
      count += 2;
    }
    if self.explainscore {
      count += 1;
    }
    if self.payload.is_some() {
      count += 2;
    }
    if let Some(ref sort) = self.sortby {
      count += 2 + if sort.order.is_some() { 1 } else { 0 } + if sort.withcount { 1 } else { 0 };
    }
    if self.limit.is_some() {
      count += 3;
    }
    if !self.params.is_empty() {
      count += 2 + self.params.len() * 2;
    }
    if self.dialect.is_some() {
      count += 2;
    }
    count
  }
}

/// Index arguments for `FT.CREATE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexKind {
  OnHash,
  JSON,
}

/// Arguments for `FT.CREATE`.
#[derive(Clone, Debug, Default)]
pub struct FtCreateOptions {
  pub on:              Option<IndexKind>,
  pub prefixes:        Vec<Str>,
  pub filter:          Option<Str>,
  pub language:        Option<Str>,
  pub language_field:  Option<Str>,
  pub score:           Option<f64>,
  pub score_field:     Option<f64>,
  pub payload_field:   Option<Str>,
  pub maxtextfields:   bool,
  pub temporary:       Option<u64>,
  pub nooffsets:       bool,
  pub nohl:            bool,
  pub nofields:        bool,
  pub nofreqs:         bool,
  pub stopwords:       Vec<Str>,
  pub skipinitialscan: bool,
}

impl FtCreateOptions {
  pub(crate) fn num_args(&self) -> usize {
    let mut count = 0;
    if self.on.is_some() {
      count += 2;
    }
    if !self.prefixes.is_empty() {
      count += 2 + self.prefixes.len();
    }
    if self.filter.is_some() {
      count += 2;
    }
    if self.language.is_some() {
      count += 2;
    }
    if self.language_field.is_some() {
      count += 2;
    }
    if self.score.is_some() {
      count += 2;
    }
    if self.score_field.is_some() {
      count += 2;
    }
    if self.payload_field.is_some() {
      count += 2;
    }
    if self.maxtextfields {
      count += 1;
    }
    if self.temporary.is_some() {
      count += 2;
    }
    if self.nooffsets {
      count += 1;
    }
    if self.nohl {
      count += 1;
    }
    if self.nofields {
      count += 1;
    }
    if self.nofreqs {
      count += 1;
    }
    if !self.stopwords.is_empty() {
      count += 2 + self.stopwords.len();
    }
    if self.skipinitialscan {
      count += 1;
    }

    count
  }
}

/// One of the available schema types used with `FT.CREATE` or `FT.ALTER`.
#[derive(Clone, Debug)]
pub enum SearchSchemaKind {
  Text {
    sortable:       bool,
    unf:            bool,
    nostem:         bool,
    phonetic:       bool,
    weight:         Option<i64>,
    withsuffixtrie: bool,
    noindex:        bool,
  },
  Tag {
    sortable:       bool,
    unf:            bool,
    separator:      Option<char>,
    casesensitive:  bool,
    withsuffixtrie: bool,
    noindex:        bool,
  },
  Numeric {
    sortable: bool,
    unf:      bool,
    noindex:  bool,
  },
  Geo {
    sortable: bool,
    unf:      bool,
    noindex:  bool,
  },
  Vector {
    noindex: bool,
  },
  GeoShape {
    noindex: bool,
  },
  Custom {
    name: Str,
    args: Vec<RedisValue>,
  },
}

impl SearchSchemaKind {
  pub(crate) fn num_args(&self) -> usize {
    unimplemented!()
  }
}

/// Arguments for `SCHEMA` in `FT.CREATE`.
#[derive(Clone, Debug)]
pub struct SearchSchema {
  pub field_name: Str,
  pub alias:      Option<Str>,
  pub kind:       SearchSchemaKind,
}

impl SearchSchema {
  pub(crate) fn num_args(&self) -> usize {
    let mut count = 2;
    if self.alias.is_some() {
      count += 2;
    }
    count + self.kind.num_args()
  }
}

/// Arguments to `FT.ALTER`.
#[derive(Clone, Debug)]
pub struct FtAlterOptions {
  pub skipinitialscan: bool,
  pub attribute:       Str,
  pub options:         SearchSchemaKind,
}

impl FtAlterOptions {
  pub(crate) fn num_args(&self) -> usize {
    // [SKIPINITIALSCAN] SCHEMA ADD {attribute} {options} ...
    let mut count = 3;
    if self.skipinitialscan {
      count += 1;
    }
    count + self.options.num_args()
  }
}

/// Arguments to `TERMS` in `FT.SPELLCHECK`,
#[derive(Clone, Debug)]
pub enum SpellcheckTerms {
  Include { dictionary: Str, terms: Vec<Str> },
  Exclude { dictionary: Str, terms: Vec<Str> },
}

impl SpellcheckTerms {
  pub(crate) fn num_args(&self) -> usize {
    3 + match self {
      SpellcheckTerms::Include { terms, .. } => terms.len(),
      SpellcheckTerms::Exclude { terms, .. } => terms.len(),
    }
  }
}
