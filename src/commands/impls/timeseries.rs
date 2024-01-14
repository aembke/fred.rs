use crate::{
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  prelude::{FromRedis, RedisKey},
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::{
    Aggregator,
    DuplicatePolicy,
    Encoding,
    GetLabels,
    GetTimestamp,
    GroupBy,
    RangeAggregation,
    RedisMap,
    RedisValue,
    Timestamp,
  },
  utils,
};
use bytes_utils::Str;

static LATEST: &str = "LATEST";
static FILTER_BY_TS: &str = "FILTER_BY_TS";
static FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
static COUNT: &str = "COUNT";
static ALIGN: &str = "ALIGN";
static AGGREGATION: &str = "AGGREGATION";
static BUCKETTIMESTAMP: &str = "BUCKETTIMESTAMP";
static EMPTY: &str = "EMPTY";
static WITHLABELS: &str = "WITHLABELS";
static SELECTED_LABELS: &str = "SELECTED_LABELS";
static FILTER: &str = "FILTER";
static GROUPBY: &str = "GROUPBY";
static REDUCE: &str = "REDUCE";
static RETENTION: &str = "RETENTION";
static ENCODING: &str = "ENCODING";
static CHUNK_SIZE: &str = "CHUNK_SIZE";
static ON_DUPLICATE: &str = "ON_DUPLICATE";
static DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
static LABELS: &str = "LABELS";
static UNCOMPRESSED: &str = "UNCOMPRESSED";
static TIMESTAMP: &str = "TIMESTAMP";

fn add_labels(args: &mut Vec<RedisValue>, labels: RedisMap) {
  if !labels.is_empty() {
    args.push(static_val!(LABELS));

    for (label, value) in labels.inner().into_iter() {
      args.push(label.into());
      args.push(value);
    }
  }
}

pub async fn ts_add<C: ClientLike>(
  client: &C,
  key: RedisKey,
  timestamp: Timestamp,
  value: f64,
  retention: Option<u64>,
  encoding: Option<Encoding>,
  chunk_size: Option<u64>,
  on_duplicate: Option<DuplicatePolicy>,
  labels: RedisMap,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(12 + labels.len() * 2);
    args.push(key.into());
    args.push(timestamp.to_value());
    args.push(value.into());

    if let Some(retention) = retention {
      args.push(static_val!(RETENTION));
      args.push(retention.try_into()?);
    }
    if let Some(encoding) = encoding {
      args.push(static_val!(ENCODING));
      args.push(encoding.to_str().into());
    }
    if let Some(chunk_size) = chunk_size {
      args.push(static_val!(CHUNK_SIZE));
      args.push(chunk_size.into());
    }
    if let Some(duplicate) = on_duplicate {
      args.push(static_val!(ON_DUPLICATE));
      args.push(duplicate.to_str().into());
    }

    add_labels(&mut args, labels);
    Ok((RedisCommandKind::TsAdd, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_alter<C: ClientLike>(
  client: &C,
  key: RedisKey,
  retention: Option<u64>,
  chunk_size: Option<u64>,
  duplicate_policy: Option<DuplicatePolicy>,
  labels: RedisMap,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(8 + labels.len() * 2);
    args.push(key.into());

    if let Some(retention) = retention {
      args.push(static_val!(RETENTION));
      args.push(retention.try_into()?);
    }
    if let Some(chunk_size) = chunk_size {
      args.push(static_val!(CHUNK_SIZE));
      args.push(chunk_size.into());
    }
    if let Some(duplicate) = duplicate_policy {
      args.push(static_val!(DUPLICATE_POLICY));
      args.push(duplicate.to_str().into());
    }

    add_labels(&mut args, labels);
    Ok((RedisCommandKind::TsAlter, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_create<C: ClientLike>(
  client: &C,
  key: RedisKey,
  retention: Option<u64>,
  encoding: Option<Encoding>,
  chunk_size: Option<u64>,
  duplicate_policy: Option<DuplicatePolicy>,
  labels: RedisMap,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(10 + labels.len() * 2);
    args.push(key.into());

    if let Some(retention) = retention {
      args.push(static_val!(RETENTION));
      args.push(retention.try_into()?);
    }
    if let Some(encoding) = encoding {
      args.push(static_val!(ENCODING));
      args.push(encoding.to_str().into());
    }
    if let Some(chunk_size) = chunk_size {
      args.push(static_val!(CHUNK_SIZE));
      args.push(chunk_size.into());
    }
    if let Some(duplicate) = duplicate_policy {
      args.push(static_val!(DUPLICATE_POLICY));
      args.push(duplicate.to_str().into());
    }

    add_labels(&mut args, labels);
    Ok((RedisCommandKind::TsCreate, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_createrule<C: ClientLike>(
  client: &C,
  src: RedisKey,
  dest: RedisKey,
  aggregation: (Aggregator, u64),
  align_timestamp: Option<u64>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(6);
    args.extend([
      src.into(),
      dest.into(),
      static_val!(AGGREGATION),
      aggregation.0.to_str().into(),
      aggregation.1.try_into()?,
    ]);

    if let Some(align) = align_timestamp {
      args.push(align.try_into()?)
    }
    Ok((RedisCommandKind::TsCreateRule, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_decrby<C: ClientLike>(
  client: &C,
  key: RedisKey,
  subtrahend: f64,
  timestamp: Option<Timestamp>,
  retention: Option<u64>,
  uncompressed: bool,
  chunk_size: Option<u64>,
  labels: RedisMap,
) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_del<C: ClientLike>(client: &C, key: RedisKey, from: u64, to: u64) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_deleterule<C: ClientLike>(client: &C, src: RedisKey, dest: RedisKey) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_get<C: ClientLike>(client: &C, key: RedisKey, latest: bool) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_incrby<C: ClientLike>(
  client: &C,
  key: RedisKey,
  addend: f64,
  timestamp: Option<Timestamp>,
  retention: Option<u64>,
  uncompressed: bool,
  chunk_size: Option<u64>,
  labels: RedisMap,
) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_info<C: ClientLike>(client: &C, key: RedisKey, debug: bool) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_madd<C: ClientLike>(client: &C, samples: Vec<(RedisKey, Timestamp, f64)>) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_mget<C: ClientLike>(
  client: &C,
  latest: bool,
  labels: Option<GetLabels>,
  filters: Vec<Str>,
) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_mrange<C: ClientLike>(
  client: &C,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<u64>,
  filter_by_value: Option<(u64, u64)>,
  labels: Option<GetLabels>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
  filters: Vec<Str>,
  group_by: Option<GroupBy>,
) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_mrevrange<C: ClientLike>(
  client: &C,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<u64>,
  filter_by_value: Option<(u64, u64)>,
  labels: Option<GetLabels>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
  filters: Vec<Str>,
  group_by: Option<GroupBy>,
) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_queryindex<C: ClientLike>(client: &C, filters: Vec<Str>) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_range<C: ClientLike>(
  client: &C,
  key: RedisKey,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<u64>,
  filter_by_value: Option<(u64, u64)>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
) -> RedisResult<RedisValue> {
  unimplemented!()
}

pub async fn ts_revrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<u64>,
  filter_by_value: Option<(u64, u64)>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
) -> RedisResult<RedisValue> {
  unimplemented!()
}
