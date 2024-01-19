use crate::{
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  prelude::RedisKey,
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
static DEBUG: &str = "DEBUG";

fn add_labels(args: &mut Vec<RedisValue>, labels: RedisMap) {
  if !labels.is_empty() {
    args.push(static_val!(LABELS));

    for (label, value) in labels.inner().into_iter() {
      args.push(label.into());
      args.push(value);
    }
  }
}

fn add_retention(args: &mut Vec<RedisValue>, retention: Option<u64>) -> Result<(), RedisError> {
  if let Some(retention) = retention {
    args.push(static_val!(RETENTION));
    args.push(retention.try_into()?);
  }

  Ok(())
}

fn add_encoding(args: &mut Vec<RedisValue>, encoding: Option<Encoding>) {
  if let Some(encoding) = encoding {
    args.push(static_val!(ENCODING));
    args.push(encoding.to_str().into());
  }
}

fn add_chunk_size(args: &mut Vec<RedisValue>, chunk_size: Option<u64>) -> Result<(), RedisError> {
  if let Some(chunk_size) = chunk_size {
    args.push(static_val!(CHUNK_SIZE));
    args.push(chunk_size.try_into()?);
  }

  Ok(())
}

fn add_timestamp(args: &mut Vec<RedisValue>, timestamp: Option<Timestamp>) {
  if let Some(timestamp) = timestamp {
    args.push(static_val!(TIMESTAMP));
    args.push(timestamp.to_value());
  }
}

fn add_duplicate_policy(args: &mut Vec<RedisValue>, duplicate_policy: Option<DuplicatePolicy>) {
  if let Some(duplicate) = duplicate_policy {
    args.push(static_val!(DUPLICATE_POLICY));
    args.push(duplicate.to_str().into());
  }
}

fn add_count(args: &mut Vec<RedisValue>, count: Option<u64>) -> Result<(), RedisError> {
  if let Some(count) = count {
    args.push(static_val!(COUNT));
    args.push(count.try_into()?);
  }
  Ok(())
}

fn add_get_labels(args: &mut Vec<RedisValue>, labels: Option<GetLabels>) {
  if let Some(labels) = labels {
    match labels {
      GetLabels::WithLabels => args.push(static_val!(WITHLABELS)),
      GetLabels::SelectedLabels(labels) => {
        args.push(static_val!(SELECTED_LABELS));
        args.extend(labels.into_iter().map(|v| v.into()));
      },
    }
  }
}

fn add_range_aggregation(
  args: &mut Vec<RedisValue>,
  aggregation: Option<RangeAggregation>,
) -> Result<(), RedisError> {
  if let Some(aggregation) = aggregation {
    if let Some(align) = aggregation.align {
      args.push(static_val!(ALIGN));
      args.push(align.to_value());
    }

    args.push(static_val!(AGGREGATION));
    args.push(aggregation.aggregation.to_str().into());
    args.push(aggregation.bucket_duration.try_into()?);

    if let Some(bucket_timestamp) = aggregation.bucket_timestamp {
      args.push(static_val!(BUCKETTIMESTAMP));
      args.push(bucket_timestamp.to_str().into());
    }
    if aggregation.empty {
      args.push(static_val!(EMPTY));
    }
  }

  Ok(())
}

fn add_groupby(args: &mut Vec<RedisValue>, group_by: Option<GroupBy>) {
  if let Some(group_by) = group_by {
    args.push(static_val!(GROUPBY));
    args.push(group_by.groupby.into());
    args.push(static_val!(REDUCE));
    args.push(group_by.reduce.to_str().into());
  }
}

fn add_filters(args: &mut Vec<RedisValue>, filters: Vec<Str>) {
  args.push(static_val!(FILTER));
  args.extend(filters.into_iter().map(|s| s.into()));
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

    add_retention(&mut args, retention)?;
    add_encoding(&mut args, encoding);
    add_chunk_size(&mut args, chunk_size)?;
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

    add_retention(&mut args, retention)?;
    add_chunk_size(&mut args, chunk_size)?;
    add_duplicate_policy(&mut args, duplicate_policy);
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

    add_retention(&mut args, retention)?;
    add_encoding(&mut args, encoding);
    add_chunk_size(&mut args, chunk_size)?;
    add_duplicate_policy(&mut args, duplicate_policy);
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
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(10 + labels.len() * 2);
    args.push(key.into());
    args.push(subtrahend.into());

    add_timestamp(&mut args, timestamp);
    add_retention(&mut args, retention)?;
    if uncompressed {
      args.push(static_val!(UNCOMPRESSED));
    }
    add_chunk_size(&mut args, chunk_size)?;
    add_labels(&mut args, labels);

    Ok((RedisCommandKind::TsDecrBy, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_del<C: ClientLike>(client: &C, key: RedisKey, from: i64, to: i64) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::TsDel, vec![key.into(), from.into(), to.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_deleterule<C: ClientLike>(client: &C, src: RedisKey, dest: RedisKey) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::TsDeleteRule, vec![src.into(), dest.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_get<C: ClientLike>(client: &C, key: RedisKey, latest: bool) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());
    if latest {
      args.push(static_val!(LATEST));
    }

    Ok((RedisCommandKind::TsGet, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
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
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(10 + labels.len() * 2);
    args.push(key.into());
    args.push(addend.into());

    add_timestamp(&mut args, timestamp);
    add_retention(&mut args, retention)?;
    if uncompressed {
      args.push(static_val!(UNCOMPRESSED));
    }
    add_chunk_size(&mut args, chunk_size)?;
    add_labels(&mut args, labels);

    Ok((RedisCommandKind::TsIncrBy, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_info<C: ClientLike>(client: &C, key: RedisKey, debug: bool) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());
    if debug {
      args.push(static_val!(DEBUG));
    }

    Ok((RedisCommandKind::TsInfo, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_madd<C: ClientLike>(client: &C, samples: Vec<(RedisKey, Timestamp, f64)>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(samples.len() * 3);
    for (key, timestamp, value) in samples.into_iter() {
      args.extend([key.into(), timestamp.to_value(), value.into()]);
    }
    Ok((RedisCommandKind::TsMAdd, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_mget<C: ClientLike>(
  client: &C,
  latest: bool,
  labels: Option<GetLabels>,
  filters: Vec<Str>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let labels_len = labels.as_ref().map(|l| l.args_len()).unwrap_or(0);
    let mut args = Vec::with_capacity(2 + labels_len + filters.len());
    if latest {
      args.push(static_val!(LATEST));
    }
    add_get_labels(&mut args, labels);
    add_filters(&mut args, filters);

    Ok((RedisCommandKind::TsMGet, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_mrange<C: ClientLike>(
  client: &C,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<i64>,
  filter_by_value: Option<(i64, i64)>,
  labels: Option<GetLabels>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
  filters: Vec<Str>,
  group_by: Option<GroupBy>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let labels_len = labels.as_ref().map(|l| l.args_len()).unwrap_or(0);
    let mut args = Vec::with_capacity(18 + filter_by_ts.len() + labels_len + filters.len());

    args.extend([from.to_value(), to.to_value()]);
    if latest {
      args.push(static_val!(LATEST));
    }
    if !filter_by_ts.is_empty() {
      args.push(static_val!(FILTER_BY_TS));
      args.extend(filter_by_ts.into_iter().map(|t| t.into()));
    }
    if let Some((min, max)) = filter_by_value {
      args.push(static_val!(FILTER_BY_VALUE));
      args.extend([min.into(), max.into()]);
    }
    add_get_labels(&mut args, labels);
    add_count(&mut args, count)?;
    add_range_aggregation(&mut args, aggregation)?;
    add_filters(&mut args, filters);
    add_groupby(&mut args, group_by);

    Ok((RedisCommandKind::TsMRange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_mrevrange<C: ClientLike>(
  client: &C,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<i64>,
  filter_by_value: Option<(i64, i64)>,
  labels: Option<GetLabels>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
  filters: Vec<Str>,
  group_by: Option<GroupBy>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let labels_len = labels.as_ref().map(|l| l.args_len()).unwrap_or(0);
    let mut args = Vec::with_capacity(18 + filter_by_ts.len() + labels_len + filters.len());

    args.extend([from.to_value(), to.to_value()]);
    if latest {
      args.push(static_val!(LATEST));
    }
    if !filter_by_ts.is_empty() {
      args.push(static_val!(FILTER_BY_TS));
      args.extend(filter_by_ts.into_iter().map(|t| t.into()));
    }
    if let Some((min, max)) = filter_by_value {
      args.push(static_val!(FILTER_BY_VALUE));
      args.extend([min.into(), max.into()]);
    }
    add_get_labels(&mut args, labels);
    add_count(&mut args, count)?;
    add_range_aggregation(&mut args, aggregation)?;
    add_filters(&mut args, filters);
    add_groupby(&mut args, group_by);

    Ok((RedisCommandKind::TsMRevRange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_queryindex<C: ClientLike>(client: &C, filters: Vec<Str>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    Ok((
      RedisCommandKind::TsQueryIndex,
      filters.into_iter().map(|v| v.into()).collect(),
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_range<C: ClientLike>(
  client: &C,
  key: RedisKey,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<i64>,
  filter_by_value: Option<(i64, i64)>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(14 + filter_by_ts.len());
    args.push(key.into());
    args.extend([from.to_value(), to.to_value()]);

    if latest {
      args.push(static_val!(LATEST));
    }
    if !filter_by_ts.is_empty() {
      args.push(static_val!(FILTER_BY_TS));
      args.extend(filter_by_ts.into_iter().map(|v| v.into()));
    }
    if let Some((min, max)) = filter_by_value {
      args.push(static_val!(FILTER_BY_VALUE));
      args.extend([min.into(), max.into()]);
    }
    add_count(&mut args, count)?;
    add_range_aggregation(&mut args, aggregation)?;

    Ok((RedisCommandKind::TsRange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ts_revrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  from: GetTimestamp,
  to: GetTimestamp,
  latest: bool,
  filter_by_ts: Vec<i64>,
  filter_by_value: Option<(i64, i64)>,
  count: Option<u64>,
  aggregation: Option<RangeAggregation>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(14 + filter_by_ts.len());
    args.push(key.into());
    args.extend([from.to_value(), to.to_value()]);

    if latest {
      args.push(static_val!(LATEST));
    }
    if !filter_by_ts.is_empty() {
      args.push(static_val!(FILTER_BY_TS));
      args.extend(filter_by_ts.into_iter().map(|v| v.into()));
    }
    if let Some((min, max)) = filter_by_value {
      args.push(static_val!(FILTER_BY_VALUE));
      args.extend([min.into(), max.into()]);
    }
    add_count(&mut args, count)?;
    add_range_aggregation(&mut args, aggregation)?;

    Ok((RedisCommandKind::TsRevRange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
