use super::*;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::str;
use std::sync::Arc;

fn new_range_error(kind: &Option<ZSort>) -> Result<(), RedisError> {
  if let Some(ref sort) = *kind {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      format!("Invalid range bound with {} sort", sort.to_str()),
    ))
  } else {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Invalid index range bound.",
    ))
  }
}

fn check_range_type(range: &ZRange, kind: &Option<ZSort>) -> Result<(), RedisError> {
  match kind {
    Some(_kind) => match _kind {
      ZSort::ByLex => match range.range {
        ZRangeBound::Lex(_) | ZRangeBound::InfiniteLex | ZRangeBound::NegInfinityLex => Ok(()),
        _ => new_range_error(kind),
      },
      ZSort::ByScore => match range.range {
        ZRangeBound::Score(_) | ZRangeBound::InfiniteScore | ZRangeBound::NegInfiniteScore => Ok(()),
        _ => new_range_error(kind),
      },
    },
    None => match range.range {
      ZRangeBound::Index(_) => Ok(()),
      _ => new_range_error(kind),
    },
  }
}

fn check_range_types(min: &ZRange, max: &ZRange, kind: &Option<ZSort>) -> Result<(), RedisError> {
  let _ = check_range_type(min, kind)?;
  let _ = check_range_type(max, kind)?;
  Ok(())
}

fn bytes_to_f64(b: &[u8]) -> Result<f64, RedisError> {
  str::from_utf8(b)
    .map_err(|e| e.into())
    .and_then(|s| s.parse::<f64>().map_err(|e| e.into()))
}

fn frames_to_bzpop_result(mut frames: Vec<Frame>) -> Result<Option<(RedisKey, RedisValue, f64)>, RedisError> {
  if frames.len() != 3 {
    return Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected 3 element array.",
    ));
  }
  let score_frame = frames.pop().unwrap();
  let value_frame = frames.pop().unwrap();
  let key_frame = frames.pop().unwrap();

  let score = match score_frame {
    Frame::BulkString(b) => bytes_to_f64(&b)?,
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected bulk string score.",
      ))
    }
  };
  let value = protocol_utils::frame_to_results(value_frame)?;
  let key = match key_frame {
    Frame::BulkString(b) => String::from_utf8(b)?.into(),
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected bulk string key,",
      ))
    }
  };

  Ok(Some((key, value, score)))
}

pub async fn bzpopmin<K>(
  inner: &Arc<RedisClientInner>,
  keys: K,
  timeout: f64,
) -> Result<Option<(RedisKey, RedisValue, f64)>, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout.try_into()?);

    Ok((RedisCommandKind::BzPopMin, args))
  })
  .await?;

  if let Frame::Array(frames) = frame {
    frames_to_bzpop_result(frames)
  } else {
    if frame.is_null() {
      Ok(None)
    } else {
      Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected nil or array."))
    }
  }
}

pub async fn bzpopmax<K>(
  inner: &Arc<RedisClientInner>,
  keys: K,
  timeout: f64,
) -> Result<Option<(RedisKey, RedisValue, f64)>, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout.try_into()?);

    Ok((RedisCommandKind::BzPopMax, args))
  })
  .await?;

  if let Frame::Array(frames) = frame {
    frames_to_bzpop_result(frames)
  } else {
    if frame.is_null() {
      Ok(None)
    } else {
      Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected nil or array."))
    }
  }
}

pub async fn zadd<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  options: Option<SetOptions>,
  ordering: Option<Ordering>,
  changed: bool,
  incr: bool,
  values: MultipleZaddValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(5 + (values.len() * 2));
    args.push(key.into());

    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    if let Some(ordering) = ordering {
      args.push(ordering.to_str().into());
    }
    if changed {
      args.push(CHANGED.into());
    }
    if incr {
      args.push(INCR.into());
    }

    for (score, value) in values.inner().into_iter() {
      args.push(score.try_into()?);
      args.push(value);
    }

    Ok((RedisCommandKind::Zadd, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zcard<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Zcard, key.into().into()).await
}

pub async fn zcount<K>(inner: &Arc<RedisClientInner>, key: K, min: f64, max: f64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, min, max) = (key.into(), min.try_into()?, max.try_into()?);
  args_value_cmd(inner, RedisCommandKind::Zcount, vec![key.into(), min, max]).await
}

pub async fn zdiff<K>(inner: &Arc<RedisClientInner>, keys: K, withscores: bool) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2 + keys.len());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if withscores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zdiff, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zdiffstore<D, K>(inner: &Arc<RedisClientInner>, dest: D, keys: K) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  K: Into<MultipleKeys>,
{
  let (dest, keys) = (dest.into(), keys.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2 + keys.len());
    args.push(dest.into());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Zdiffstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zincrby<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  increment: f64,
  member: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, increment) = (key.into(), increment.try_into()?);
  let args = vec![key.into(), increment, member];
  args_value_cmd(inner, RedisCommandKind::Zincrby, args).await
}

pub async fn zinter<K, W>(
  inner: &Arc<RedisClientInner>,
  keys: K,
  weights: W,
  aggregate: Option<AggregateOptions>,
  withscores: bool,
) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
  W: Into<MultipleWeights>,
{
  let (keys, weights) = (keys.into(), weights.into());
  let frame = utils::request_response(inner, move || {
    let args_len = 6 + keys.len() + weights.len();
    let mut args = Vec::with_capacity(args_len);
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(WEIGHTS.into());
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }
    if let Some(options) = aggregate {
      args.push(AGGREGATE.into());
      args.push(options.to_str().into());
    }
    if withscores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zinter, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zinterstore<D, K, W>(
  inner: &Arc<RedisClientInner>,
  dest: D,
  keys: K,
  weights: W,
  aggregate: Option<AggregateOptions>,
) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  K: Into<MultipleKeys>,
  W: Into<MultipleWeights>,
{
  let (dest, keys, weights) = (dest.into(), keys.into(), weights.into());
  let frame = utils::request_response(inner, move || {
    let args_len = 5 + keys.len() + weights.len();
    let mut args = Vec::with_capacity(args_len);
    args.push(dest.into());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(WEIGHTS.into());
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }
    if let Some(options) = aggregate {
      args.push(AGGREGATE.into());
      args.push(options.to_str().into());
    }

    Ok((RedisCommandKind::Zinterstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zlexcount<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  min: ZRange,
  max: ZRange,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

  let args = vec![key.into(), min.into_value()?, max.into_value()?];
  args_value_cmd(inner, RedisCommandKind::Zlexcount, args).await
}

pub async fn zpopmax<K>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let args = if let Some(count) = count {
    vec![key.into().into(), count.try_into()?]
  } else {
    vec![key.into().into()]
  };

  args_values_cmd(inner, RedisCommandKind::Zpopmax, args).await
}

pub async fn zpopmin<K>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let args = if let Some(count) = count {
    vec![key.into().into(), count.try_into()?]
  } else {
    vec![key.into().into()]
  };

  args_values_cmd(inner, RedisCommandKind::Zpopmin, args).await
}

pub async fn zrandmember<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  count: Option<(i64, bool)>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(3);
    args.push(key.into());

    if let Some((count, withscores)) = count {
      args.push(count.into());
      if withscores {
        args.push(WITH_SCORES.into());
      }
    }

    Ok((RedisCommandKind::Zrandmember, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrangestore<D, S>(
  inner: &Arc<RedisClientInner>,
  dest: D,
  source: S,
  min: ZRange,
  max: ZRange,
  sort: Option<ZSort>,
  rev: bool,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  S: Into<RedisKey>,
{
  let (dest, source) = (dest.into(), source.into());
  let _ = check_range_types(&min, &max, &sort)?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(9);
    args.push(dest.into());
    args.push(source.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if let Some(sort) = sort {
      args.push(sort.to_str().into());
    }
    if rev {
      args.push(REV.into());
    }
    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrangestore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zrange<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  min: ZRange,
  max: ZRange,
  sort: Option<ZSort>,
  rev: bool,
  limit: Option<Limit>,
  withscores: bool,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let _ = check_range_types(&min, &max, &sort)?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(9);
    args.push(key.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if let Some(sort) = sort {
      args.push(sort.to_str().into());
    }
    if rev {
      args.push(REV.into());
    }
    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(offset.into());
      args.push(count.into());
    }
    if withscores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zrange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrangebylex<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  min: ZRange,
  max: ZRange,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrangebylex, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrevrangebylex<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  max: ZRange,
  min: ZRange,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    args.push(max.into_value()?);
    args.push(min.into_value()?);

    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrevrangebylex, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrangebyscore<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  min: ZRange,
  max: ZRange,
  withscores: bool,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if withscores {
      args.push(WITH_SCORES.into());
    }
    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrangebyscore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrevrangebyscore<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  max: ZRange,
  min: ZRange,
  withscores: bool,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key.into());
    args.push(max.into_value()?);
    args.push(min.into_value()?);

    if withscores {
      args.push(WITH_SCORES.into());
    }
    if let Some((offset, count)) = limit {
      args.push(LIMIT.into());
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrevrangebyscore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrank<K>(inner: &Arc<RedisClientInner>, key: K, member: RedisValue) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  args_value_cmd(inner, RedisCommandKind::Zrank, vec![key.into().into(), member]).await
}

pub async fn zrem<K>(inner: &Arc<RedisClientInner>, key: K, members: MultipleValues) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + members.len());
    args.push(key.into());

    for member in members.inner().into_iter() {
      args.push(member);
    }
    Ok((RedisCommandKind::Zrem, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zremrangebylex<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  min: ZRange,
  max: ZRange,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

    Ok((
      RedisCommandKind::Zremrangebylex,
      vec![key.into(), min.into_value()?, max.into_value()?],
    ))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zremrangebyrank<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  start: i64,
  stop: i64,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, start, stop) = (key.into(), start.into(), stop.into());
  args_value_cmd(inner, RedisCommandKind::Zremrangebyrank, vec![key.into(), start, stop]).await
}

pub async fn zremrangebyscore<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  min: ZRange,
  max: ZRange,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let _ = check_range_types(&min, &max, &Some(ZSort::ByScore))?;

    Ok((
      RedisCommandKind::Zremrangebyscore,
      vec![key.into(), min.into_value()?, max.into_value()?],
    ))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zrevrange<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  start: i64,
  stop: i64,
  withscores: bool,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, start, stop) = (key.into(), start.into(), stop.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(start);
    args.push(stop);

    if withscores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zrevrange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrevrank<K>(inner: &Arc<RedisClientInner>, key: K, member: RedisValue) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  args_value_cmd(inner, RedisCommandKind::Zrevrank, vec![key.into().into(), member]).await
}

pub async fn zscore<K>(inner: &Arc<RedisClientInner>, key: K, member: RedisValue) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  args_value_cmd(inner, RedisCommandKind::Zscore, vec![key.into().into(), member]).await
}

pub async fn zunion<K, W>(
  inner: &Arc<RedisClientInner>,
  keys: K,
  weights: W,
  aggregate: Option<AggregateOptions>,
  withscores: bool,
) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
  W: Into<MultipleWeights>,
{
  let (keys, weights) = (keys.into(), weights.into());
  let frame = utils::request_response(inner, move || {
    let args_len = keys.len() + weights.len();
    let mut args = Vec::with_capacity(5 + args_len);
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(WEIGHTS.into());
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }

    if let Some(aggregate) = aggregate {
      args.push(AGGREGATE.into());
      args.push(aggregate.to_str().into());
    }
    if withscores {
      args.push(WITH_SCORES.into());
    }

    Ok((RedisCommandKind::Zunion, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zunionstore<D, K, W>(
  inner: &Arc<RedisClientInner>,
  dest: D,
  keys: K,
  weights: W,
  aggregate: Option<AggregateOptions>,
) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  K: Into<MultipleKeys>,
  W: Into<MultipleWeights>,
{
  let (dest, keys, weights) = (dest.into(), keys.into(), weights.into());
  let frame = utils::request_response(inner, move || {
    let args_len = keys.len() + weights.len();
    let mut args = Vec::with_capacity(5 + args_len);
    args.push(dest.into());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(WEIGHTS.into());
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }

    if let Some(aggregate) = aggregate {
      args.push(AGGREGATE.into());
      args.push(aggregate.to_str().into());
    }

    Ok((RedisCommandKind::Zunionstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zmscore<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  members: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + members.len());
    args.push(key.into());

    for member in members.inner().into_iter() {
      args.push(member);
    }
    Ok((RedisCommandKind::Zmscore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
