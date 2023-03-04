use super::*;
use crate::{
  error::*,
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};
use std::convert::TryInto;

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

pub async fn bzmpop<C: ClientLike>(
  client: &C,
  timeout: f64,
  keys: MultipleKeys,
  sort: ZCmp,
  count: Option<i64>,
) -> Result<RedisValue, RedisError> {
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len() + 4);
    args.push(timeout);
    args.push(keys.len().try_into()?);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(sort.to_str().into());
    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.into());
    }

    Ok((RedisCommandKind::BzmPop, args))
  })
  .await?;

  let _ = protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn bzpopmin<C: ClientLike>(client: &C, keys: MultipleKeys, timeout: f64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout.try_into()?);

    Ok((RedisCommandKind::BzPopMin, args))
  })
  .await?;

  let _ = protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn bzpopmax<C: ClientLike>(client: &C, keys: MultipleKeys, timeout: f64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout.try_into()?);

    Ok((RedisCommandKind::BzPopMax, args))
  })
  .await?;

  let _ = protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn zadd<C: ClientLike>(
  client: &C,
  key: RedisKey,
  options: Option<SetOptions>,
  ordering: Option<Ordering>,
  changed: bool,
  incr: bool,
  values: MultipleZaddValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(5 + (values.len() * 2));
    args.push(key.into());

    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    if let Some(ordering) = ordering {
      args.push(ordering.to_str().into());
    }
    if changed {
      args.push(static_val!(CHANGED));
    }
    if incr {
      args.push(static_val!(INCR));
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

pub async fn zcard<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Zcard, key.into()).await
}

pub async fn zcount<C: ClientLike>(client: &C, key: RedisKey, min: f64, max: f64) -> Result<RedisValue, RedisError> {
  let (min, max) = (min.try_into()?, max.try_into()?);
  args_value_cmd(client, RedisCommandKind::Zcount, vec![key.into(), min, max]).await
}

pub async fn zdiff<C: ClientLike>(
  client: &C,
  keys: MultipleKeys,
  withscores: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2 + keys.len());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if withscores {
      args.push(static_val!(WITH_SCORES));
    }

    Ok((RedisCommandKind::Zdiff, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zdiffstore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  keys: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn zincrby<C: ClientLike>(
  client: &C,
  key: RedisKey,
  increment: f64,
  member: RedisValue,
) -> Result<RedisValue, RedisError> {
  let increment = increment.try_into()?;
  let args = vec![key.into(), increment, member];
  args_value_cmd(client, RedisCommandKind::Zincrby, args).await
}

pub async fn zinter<C: ClientLike>(
  client: &C,
  keys: MultipleKeys,
  weights: MultipleWeights,
  aggregate: Option<AggregateOptions>,
  withscores: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args_len = 6 + keys.len() + weights.len();
    let mut args = Vec::with_capacity(args_len);
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(static_val!(WEIGHTS));
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }
    if let Some(options) = aggregate {
      args.push(static_val!(AGGREGATE));
      args.push(options.to_str().into());
    }
    if withscores {
      args.push(static_val!(WITH_SCORES));
    }

    Ok((RedisCommandKind::Zinter, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zinterstore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  keys: MultipleKeys,
  weights: MultipleWeights,
  aggregate: Option<AggregateOptions>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args_len = 5 + keys.len() + weights.len();
    let mut args = Vec::with_capacity(args_len);
    args.push(dest.into());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(static_val!(WEIGHTS));
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }
    if let Some(options) = aggregate {
      args.push(static_val!(AGGREGATE));
      args.push(options.to_str().into());
    }

    Ok((RedisCommandKind::Zinterstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zlexcount<C: ClientLike>(
  client: &C,
  key: RedisKey,
  min: ZRange,
  max: ZRange,
) -> Result<RedisValue, RedisError> {
  let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

  let args = vec![key.into(), min.into_value()?, max.into_value()?];
  args_value_cmd(client, RedisCommandKind::Zlexcount, args).await
}

pub async fn zpopmax<C: ClientLike>(
  client: &C,
  key: RedisKey,
  count: Option<usize>,
) -> Result<RedisValue, RedisError> {
  let args = if let Some(count) = count {
    vec![key.into(), count.try_into()?]
  } else {
    vec![key.into()]
  };

  args_values_cmd(client, RedisCommandKind::Zpopmax, args).await
}

pub async fn zpopmin<C: ClientLike>(
  client: &C,
  key: RedisKey,
  count: Option<usize>,
) -> Result<RedisValue, RedisError> {
  let args = if let Some(count) = count {
    vec![key.into(), count.try_into()?]
  } else {
    vec![key.into()]
  };

  args_values_cmd(client, RedisCommandKind::Zpopmin, args).await
}

pub async fn zmpop<C: ClientLike>(
  client: &C,
  keys: MultipleKeys,
  sort: ZCmp,
  count: Option<i64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len() + 3);
    args.push(keys.len().try_into()?);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(sort.to_str().into());
    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zmpop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrandmember<C: ClientLike>(
  client: &C,
  key: RedisKey,
  count: Option<(i64, bool)>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(3);
    args.push(key.into());

    if let Some((count, withscores)) = count {
      args.push(count.into());
      if withscores {
        args.push(static_val!(WITH_SCORES));
      }
    }

    Ok((RedisCommandKind::Zrandmember, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrangestore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  source: RedisKey,
  min: ZRange,
  max: ZRange,
  sort: Option<ZSort>,
  rev: bool,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError> {
  let _ = check_range_types(&min, &max, &sort)?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(9);
    args.push(dest.into());
    args.push(source.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if let Some(sort) = sort {
      args.push(sort.to_str().into());
    }
    if rev {
      args.push(static_val!(REV));
    }
    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrangestore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  min: ZRange,
  max: ZRange,
  sort: Option<ZSort>,
  rev: bool,
  limit: Option<Limit>,
  withscores: bool,
) -> Result<RedisValue, RedisError> {
  let _ = check_range_types(&min, &max, &sort)?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(9);
    args.push(key.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if let Some(sort) = sort {
      args.push(sort.to_str().into());
    }
    if rev {
      args.push(static_val!(REV));
    }
    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }
    if withscores {
      args.push(static_val!(WITH_SCORES));
    }

    Ok((RedisCommandKind::Zrange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrangebylex<C: ClientLike>(
  client: &C,
  key: RedisKey,
  min: ZRange,
  max: ZRange,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError> {
  let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrangebylex, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrevrangebylex<C: ClientLike>(
  client: &C,
  key: RedisKey,
  max: ZRange,
  min: ZRange,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError> {
  let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    args.push(max.into_value()?);
    args.push(min.into_value()?);

    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrevrangebylex, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrangebyscore<C: ClientLike>(
  client: &C,
  key: RedisKey,
  min: ZRange,
  max: ZRange,
  withscores: bool,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key.into());
    args.push(min.into_value()?);
    args.push(max.into_value()?);

    if withscores {
      args.push(static_val!(WITH_SCORES));
    }
    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrangebyscore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrevrangebyscore<C: ClientLike>(
  client: &C,
  key: RedisKey,
  max: ZRange,
  min: ZRange,
  withscores: bool,
  limit: Option<Limit>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key.into());
    args.push(max.into_value()?);
    args.push(min.into_value()?);

    if withscores {
      args.push(static_val!(WITH_SCORES));
    }
    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }

    Ok((RedisCommandKind::Zrevrangebyscore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrank<C: ClientLike>(client: &C, key: RedisKey, member: RedisValue) -> Result<RedisValue, RedisError> {
  args_value_cmd(client, RedisCommandKind::Zrank, vec![key.into(), member]).await
}

pub async fn zrem<C: ClientLike>(
  client: &C,
  key: RedisKey,
  members: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn zremrangebylex<C: ClientLike>(
  client: &C,
  key: RedisKey,
  min: ZRange,
  max: ZRange,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let _ = check_range_types(&min, &max, &Some(ZSort::ByLex))?;

    Ok((RedisCommandKind::Zremrangebylex, vec![
      key.into(),
      min.into_value()?,
      max.into_value()?,
    ]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zremrangebyrank<C: ClientLike>(
  client: &C,
  key: RedisKey,
  start: i64,
  stop: i64,
) -> Result<RedisValue, RedisError> {
  let (start, stop) = (start.into(), stop.into());
  args_value_cmd(client, RedisCommandKind::Zremrangebyrank, vec![key.into(), start, stop]).await
}

pub async fn zremrangebyscore<C: ClientLike>(
  client: &C,
  key: RedisKey,
  min: ZRange,
  max: ZRange,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let _ = check_range_types(&min, &max, &Some(ZSort::ByScore))?;

    Ok((RedisCommandKind::Zremrangebyscore, vec![
      key.into(),
      min.into_value()?,
      max.into_value()?,
    ]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zrevrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  start: i64,
  stop: i64,
  withscores: bool,
) -> Result<RedisValue, RedisError> {
  let (start, stop) = (start.into(), stop.into());
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(start);
    args.push(stop);

    if withscores {
      args.push(static_val!(WITH_SCORES));
    }

    Ok((RedisCommandKind::Zrevrange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zrevrank<C: ClientLike>(
  client: &C,
  key: RedisKey,
  member: RedisValue,
) -> Result<RedisValue, RedisError> {
  args_value_cmd(client, RedisCommandKind::Zrevrank, vec![key.into(), member]).await
}

pub async fn zscore<C: ClientLike>(client: &C, key: RedisKey, member: RedisValue) -> Result<RedisValue, RedisError> {
  args_value_cmd(client, RedisCommandKind::Zscore, vec![key.into(), member]).await
}

pub async fn zunion<C: ClientLike>(
  client: &C,
  keys: MultipleKeys,
  weights: MultipleWeights,
  aggregate: Option<AggregateOptions>,
  withscores: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args_len = keys.len() + weights.len();
    let mut args = Vec::with_capacity(5 + args_len);
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(static_val!(WEIGHTS));
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }

    if let Some(aggregate) = aggregate {
      args.push(static_val!(AGGREGATE));
      args.push(aggregate.to_str().into());
    }
    if withscores {
      args.push(static_val!(WITH_SCORES));
    }

    Ok((RedisCommandKind::Zunion, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn zunionstore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  keys: MultipleKeys,
  weights: MultipleWeights,
  aggregate: Option<AggregateOptions>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args_len = keys.len() + weights.len();
    let mut args = Vec::with_capacity(5 + args_len);
    args.push(dest.into());
    args.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    if weights.len() > 0 {
      args.push(static_val!(WEIGHTS));
      for weight in weights.inner().into_iter() {
        args.push(weight.try_into()?);
      }
    }

    if let Some(aggregate) = aggregate {
      args.push(static_val!(AGGREGATE));
      args.push(aggregate.to_str().into());
    }

    Ok((RedisCommandKind::Zunionstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn zmscore<C: ClientLike>(
  client: &C,
  key: RedisKey,
  members: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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
