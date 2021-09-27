use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

pub async fn blpop<K>(inner: &Arc<RedisClientInner>, keys: K, timeout: f64) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len() + 1);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout);

    Ok((RedisCommandKind::BlPop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn brpop<K>(inner: &Arc<RedisClientInner>, keys: K, timeout: f64) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len() + 1);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout);

    Ok((RedisCommandKind::BrPop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn brpoplpush<S, D>(
  inner: &Arc<RedisClientInner>,
  source: S,
  destination: D,
  timeout: f64,
) -> Result<RedisValue, RedisError>
where
  S: Into<RedisKey>,
  D: Into<RedisKey>,
{
  let (source, destination) = (source.into(), destination.into());
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::BrPopLPush,
      vec![source.into(), destination.into(), timeout],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn blmove<S, D>(
  inner: &Arc<RedisClientInner>,
  source: S,
  destination: D,
  source_direction: LMoveDirection,
  destination_direction: LMoveDirection,
  timeout: f64,
) -> Result<RedisValue, RedisError>
where
  S: Into<RedisKey>,
  D: Into<RedisKey>,
{
  let (source, destination) = (source.into(), destination.into());
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(inner, move || {
    let args = vec![
      source.into(),
      destination.into(),
      source_direction.to_str().into(),
      destination_direction.to_str().into(),
      timeout,
    ];

    Ok((RedisCommandKind::BlMove, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lindex<K>(inner: &Arc<RedisClientInner>, key: K, index: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let args: Vec<RedisValue> = vec![key.into().into(), index.into()];
  args_value_cmd(inner, RedisCommandKind::LIndex, args).await
}

pub async fn linsert<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  location: ListLocation,
  pivot: RedisValue,
  element: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::LInsert,
      vec![key.into(), location.to_str().into(), pivot, element],
    ))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn llen<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::LLen, key.into().into()).await
}

pub async fn lpop<K>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(count.try_into()?);
    }

    Ok((RedisCommandKind::LPop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lpos<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  element: RedisValue,
  rank: Option<i64>,
  count: Option<i64>,
  maxlen: Option<i64>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(8);
    args.push(key.into());
    args.push(element);

    if let Some(rank) = rank {
      args.push(RANK.into());
      args.push(rank.into());
    }
    if let Some(count) = count {
      args.push(COUNT.into());
      args.push(count.into());
    }
    if let Some(maxlen) = maxlen {
      args.push(MAXLEN.into());
      args.push(maxlen.into());
    }

    Ok((RedisCommandKind::LPos, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lpush<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.inner().into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::LPush, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn lpushx<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.inner().into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::LPushX, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn lrange<K>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, start, stop) = (key.into().into(), start.into(), stop.into());
  args_values_cmd(inner, RedisCommandKind::LRange, vec![key, start, stop]).await
}

pub async fn lrem<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  count: i64,
  element: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, count) = (key.into(), count.into());
  args_value_cmd(inner, RedisCommandKind::LRem, vec![key.into(), count, element]).await
}

pub async fn lset<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  index: i64,
  element: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let args = vec![key.into().into(), index.into(), element];
  args_value_cmd(inner, RedisCommandKind::LSet, args).await
}

pub async fn ltrim<K>(inner: &Arc<RedisClientInner>, key: K, start: i64, stop: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let args = vec![key.into().into(), start.into(), stop.into()];
  args_value_cmd(inner, RedisCommandKind::LTrim, args).await
}

pub async fn rpop<K>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(count.try_into()?);
    }

    Ok((RedisCommandKind::Rpop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn rpoplpush<S, D>(inner: &Arc<RedisClientInner>, source: S, dest: D) -> Result<RedisValue, RedisError>
where
  S: Into<RedisKey>,
  D: Into<RedisKey>,
{
  let args = vec![source.into().into(), dest.into().into()];
  args_value_cmd(inner, RedisCommandKind::Rpoplpush, args).await
}

pub async fn lmove<S, D>(
  inner: &Arc<RedisClientInner>,
  source: S,
  dest: D,
  source_direction: LMoveDirection,
  dest_direction: LMoveDirection,
) -> Result<RedisValue, RedisError>
where
  S: Into<RedisKey>,
  D: Into<RedisKey>,
{
  let (source, dest) = (source.into(), dest.into());
  let frame = utils::request_response(inner, move || {
    let args = vec![
      source.into(),
      dest.into(),
      source_direction.to_str().into(),
      dest_direction.to_str().into(),
    ];

    Ok((RedisCommandKind::LMove, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn rpush<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.inner().into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::Rpush, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn rpushx<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.inner().into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::Rpushx, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
