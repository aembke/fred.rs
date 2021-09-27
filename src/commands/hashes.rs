use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

pub async fn hdel<K, F>(inner: &Arc<RedisClientInner>, key: K, fields: F) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<MultipleKeys>,
{
  let (key, fields) = (key.into(), fields.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + fields.len());
    args.push(key.into());

    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HDel, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hexists<K, F>(inner: &Arc<RedisClientInner>, key: K, field: F) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<RedisKey>,
{
  let args: Vec<RedisValue> = vec![key.into().into(), field.into().into()];
  args_value_cmd(inner, RedisCommandKind::HExists, args).await
}

pub async fn hget<K, F>(inner: &Arc<RedisClientInner>, key: K, field: F) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<RedisKey>,
{
  let args: Vec<RedisValue> = vec![key.into().into(), field.into().into()];
  args_value_cmd(inner, RedisCommandKind::HGet, args).await
}

pub async fn hgetall<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::HGetAll, vec![key.into()]))).await?;
  Ok(RedisValue::Map(protocol_utils::frame_to_map(frame)?))
}

pub async fn hincrby<K, F>(
  inner: &Arc<RedisClientInner>,
  key: K,
  field: F,
  increment: i64,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<RedisKey>,
{
  let args: Vec<RedisValue> = vec![key.into().into(), field.into().into(), increment.into()];
  args_value_cmd(inner, RedisCommandKind::HIncrBy, args).await
}

pub async fn hincrbyfloat<K, F>(
  inner: &Arc<RedisClientInner>,
  key: K,
  field: F,
  increment: f64,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<RedisKey>,
{
  let args: Vec<RedisValue> = vec![key.into().into(), field.into().into(), increment.try_into()?];
  args_value_cmd(inner, RedisCommandKind::HIncrByFloat, args).await
}

pub async fn hkeys<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::HKeys, vec![key.into()]))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn hlen<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::HLen, key.into().into()).await
}

pub async fn hmget<K, F>(inner: &Arc<RedisClientInner>, key: K, fields: F) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<MultipleKeys>,
{
  let (key, fields) = (key.into(), fields.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + fields.len());
    args.push(key.into());

    for field in fields.inner().into_iter() {
      args.push(field.into());
    }
    Ok((RedisCommandKind::HMGet, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hmset<K, V>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  V: Into<RedisMap>,
{
  let (key, values) = (key.into(), values.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + (values.len() * 2));
    args.push(key.into());

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }
    Ok((RedisCommandKind::HMSet, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hset<K, V>(inner: &Arc<RedisClientInner>, key: K, values: V) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  V: Into<RedisMap>,
{
  let (key, values) = (key.into(), values.into());

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + (values.len() * 2));
    args.push(key.into());

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::HSet, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hsetnx<K, F>(
  inner: &Arc<RedisClientInner>,
  key: K,
  field: F,
  value: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<RedisKey>,
{
  let (key, field) = (key.into(), field.into());

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HSetNx, vec![key.into(), field.into(), value]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hrandfield<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  count: Option<(i64, bool)>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (has_count, has_values) = count.as_ref().map(|(_c, b)| (true, *b)).unwrap_or((false, false));
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(3);
    args.push(key.into());

    if let Some((count, with_values)) = count {
      args.push(count.into());
      if with_values {
        args.push(WITH_VALUES.into());
      }
    }

    Ok((RedisCommandKind::HRandField, args))
  })
  .await?;

  if has_count {
    if has_values {
      protocol_utils::frame_to_map(frame).map(|m| RedisValue::Map(m))
    } else {
      protocol_utils::frame_to_results(frame)
    }
  } else {
    protocol_utils::frame_to_single_result(frame)
  }
}

pub async fn hstrlen<K, F>(inner: &Arc<RedisClientInner>, key: K, field: F) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  F: Into<RedisKey>,
{
  let (key, field) = (key.into(), field.into());

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::HStrLen, vec![key.into(), field.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hvals<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_values_cmd(inner, RedisCommandKind::HVals, key.into().into()).await
}
