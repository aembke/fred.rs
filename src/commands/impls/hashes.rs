use super::*;
use crate::{
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};
use std::{convert::TryInto, sync::Arc};

pub async fn hdel<C: ClientLike>(client: C, key: RedisKey, fields: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn hexists<C: ClientLike>(client: C, key: RedisKey, field: RedisKey) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into()];
  args_value_cmd(client, RedisCommandKind::HExists, args).await
}

pub async fn hget<C: ClientLike>(client: C, key: RedisKey, field: RedisKey) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into()];
  args_value_cmd(client, RedisCommandKind::HGet, args).await
}

pub async fn hgetall<C: ClientLike>(client: C, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::HGetAll, vec![key.into()]))).await?;
  Ok(RedisValue::Map(protocol_utils::frame_to_map(frame)?))
}

pub async fn hincrby<C: ClientLike>(
  client: C,
  key: RedisKey,
  field: RedisKey,
  increment: i64,
) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into(), increment.into()];
  args_value_cmd(client, RedisCommandKind::HIncrBy, args).await
}

pub async fn hincrbyfloat<C: ClientLike>(
  client: C,
  key: RedisKey,
  field: RedisKey,
  increment: f64,
) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into(), increment.try_into()?];
  args_value_cmd(client, RedisCommandKind::HIncrByFloat, args).await
}

pub async fn hkeys<C: ClientLike>(client: C, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::HKeys, vec![key.into()]))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn hlen<C: ClientLike>(client: C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::HLen, key.into()).await
}

pub async fn hmget<C: ClientLike>(client: C, key: RedisKey, fields: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn hmset<C: ClientLike>(client: C, key: RedisKey, values: RedisMap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn hset<C: ClientLike>(client: C, key: RedisKey, values: RedisMap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn hsetnx<C: ClientLike>(
  client: C,
  key: RedisKey,
  field: RedisKey,
  value: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::HSetNx, vec![key.into(), field.into(), value]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hrandfield<C: ClientLike>(
  client: C,
  key: RedisKey,
  count: Option<(i64, bool)>,
) -> Result<RedisValue, RedisError> {
  let (has_count, has_values) = count.as_ref().map(|(_c, b)| (true, *b)).unwrap_or((false, false));

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(3);
    args.push(key.into());

    if let Some((count, with_values)) = count {
      args.push(count.into());
      if with_values {
        args.push(static_val!(WITH_VALUES));
      }
    }

    Ok((RedisCommandKind::HRandField, args))
  })
  .await?;

  if has_count {
    if has_values {
      let frame = protocol_utils::flatten_frame(frame);
      protocol_utils::frame_to_map(frame).map(|m| RedisValue::Map(m))
    } else {
      protocol_utils::frame_to_results(frame)
    }
  } else {
    protocol_utils::frame_to_single_result(frame)
  }
}

pub async fn hstrlen<C: ClientLike>(client: C, key: RedisKey, field: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::HStrLen, vec![key.into(), field.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hvals<C: ClientLike>(client: C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(client, RedisCommandKind::HVals, key.into()).await
}
