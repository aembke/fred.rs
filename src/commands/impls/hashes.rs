use super::*;
use crate::{
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};
use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, Resp3Frame as _Resp3Frame};
use std::{convert::TryInto, str};

pub static FIELDS: &str = "FIELDS";

fn frame_is_queued(frame: &Resp3Frame) -> bool {
  match frame {
    Resp3Frame::SimpleString { ref data, .. } | Resp3Frame::BlobString { ref data, .. } => {
      str::from_utf8(data).ok().map(|s| s == QUEUED).unwrap_or(false)
    },
    _ => false,
  }
}

pub async fn hdel<C: ClientLike>(client: &C, key: RedisKey, fields: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + fields.len());
    args.push(key.into());

    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HDel, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hexists<C: ClientLike>(client: &C, key: RedisKey, field: RedisKey) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into()];
  args_value_cmd(client, RedisCommandKind::HExists, args).await
}

pub async fn hget<C: ClientLike>(client: &C, key: RedisKey, field: RedisKey) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into()];
  args_value_cmd(client, RedisCommandKind::HGet, args).await
}

pub async fn hgetall<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::HGetAll, vec![key.into()]))).await?;

  if frame.as_str().map(|s| s == QUEUED).unwrap_or(false) {
    protocol_utils::frame_to_results(frame)
  } else {
    Ok(RedisValue::Map(protocol_utils::frame_to_map(frame)?))
  }
}

pub async fn hincrby<C: ClientLike>(
  client: &C,
  key: RedisKey,
  field: RedisKey,
  increment: i64,
) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into(), increment.into()];
  args_value_cmd(client, RedisCommandKind::HIncrBy, args).await
}

pub async fn hincrbyfloat<C: ClientLike>(
  client: &C,
  key: RedisKey,
  field: RedisKey,
  increment: f64,
) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), field.into(), increment.try_into()?];
  args_value_cmd(client, RedisCommandKind::HIncrByFloat, args).await
}

pub async fn hkeys<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::HKeys, vec![key.into()]))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn hlen<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::HLen, key.into()).await
}

pub async fn hmget<C: ClientLike>(client: &C, key: RedisKey, fields: MultipleKeys) -> Result<RedisValue, RedisError> {
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

pub async fn hmset<C: ClientLike>(client: &C, key: RedisKey, values: RedisMap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + (values.len() * 2));
    args.push(key.into());

    for (key, value) in values.inner().into_iter().filter(|x| !x.1.is_null()) {
      args.push(key.into());
      args.push(value);
    }
    Ok((RedisCommandKind::HMSet, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hset<C: ClientLike>(client: &C, key: RedisKey, values: RedisMap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + (values.len() * 2));
    args.push(key.into());

    for (key, value) in values.inner().into_iter().filter(|x| !x.1.is_null()) {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::HSet, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hsetnx<C: ClientLike>(
  client: &C,
  key: RedisKey,
  field: RedisKey,
  value: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::HSetNx, vec![key.into(), field.into(), value]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hrandfield<C: ClientLike>(
  client: &C,
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
    if has_values && frame.as_str().map(|s| s != QUEUED).unwrap_or(true) {
      let frame = protocol_utils::flatten_frame(frame);
      protocol_utils::frame_to_map(frame).map(RedisValue::Map)
    } else {
      protocol_utils::frame_to_results(frame)
    }
  } else {
    protocol_utils::frame_to_results(frame)
  }
}

pub async fn hstrlen<C: ClientLike>(client: &C, key: RedisKey, field: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::HStrLen, vec![key.into(), field.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hvals<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(client, RedisCommandKind::HVals, key.into()).await
}

pub async fn httl<C: ClientLike>(client: &C, key: RedisKey, fields: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 3);
    args.extend([key.into(), static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HTtl, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hexpire<C: ClientLike>(
  client: &C,
  key: RedisKey,
  seconds: i64,
  options: Option<ExpireOptions>,
  fields: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 4);
    args.extend([key.into(), seconds.into()]);
    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    args.extend([static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HExpire, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hexpire_at<C: ClientLike>(
  client: &C,
  key: RedisKey,
  time: i64,
  options: Option<ExpireOptions>,
  fields: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 4);
    args.extend([key.into(), time.into()]);
    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    args.extend([static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HExpireAt, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hexpire_time<C: ClientLike>(
  client: &C,
  key: RedisKey,
  fields: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 3);
    args.extend([key.into(), static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HExpireAt, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hpttl<C: ClientLike>(client: &C, key: RedisKey, fields: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 3);
    args.extend([key.into(), static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HPTtl, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hpexpire<C: ClientLike>(
  client: &C,
  key: RedisKey,
  milliseconds: i64,
  options: Option<ExpireOptions>,
  fields: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 4);
    args.extend([key.into(), milliseconds.into()]);
    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    args.extend([static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HPExpire, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hpexpire_at<C: ClientLike>(
  client: &C,
  key: RedisKey,
  time: i64,
  options: Option<ExpireOptions>,
  fields: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 4);
    args.extend([key.into(), time.into()]);
    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    args.extend([static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HPExpireAt, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn hpexpire_time<C: ClientLike>(
  client: &C,
  key: RedisKey,
  fields: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(fields.len() + 3);
    args.extend([key.into(), static_val!(FIELDS), fields.len().try_into()?]);
    for field in fields.inner().into_iter() {
      args.push(field.into());
    }

    Ok((RedisCommandKind::HPExpireTime, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
