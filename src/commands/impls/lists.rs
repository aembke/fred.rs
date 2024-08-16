use super::*;
use crate::{
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};
use bytes_utils::Str;
use std::convert::TryInto;

pub async fn sort_ro<C: ClientLike>(
  client: &C,
  key: RedisKey,
  by: Option<Str>,
  limit: Option<Limit>,
  get: MultipleStrings,
  order: Option<SortOrder>,
  alpha: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(8 + get.len() * 2);
    args.push(key.into());

    if let Some(pattern) = by {
      args.push(static_val!("BY"));
      args.push(pattern.into());
    }
    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }
    for pattern in get.inner().into_iter() {
      args.push(static_val!(GET));
      args.push(pattern.into());
    }
    if let Some(order) = order {
      args.push(order.to_str().into());
    }
    if alpha {
      args.push(static_val!("ALPHA"));
    }

    Ok((RedisCommandKind::SortRo, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sort<C: ClientLike>(
  client: &C,
  key: RedisKey,
  by: Option<Str>,
  limit: Option<Limit>,
  get: MultipleStrings,
  order: Option<SortOrder>,
  alpha: bool,
  store: Option<RedisKey>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(10 + get.len() * 2);
    args.push(key.into());

    if let Some(pattern) = by {
      args.push(static_val!("BY"));
      args.push(pattern.into());
    }
    if let Some((offset, count)) = limit {
      args.push(static_val!(LIMIT));
      args.push(offset.into());
      args.push(count.into());
    }
    for pattern in get.inner().into_iter() {
      args.push(static_val!(GET));
      args.push(pattern.into());
    }
    if let Some(order) = order {
      args.push(order.to_str().into());
    }
    if alpha {
      args.push(static_val!("ALPHA"));
    }
    if let Some(dest) = store {
      args.push(static_val!(STORE));
      args.push(dest.into());
    }

    Ok((RedisCommandKind::Sort, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn blmpop<C: ClientLike>(
  client: &C,
  timeout: f64,
  keys: MultipleKeys,
  direction: LMoveDirection,
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
    args.push(direction.to_str().into());
    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.into());
    }

    Ok((RedisCommandKind::BlmPop, args))
  })
  .await?;

  protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn blpop<C: ClientLike>(client: &C, keys: MultipleKeys, timeout: f64) -> Result<RedisValue, RedisError> {
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len() + 1);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout);

    Ok((RedisCommandKind::BlPop, args))
  })
  .await?;

  protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn brpop<C: ClientLike>(client: &C, keys: MultipleKeys, timeout: f64) -> Result<RedisValue, RedisError> {
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len() + 1);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(timeout);

    Ok((RedisCommandKind::BrPop, args))
  })
  .await?;

  protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn brpoplpush<C: ClientLike>(
  client: &C,
  source: RedisKey,
  destination: RedisKey,
  timeout: f64,
) -> Result<RedisValue, RedisError> {
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::BrPopLPush, vec![
      source.into(),
      destination.into(),
      timeout,
    ]))
  })
  .await?;

  protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn blmove<C: ClientLike>(
  client: &C,
  source: RedisKey,
  destination: RedisKey,
  source_direction: LMoveDirection,
  destination_direction: LMoveDirection,
  timeout: f64,
) -> Result<RedisValue, RedisError> {
  let timeout: RedisValue = timeout.try_into()?;

  let frame = utils::request_response(client, move || {
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

  protocol_utils::check_null_timeout(&frame)?;
  protocol_utils::frame_to_results(frame)
}

pub async fn lmpop<C: ClientLike>(
  client: &C,
  keys: MultipleKeys,
  direction: LMoveDirection,
  count: Option<i64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len() + 3);
    args.push(keys.len().try_into()?);
    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    args.push(direction.to_str().into());
    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.into());
    }

    Ok((RedisCommandKind::LMPop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lindex<C: ClientLike>(client: &C, key: RedisKey, index: i64) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = vec![key.into(), index.into()];
  args_value_cmd(client, RedisCommandKind::LIndex, args).await
}

pub async fn linsert<C: ClientLike>(
  client: &C,
  key: RedisKey,
  location: ListLocation,
  pivot: RedisValue,
  element: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::LInsert, vec![
      key.into(),
      location.to_str().into(),
      pivot,
      element,
    ]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn llen<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::LLen, key.into()).await
}

pub async fn lpop<C: ClientLike>(client: &C, key: RedisKey, count: Option<usize>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn lpos<C: ClientLike>(
  client: &C,
  key: RedisKey,
  element: RedisValue,
  rank: Option<i64>,
  count: Option<i64>,
  maxlen: Option<i64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(8);
    args.push(key.into());
    args.push(element);

    if let Some(rank) = rank {
      args.push(static_val!(RANK));
      args.push(rank.into());
    }
    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.into());
    }
    if let Some(maxlen) = maxlen {
      args.push(static_val!(MAXLEN));
      args.push(maxlen.into());
    }

    Ok((RedisCommandKind::LPos, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lpush<C: ClientLike>(
  client: &C,
  key: RedisKey,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let elements = elements.into_multiple_values();
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::LPush, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lpushx<C: ClientLike>(
  client: &C,
  key: RedisKey,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let elements = elements.into_multiple_values();
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::LPushX, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn lrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  start: i64,
  stop: i64,
) -> Result<RedisValue, RedisError> {
  let (key, start, stop) = (key.into(), start.into(), stop.into());
  args_values_cmd(client, RedisCommandKind::LRange, vec![key, start, stop]).await
}

pub async fn lrem<C: ClientLike>(
  client: &C,
  key: RedisKey,
  count: i64,
  element: RedisValue,
) -> Result<RedisValue, RedisError> {
  let (key, count) = (key.into(), count.into());
  args_value_cmd(client, RedisCommandKind::LRem, vec![key, count, element]).await
}

pub async fn lset<C: ClientLike>(
  client: &C,
  key: RedisKey,
  index: i64,
  element: RedisValue,
) -> Result<RedisValue, RedisError> {
  let args = vec![key.into(), index.into(), element];
  args_value_cmd(client, RedisCommandKind::LSet, args).await
}

pub async fn ltrim<C: ClientLike>(
  client: &C,
  key: RedisKey,
  start: i64,
  stop: i64,
) -> Result<RedisValue, RedisError> {
  let args = vec![key.into(), start.into(), stop.into()];
  args_value_cmd(client, RedisCommandKind::LTrim, args).await
}

pub async fn rpop<C: ClientLike>(client: &C, key: RedisKey, count: Option<usize>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn rpoplpush<C: ClientLike>(
  client: &C,
  source: RedisKey,
  dest: RedisKey,
) -> Result<RedisValue, RedisError> {
  let args = vec![source.into(), dest.into()];
  args_value_cmd(client, RedisCommandKind::Rpoplpush, args).await
}

pub async fn lmove<C: ClientLike>(
  client: &C,
  source: RedisKey,
  dest: RedisKey,
  source_direction: LMoveDirection,
  dest_direction: LMoveDirection,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args = vec![
      source.into(),
      dest.into(),
      source_direction.to_str().into(),
      dest_direction.to_str().into(),
    ];

    Ok((RedisCommandKind::LMove, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn rpush<C: ClientLike>(
  client: &C,
  key: RedisKey,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let elements = elements.into_multiple_values();
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::Rpush, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn rpushx<C: ClientLike>(
  client: &C,
  key: RedisKey,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let elements = elements.into_multiple_values();
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.into_iter() {
      args.push(element);
    }

    Ok((RedisCommandKind::Rpushx, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
