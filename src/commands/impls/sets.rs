use super::*;
use crate::{
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    types::*,
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use std::{convert::TryInto, sync::Arc};

pub async fn sadd<C: ClientLike>(
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
    Ok((RedisCommandKind::Sadd, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn scard<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Scard, key.into()).await
}

pub async fn sdiff<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sdiff, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sdiffstore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  keys: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + keys.len());
    args.push(dest.into());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sdiffstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn sinter<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sinter, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sinterstore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  keys: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + keys.len());
    args.push(dest.into());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sinterstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn sismember<C: ClientLike>(
  client: &C,
  key: RedisKey,
  member: RedisValue,
) -> Result<RedisValue, RedisError> {
  args_value_cmd(client, RedisCommandKind::Sismember, vec![key.into(), member]).await
}

pub async fn smismember<C: ClientLike>(
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
    Ok((RedisCommandKind::Smismember, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn smembers<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(client, RedisCommandKind::Smembers, key.into()).await
}

pub async fn smove<C: ClientLike>(
  client: &C,
  source: RedisKey,
  dest: RedisKey,
  member: RedisValue,
) -> Result<RedisValue, RedisError> {
  let args = vec![source.into(), dest.into(), member];
  args_value_cmd(client, RedisCommandKind::Smove, args).await
}

pub async fn spop<C: ClientLike>(client: &C, key: RedisKey, count: Option<usize>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(count.try_into()?);
    }
    Ok((RedisCommandKind::Spop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn srandmember<C: ClientLike>(
  client: &C,
  key: RedisKey,
  count: Option<usize>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    args.push(key.into());

    if let Some(count) = count {
      args.push(count.try_into()?);
    }
    Ok((RedisCommandKind::Srandmember, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn srem<C: ClientLike>(
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
    Ok((RedisCommandKind::Srem, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn sunion<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sunion, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sunionstore<C: ClientLike>(
  client: &C,
  dest: RedisKey,
  keys: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + keys.len());
    args.push(dest.into());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sunionstore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
