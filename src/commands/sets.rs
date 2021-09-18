use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

pub async fn sadd<K>(inner: &Arc<RedisClientInner>, key: K, members: MultipleValues) -> Result<RedisValue, RedisError>
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
    Ok((RedisCommandKind::Sadd, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn scard<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Scard, key.into().into()).await
}

pub async fn sdiff<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sdiff, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sdiffstore<D, K>(inner: &Arc<RedisClientInner>, dest: D, keys: K) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  K: Into<MultipleKeys>,
{
  let (dest, keys) = (dest.into(), keys.into());
  let frame = utils::request_response(inner, move || {
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

pub async fn sinter<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sinter, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sinterstore<D, K>(inner: &Arc<RedisClientInner>, dest: D, keys: K) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  K: Into<MultipleKeys>,
{
  let (dest, keys) = (dest.into(), keys.into());
  let frame = utils::request_response(inner, move || {
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

pub async fn sismember<K>(inner: &Arc<RedisClientInner>, key: K, member: RedisValue) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  args_value_cmd(inner, RedisCommandKind::Sismember, vec![key.into().into(), member]).await
}

pub async fn smismember<K>(
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
    Ok((RedisCommandKind::Smismember, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn smembers<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_values_cmd(inner, RedisCommandKind::Smembers, key.into().into()).await
}

pub async fn smove<S, D>(
  inner: &Arc<RedisClientInner>,
  source: S,
  dest: D,
  member: RedisValue,
) -> Result<RedisValue, RedisError>
where
  S: Into<RedisKey>,
  D: Into<RedisKey>,
{
  let (source, dest) = (source.into(), dest.into());
  let args = vec![source.into(), dest.into(), member];
  args_value_cmd(inner, RedisCommandKind::Smove, args).await
}

pub async fn spop<K>(inner: &Arc<RedisClientInner>, key: K, count: Option<usize>) -> Result<RedisValue, RedisError>
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
    Ok((RedisCommandKind::Spop, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn srandmember<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  count: Option<usize>,
) -> Result<RedisValue, RedisError>
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
    Ok((RedisCommandKind::Srandmember, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn srem<K>(inner: &Arc<RedisClientInner>, key: K, members: MultipleValues) -> Result<RedisValue, RedisError>
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
    Ok((RedisCommandKind::Srem, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn sunion<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }
    Ok((RedisCommandKind::Sunion, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sunionstore<D, K>(inner: &Arc<RedisClientInner>, dest: D, keys: K) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  K: Into<MultipleKeys>,
{
  let (dest, keys) = (dest.into(), keys.into());
  let frame = utils::request_response(inner, move || {
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
