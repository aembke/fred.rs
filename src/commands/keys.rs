use super::*;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

value_cmd!(randomkey, Randomkey);

pub async fn get<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_values_cmd(inner, RedisCommandKind::Get, key.into().into()).await
}

pub async fn set(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  value: RedisValue,
  expire: Option<Expiration>,
  options: Option<SetOptions>,
  get: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    args.push(value);

    if let Some(expire) = expire {
      let (k, v) = expire.into_args();
      args.push(k.into());
      if let Some(v) = v {
        args.push(v.into());
      }
    }
    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    if get {
      args.push(GET.into());
    }

    Ok((RedisCommandKind::Set, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn del<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  utils::check_empty_keys(&keys)?;

  let args: Vec<RedisValue> = keys.inner().drain(..).map(|k| k.into()).collect();
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::Del, args))).await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn incr<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Incr, key.into().into()).await
}

pub async fn decr<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Decr, key.into().into()).await
}

pub async fn incr_by<K>(inner: &Arc<RedisClientInner>, key: K, val: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::IncrBy, vec![key.into(), val.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn decr_by<K>(inner: &Arc<RedisClientInner>, key: K, val: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::DecrBy, vec![key.into(), val.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn incr_by_float<K>(inner: &Arc<RedisClientInner>, key: K, val: f64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let (key, val) = (key.into(), val.try_into()?);
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::IncrByFloat, vec![key.into(), val]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn ttl<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Ttl, key.into().into()).await
}

pub async fn pttl<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Pttl, key.into().into()).await
}

pub async fn persist<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Persist, key.into().into()).await
}

pub async fn expire<K>(inner: &Arc<RedisClientInner>, key: K, seconds: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Expire, vec![key.into(), seconds.into()]))
  })
  .await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn expire_at<K>(inner: &Arc<RedisClientInner>, key: K, timestamp: i64) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ExpireAt, vec![key.into(), timestamp.into()]))
  })
  .await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn exists<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  utils::check_empty_keys(&keys)?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }

    Ok((RedisCommandKind::Exists, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn dump<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_values_cmd(inner, RedisCommandKind::Dump, key.into().into()).await
}

pub async fn restore<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  ttl: i64,
  serialized: RedisValue,
  replace: bool,
  absttl: bool,
  idletime: Option<i64>,
  frequency: Option<i64>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(9);
    args.push(key.into());
    args.push(ttl.into());
    args.push(serialized);

    if replace {
      args.push(REPLACE.into());
    }
    if absttl {
      args.push(ABSTTL.into());
    }
    if let Some(idletime) = idletime {
      args.push(IDLE_TIME.into());
      args.push(idletime.into());
    }
    if let Some(frequency) = frequency {
      args.push(FREQ.into());
      args.push(frequency.into());
    }

    Ok((RedisCommandKind::Restore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn getrange<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  start: usize,
  end: usize,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::GetRange,
      vec![key.into(), start.try_into()?, end.try_into()?],
    ))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn setrange<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  offset: u32,
  value: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Setrange, vec![key.into(), offset.into(), value]))
  })
  .await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn getset<K>(inner: &Arc<RedisClientInner>, key: K, value: RedisValue) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  args_values_cmd(inner, RedisCommandKind::GetSet, vec![key.into().into(), value]).await
}

pub async fn getdel<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_values_cmd(inner, RedisCommandKind::GetDel, key.into().into()).await
}

pub async fn strlen<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::Strlen, key.into().into()).await
}

pub async fn mget<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let keys = keys.into();
  utils::check_empty_keys(&keys)?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }

    Ok((RedisCommandKind::Mget, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn mset<V>(inner: &Arc<RedisClientInner>, values: V) -> Result<RedisValue, RedisError>
where
  V: Into<RedisMap>,
{
  let values = values.into();
  if values.len() == 0 {
    return Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Values cannot be empty.",
    ));
  }

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(values.len() * 2);

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Mset, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn msetnx<V>(inner: &Arc<RedisClientInner>, values: V) -> Result<RedisValue, RedisError>
where
  V: Into<RedisMap>,
{
  let values = values.into();
  if values.len() == 0 {
    return Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Values cannot be empty.",
    ));
  }

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(values.len() * 2);

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Msetnx, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn copy<S, D>(
  inner: &Arc<RedisClientInner>,
  source: S,
  destination: D,
  db: Option<u8>,
  replace: bool,
) -> Result<RedisValue, RedisError>
where
  S: Into<RedisKey>,
  D: Into<RedisKey>,
{
  let (source, destination) = (source.into(), destination.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(5);
    args.push(source.into());
    args.push(destination.into());

    if let Some(db) = db {
      args.push(DB.into());
      args.push(db.into());
    }
    if replace {
      args.push(REPLACE.into());
    }

    Ok((RedisCommandKind::Copy, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn watch<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<(), RedisError>
where
  K: Into<MultipleKeys>,
{
  let args = keys.into().inner().into_iter().map(|k| k.into()).collect();
  args_ok_cmd(inner, RedisCommandKind::Watch, args).await
}

ok_cmd!(unwatch, Unwatch);
