use super::*;
use crate::{
  error::*,
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};
use std::convert::TryInto;

fn check_empty_keys(keys: &MultipleKeys) -> Result<(), RedisError> {
  if keys.len() == 0 {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "At least one key is required.",
    ))
  } else {
    Ok(())
  }
}

value_cmd!(randomkey, Randomkey);

pub async fn get<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(client, RedisCommandKind::Get, key.into()).await
}

pub async fn set<C: ClientLike>(
  client: &C,
  key: RedisKey,
  value: RedisValue,
  expire: Option<Expiration>,
  options: Option<SetOptions>,
  get: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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
      args.push(static_val!(GET));
    }

    Ok((RedisCommandKind::Set, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn del<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  check_empty_keys(&keys)?;

  let args: Vec<RedisValue> = keys.inner().drain(..).map(|k| k.into()).collect();
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::Del, args))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn unlink<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  check_empty_keys(&keys)?;

  let args: Vec<RedisValue> = keys.inner().drain(..).map(|k| k.into()).collect();
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::Unlink, args))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn append<C: ClientLike>(client: &C, key: RedisKey, value: RedisValue) -> Result<RedisValue, RedisError> {
  args_value_cmd(client, RedisCommandKind::Append, vec![key.into(), value]).await
}

pub async fn incr<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Incr, key.into()).await
}

pub async fn decr<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Decr, key.into()).await
}

pub async fn incr_by<C: ClientLike>(client: &C, key: RedisKey, val: i64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::IncrBy, vec![key.into(), val.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn decr_by<C: ClientLike>(client: &C, key: RedisKey, val: i64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::DecrBy, vec![key.into(), val.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn incr_by_float<C: ClientLike>(client: &C, key: RedisKey, val: f64) -> Result<RedisValue, RedisError> {
  let val: RedisValue = val.try_into()?;
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::IncrByFloat, vec![key.into(), val]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ttl<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Ttl, key.into()).await
}

pub async fn pttl<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Pttl, key.into()).await
}

pub async fn persist<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Persist, key.into()).await
}

pub async fn expire<C: ClientLike>(client: &C, key: RedisKey, seconds: i64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Expire, vec![key.into(), seconds.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn expire_at<C: ClientLike>(client: &C, key: RedisKey, timestamp: i64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::ExpireAt, vec![key.into(), timestamp.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn exists<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  check_empty_keys(&keys)?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }

    Ok((RedisCommandKind::Exists, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn dump<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(client, RedisCommandKind::Dump, key.into()).await
}

pub async fn restore<C: ClientLike>(
  client: &C,
  key: RedisKey,
  ttl: i64,
  serialized: RedisValue,
  replace: bool,
  absttl: bool,
  idletime: Option<i64>,
  frequency: Option<i64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(9);
    args.push(key.into());
    args.push(ttl.into());
    args.push(serialized);

    if replace {
      args.push(static_val!(REPLACE));
    }
    if absttl {
      args.push(static_val!(ABSTTL));
    }
    if let Some(idletime) = idletime {
      args.push(static_val!(IDLE_TIME));
      args.push(idletime.into());
    }
    if let Some(frequency) = frequency {
      args.push(static_val!(FREQ));
      args.push(frequency.into());
    }

    Ok((RedisCommandKind::Restore, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn getrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  start: usize,
  end: usize,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::GetRange, vec![
      key.into(),
      start.try_into()?,
      end.try_into()?,
    ]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn setrange<C: ClientLike>(
  client: &C,
  key: RedisKey,
  offset: u32,
  value: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Setrange, vec![key.into(), offset.into(), value]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn getset<C: ClientLike>(client: &C, key: RedisKey, value: RedisValue) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::GetSet, vec![key.into(), value]).await
}

pub async fn rename<C: ClientLike>(
  client: &C,
  source: RedisKey,
  destination: RedisKey,
) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Rename, vec![
    source.into(),
    destination.into(),
  ])
  .await
}

pub async fn renamenx<C: ClientLike>(
  client: &C,
  source: RedisKey,
  destination: RedisKey,
) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Renamenx, vec![
    source.into(),
    destination.into(),
  ])
  .await
}

pub async fn getdel<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(client, RedisCommandKind::GetDel, key.into()).await
}

pub async fn strlen<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Strlen, key.into()).await
}

pub async fn mget<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  check_empty_keys(&keys)?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(keys.len());

    for key in keys.inner().into_iter() {
      args.push(key.into());
    }

    Ok((RedisCommandKind::Mget, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn mset<C: ClientLike>(client: &C, values: RedisMap) -> Result<RedisValue, RedisError> {
  if values.len() == 0 {
    return Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Values cannot be empty.",
    ));
  }

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(values.len() * 2);

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Mset, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn msetnx<C: ClientLike>(client: &C, values: RedisMap) -> Result<RedisValue, RedisError> {
  if values.len() == 0 {
    return Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Values cannot be empty.",
    ));
  }

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(values.len() * 2);

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Msetnx, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn copy<C: ClientLike>(
  client: &C,
  source: RedisKey,
  destination: RedisKey,
  db: Option<u8>,
  replace: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(5);
    args.push(source.into());
    args.push(destination.into());

    if let Some(db) = db {
      args.push(static_val!(DB));
      args.push(db.into());
    }
    if replace {
      args.push(static_val!(REPLACE));
    }

    Ok((RedisCommandKind::Copy, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn watch<C: ClientLike>(client: &C, keys: MultipleKeys) -> Result<(), RedisError> {
  let args = keys.inner().into_iter().map(|k| k.into()).collect();
  args_ok_cmd(client, RedisCommandKind::Watch, args).await
}

ok_cmd!(unwatch, Unwatch);

pub async fn lcs<C: ClientLike>(
  client: &C,
  key1: RedisKey,
  key2: RedisKey,
  len: bool,
  idx: bool,
  minmatchlen: Option<i64>,
  withmatchlen: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(7);
    args.push(key1.into());
    args.push(key2.into());

    if len {
      args.push(static_val!(LEN));
    }
    if idx {
      args.push(static_val!(IDX));
    }
    if let Some(minmatchlen) = minmatchlen {
      args.push(static_val!(MINMATCHLEN));
      args.push(minmatchlen.into());
    }
    if withmatchlen {
      args.push(static_val!(WITHMATCHLEN));
    }

    Ok((RedisCommandKind::Lcs, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
