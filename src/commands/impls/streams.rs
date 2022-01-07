use super::*;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::RedisCommandKind;
use crate::protocol::utils as protocol_utils;
use crate::types::{
  MultipleIDs, MultipleKeys, MultipleOrderedPairs, MultipleStrings, RedisKey, RedisValue, XCap, XID,
};
use crate::utils;
use redis_protocol::redis_keyslot;
use std::convert::TryInto;
use std::sync::Arc;

fn check_map_array_wrapper(value: RedisValue) -> RedisValue {
  // due to the automatic pop() on single element arrays the result here can be difficult to type generically.
  // in commands like XINFO_* if the result only has one consumer you get a map, if it mas more than one you get
  // an array of maps. ideally we'd get an array regardless, hence this added check...

  if value.is_probably_map() {
    RedisValue::Array(vec![value])
  } else {
    value
  }
}

fn encode_cap(args: &mut Vec<RedisValue>, cap: XCap) {
  if let Some((kind, trim, threshold, limit)) = cap.into_parts() {
    args.push(kind.to_str().into());
    args.push(trim.to_str().into());
    args.push(threshold.into_arg());
    if let Some(count) = limit {
      args.push(LIMIT.into());
      args.push(count.into());
    }
  }
}

pub async fn xinfo_consumers(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  groupname: String,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let args = vec![key.into(), groupname.into()];
    Ok((RedisCommandKind::XinfoConsumers, args))
  })
  .await?;

  Ok(check_map_array_wrapper(protocol_utils::frame_to_results(frame)?))
}

pub async fn xinfo_groups(inner: &Arc<RedisClientInner>, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::XinfoGroups, vec![key.into()]))).await?;
  Ok(check_map_array_wrapper(protocol_utils::frame_to_results(frame)?))
}

pub async fn xinfo_stream(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  full: bool,
  count: Option<u64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());

    if full {
      args.push(FULL.into());
      if let Some(count) = count {
        args.push(COUNT.into());
        args.push(count.try_into()?);
      }
    }

    Ok((RedisCommandKind::XinfoStream, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xadd(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  nomkstream: bool,
  cap: XCap,
  id: XID,
  fields: MultipleOrderedPairs,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(8 + (fields.len() * 2));
    args.push(key.into());

    if nomkstream {
      args.push(NOMKSTREAM.into());
    }
    encode_cap(&mut args, cap);

    args.push(id.into_string().into());
    for (key, value) in fields.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Xadd, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xtrim(inner: &Arc<RedisClientInner>, key: RedisKey, cap: XCap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    encode_cap(&mut args, cap);

    Ok((RedisCommandKind::Xtrim, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xdel(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  ids: MultipleStrings,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + ids.len());
    args.push(key.into());

    for id in ids.inner().into_iter() {
      args.push(id.into());
    }
    Ok((RedisCommandKind::Xdel, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xrange(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  start: RedisValue,
  end: RedisValue,
  count: Option<u64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(5);
    args.push(key.into());
    args.push(start);
    args.push(end);

    if let Some(count) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
    }

    Ok((RedisCommandKind::Xrange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xrevrange(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  end: RedisValue,
  start: RedisValue,
  count: Option<u64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(5);
    args.push(key.into());
    args.push(end);
    args.push(start);

    if let Some(count) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
    }

    Ok((RedisCommandKind::Xrevrange, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xlen(inner: &Arc<RedisClientInner>, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(inner, RedisCommandKind::Xlen, key.into()).await
}

pub async fn xread(
  inner: &Arc<RedisClientInner>,
  count: Option<u64>,
  block: Option<u64>,
  keys: MultipleKeys,
  ids: MultipleIDs,
) -> Result<RedisValue, RedisError> {
  let is_clustered = utils::is_clustered(&inner.config);
  let frame = utils::request_response(inner, move || {
    let is_blocking = block.is_some();
    let mut hash_slot = None;

    let mut args = Vec::with_capacity(5 + keys.len() + ids.len());

    if let Some(count) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
    }
    if let Some(block) = block {
      args.push(BLOCK.into());
      args.push(block.try_into()?);
    }

    args.push(STREAMS.into());
    for (idx, key) in keys.inner().into_iter().enumerate() {
      // set the hash slot from the first key. if any other keys are on other cluster nodes the server will say something
      if is_clustered && idx == 0 {
        hash_slot = Some(redis_keyslot(key.as_bytes()));
      }

      args.push(key.into());
    }
    for id in ids.inner().into_iter() {
      args.push(id.into_string().into());
    }

    Ok((RedisCommandKind::Xread((is_blocking, hash_slot)), args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_create(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  groupname: String,
  id: XID,
  mkstream: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(groupname.into());
    args.push(id.into_string().into());
    if mkstream {
      args.push(MKSTREAM.into());
    }

    Ok((RedisCommandKind::Xgroupcreate, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_createconsumer(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  groupname: String,
  consumername: String,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::XgroupCreateConsumer,
      vec![key.into(), groupname.into(), consumername.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_delconsumer(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  groupname: String,
  consumername: String,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::XgroupDelConsumer,
      vec![key.into(), groupname.into(), consumername.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_destroy(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  groupname: String,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::XgroupDestroy, vec![key.into(), groupname.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_setid(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  groupname: String,
  id: XID,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::XgroupSetId,
      vec![key.into(), groupname.into(), id.into_string().into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xreadgroup(
  inner: &Arc<RedisClientInner>,
  group: String,
  consumer: String,
  count: Option<u64>,
  block: Option<u64>,
  noack: bool,
  keys: MultipleKeys,
  ids: MultipleIDs,
) -> Result<RedisValue, RedisError> {
  let is_clustered = utils::is_clustered(&inner.config);
  let frame = utils::request_response(inner, move || {
    let is_blocking = block.is_some();
    let mut hash_slot = None;

    let mut args = Vec::with_capacity(9 + keys.len() + ids.len());
    args.push(GROUP.into());
    args.push(group.into());
    args.push(consumer.into());

    if let Some(count) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
    }
    if let Some(block) = block {
      args.push(BLOCK.into());
      args.push(block.try_into()?);
    }
    if noack {
      args.push(NOACK.into());
    }

    args.push(STREAMS.into());
    for (idx, key) in keys.inner().into_iter().enumerate() {
      if is_clustered && idx == 0 {
        hash_slot = Some(redis_keyslot(key.as_bytes()));
      }

      args.push(key.into());
    }
    for id in ids.inner().into_iter() {
      args.push(id.into_string().into());
    }

    Ok((RedisCommandKind::Xreadgroup((is_blocking, hash_slot)), args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xack(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  group: String,
  ids: MultipleIDs,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2 + ids.len());
    args.push(key.into());
    args.push(group.into());

    for id in ids.inner().into_iter() {
      args.push(id.into_string().into());
    }
    Ok((RedisCommandKind::Xack, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xclaim(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  group: String,
  consumer: String,
  min_idle_time: u64,
  ids: MultipleIDs,
  idle: Option<u64>,
  time: Option<u64>,
  retry_count: Option<u64>,
  force: bool,
  justid: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(12 + ids.len());
    args.push(key.into());
    args.push(group.into());
    args.push(consumer.into());
    args.push(min_idle_time.try_into()?);

    for id in ids.inner().into_iter() {
      args.push(id.into_string().into());
    }
    if let Some(idle) = idle {
      args.push(IDLE.into());
      args.push(idle.try_into()?);
    }
    if let Some(time) = time {
      args.push(TIME.into());
      args.push(time.try_into()?);
    }
    if let Some(retry_count) = retry_count {
      args.push(RETRYCOUNT.into());
      args.push(retry_count.try_into()?);
    }
    if force {
      args.push(FORCE.into());
    }
    if justid {
      args.push(JUSTID.into());
    }

    Ok((RedisCommandKind::Xclaim, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xautoclaim(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  group: String,
  consumer: String,
  min_idle_time: u64,
  start: XID,
  count: Option<u64>,
  justid: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(8);
    args.push(key.into());
    args.push(group.into());
    args.push(consumer.into());
    args.push(min_idle_time.try_into()?);
    args.push(start.into_string().into());

    if let Some(count) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
    }
    if justid {
      args.push(JUSTID.into());
    }

    Ok((RedisCommandKind::Xautoclaim, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xpending(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  group: String,
  cmd_args: Option<(Option<u64>, XID, XID, u64, Option<String>)>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(8);
    args.push(key.into());
    args.push(group.into());

    if let Some((idle, start, end, count, consumer)) = cmd_args {
      if let Some(idle) = idle {
        args.push(IDLE.into());
        args.push(idle.try_into()?);
      }
      args.push(start.into_string().into());
      args.push(end.into_string().into());
      args.push(count.try_into()?);
      if let Some(consumer) = consumer {
        args.push(consumer.into());
      }
    }

    Ok((RedisCommandKind::Xpending, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
