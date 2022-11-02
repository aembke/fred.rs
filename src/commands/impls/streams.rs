use super::*;
use crate::{
  error::RedisError,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    hashers::ClusterHash,
    utils as protocol_utils,
  },
  types::{
    MultipleIDs,
    MultipleKeys,
    MultipleOrderedPairs,
    MultipleStrings,
    RedisKey,
    RedisValue,
    XCap,
    XPendingArgs,
    XID,
  },
  utils,
};
use bytes_utils::Str;
use redis_protocol::redis_keyslot;
use std::{convert::TryInto, sync::Arc};

fn encode_cap(args: &mut Vec<RedisValue>, cap: XCap) {
  if let Some((kind, trim, threshold, limit)) = cap.into_parts() {
    args.push(kind.to_str().into());
    args.push(trim.to_str().into());
    args.push(threshold.into_arg());
    if let Some(count) = limit {
      args.push(static_val!(LIMIT));
      args.push(count.into());
    }
  }
}

pub async fn xinfo_consumers<C: ClientLike>(
  client: C,
  key: RedisKey,
  groupname: Str,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args = vec![key.into(), groupname.into()];
    Ok((RedisCommandKind::XinfoConsumers, args))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xinfo_groups<C: ClientLike>(client: C, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::XinfoGroups, vec![key.into()]))).await?;
  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xinfo_stream<C: ClientLike>(
  client: C,
  key: RedisKey,
  full: bool,
  count: Option<u64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());

    if full {
      args.push(static_val!(FULL));
      if let Some(count) = count {
        args.push(static_val!(COUNT));
        args.push(count.try_into()?);
      }
    }

    Ok((RedisCommandKind::XinfoStream, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xadd<C: ClientLike>(
  client: C,
  key: RedisKey,
  nomkstream: bool,
  cap: XCap,
  id: XID,
  fields: MultipleOrderedPairs,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(8 + (fields.len() * 2));
    args.push(key.into());

    if nomkstream {
      args.push(static_val!(NOMKSTREAM));
    }
    encode_cap(&mut args, cap);

    args.push(id.into_str().into());
    for (key, value) in fields.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Xadd, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xtrim<C: ClientLike>(client: C, key: RedisKey, cap: XCap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(6);
    args.push(key.into());
    encode_cap(&mut args, cap);

    Ok((RedisCommandKind::Xtrim, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xdel<C: ClientLike>(client: C, key: RedisKey, ids: MultipleStrings) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn xrange<C: ClientLike>(
  client: C,
  key: RedisKey,
  start: RedisValue,
  end: RedisValue,
  count: Option<u64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(5);
    args.push(key.into());
    args.push(start);
    args.push(end);

    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
    }

    Ok((RedisCommandKind::Xrange, args))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xrevrange<C: ClientLike>(
  client: C,
  key: RedisKey,
  end: RedisValue,
  start: RedisValue,
  count: Option<u64>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(5);
    args.push(key.into());
    args.push(end);
    args.push(start);

    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
    }

    Ok((RedisCommandKind::Xrevrange, args))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xlen<C: ClientLike>(client: C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::Xlen, key.into()).await
}

pub async fn xread<C: ClientLike>(
  client: C,
  count: Option<u64>,
  block: Option<u64>,
  keys: MultipleKeys,
  ids: MultipleIDs,
) -> Result<RedisValue, RedisError> {
  let is_clustered = client.inner().config.server.is_clustered();
  let frame = utils::request_response(client, move || {
    let is_blocking = block.is_some();
    let mut hash_slot = None;
    let mut args = Vec::with_capacity(5 + keys.len() + ids.len());

    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
    }
    if let Some(block) = block {
      args.push(static_val!(BLOCK));
      args.push(block.try_into()?);
    }
    args.push(static_val!(STREAMS));

    for (idx, key) in keys.inner().into_iter().enumerate() {
      // set the hash slot from the first key. if any other keys are on other cluster nodes the server will say
      // something
      if is_clustered && idx == 0 {
        hash_slot = Some(ClusterHash::Offset(args.len()));
      }

      args.push(key.into());
    }
    for id in ids.inner().into_iter() {
      args.push(id.into_str().into());
    }

    let mut command: RedisCommand = (RedisCommandKind::Xread, args).into();
    command.can_pipeline = !is_blocking;
    command.hasher = hash_slot.unwrap_or(ClusterHash::Random);
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xgroup_create<C: ClientLike>(
  client: C,
  key: RedisKey,
  groupname: Str,
  id: XID,
  mkstream: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(groupname.into());
    args.push(id.into_str().into());
    if mkstream {
      args.push(static_val!(MKSTREAM));
    }

    Ok((RedisCommandKind::Xgroupcreate, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_createconsumer<C: ClientLike>(
  client: C,
  key: RedisKey,
  groupname: Str,
  consumername: Str,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::XgroupCreateConsumer, vec![
      key.into(),
      groupname.into(),
      consumername.into(),
    ]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_delconsumer<C: ClientLike>(
  client: C,
  key: RedisKey,
  groupname: Str,
  consumername: Str,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::XgroupDelConsumer, vec![
      key.into(),
      groupname.into(),
      consumername.into(),
    ]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_destroy<C: ClientLike>(
  client: C,
  key: RedisKey,
  groupname: Str,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::XgroupDestroy, vec![key.into(), groupname.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xgroup_setid<C: ClientLike>(
  client: C,
  key: RedisKey,
  groupname: Str,
  id: XID,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::XgroupSetId, vec![
      key.into(),
      groupname.into(),
      id.into_str().into(),
    ]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xreadgroup<C: ClientLike>(
  client: C,
  group: Str,
  consumer: Str,
  count: Option<u64>,
  block: Option<u64>,
  noack: bool,
  keys: MultipleKeys,
  ids: MultipleIDs,
) -> Result<RedisValue, RedisError> {
  let is_clustered = client.inner().config.server.is_clustered();
  let frame = utils::request_response(client, move || {
    let is_blocking = block.is_some();
    let mut hash_slot = None;

    let mut args = Vec::with_capacity(9 + keys.len() + ids.len());
    args.push(static_val!(GROUP));
    args.push(group.into());
    args.push(consumer.into());

    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
    }
    if let Some(block) = block {
      args.push(static_val!(BLOCK));
      args.push(block.try_into()?);
    }
    if noack {
      args.push(static_val!(NOACK));
    }

    args.push(static_val!(STREAMS));
    for (idx, key) in keys.inner().into_iter().enumerate() {
      if is_clustered && idx == 0 {
        hash_slot = Some(ClusterHash::Offset(args.len()));
      }

      args.push(key.into());
    }
    for id in ids.inner().into_iter() {
      args.push(id.into_str().into());
    }

    let mut command: RedisCommand = (RedisCommandKind::Xreadgroup, args).into();
    command.can_pipeline = !is_blocking;
    command.hasher = hash_slot.unwrap_or(ClusterHash::None);
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xack<C: ClientLike>(
  client: C,
  key: RedisKey,
  group: Str,
  ids: MultipleIDs,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2 + ids.len());
    args.push(key.into());
    args.push(group.into());

    for id in ids.inner().into_iter() {
      args.push(id.into_str().into());
    }
    Ok((RedisCommandKind::Xack, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn xclaim<C: ClientLike>(
  client: C,
  key: RedisKey,
  group: Str,
  consumer: Str,
  min_idle_time: u64,
  ids: MultipleIDs,
  idle: Option<u64>,
  time: Option<u64>,
  retry_count: Option<u64>,
  force: bool,
  justid: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(12 + ids.len());
    args.push(key.into());
    args.push(group.into());
    args.push(consumer.into());
    args.push(min_idle_time.try_into()?);

    for id in ids.inner().into_iter() {
      args.push(id.into_str().into());
    }
    if let Some(idle) = idle {
      args.push(static_val!(IDLE));
      args.push(idle.try_into()?);
    }
    if let Some(time) = time {
      args.push(static_val!(TIME));
      args.push(time.try_into()?);
    }
    if let Some(retry_count) = retry_count {
      args.push(static_val!(RETRYCOUNT));
      args.push(retry_count.try_into()?);
    }
    if force {
      args.push(static_val!(FORCE));
    }
    if justid {
      args.push(static_val!(JUSTID));
    }

    Ok((RedisCommandKind::Xclaim, args))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xautoclaim<C: ClientLike>(
  client: C,
  key: RedisKey,
  group: Str,
  consumer: Str,
  min_idle_time: u64,
  start: XID,
  count: Option<u64>,
  justid: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(8);
    args.push(key.into());
    args.push(group.into());
    args.push(consumer.into());
    args.push(min_idle_time.try_into()?);
    args.push(start.into_str().into());

    if let Some(count) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
    }
    if justid {
      args.push(static_val!(JUSTID));
    }

    Ok((RedisCommandKind::Xautoclaim, args))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn xpending<C: ClientLike>(
  client: C,
  key: RedisKey,
  group: Str,
  cmd_args: XPendingArgs,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(8);
    args.push(key.into());
    args.push(group.into());

    if let Some((idle, start, end, count, consumer)) = cmd_args.into_parts()? {
      if let Some(idle) = idle {
        args.push(static_val!(IDLE));
        args.push(idle.try_into()?);
      }
      args.push(start.into_str().into());
      args.push(end.into_str().into());
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
