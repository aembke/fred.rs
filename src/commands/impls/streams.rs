use super::*;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::RedisCommandKind;
use crate::protocol::utils as protocol_utils;
use crate::types::{RedisKey, RedisValue};
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

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

  protocol_utils::frame_to_results(frame)
}

pub async fn xinfo_groups(inner: &Arc<RedisClientInner>, key: RedisKey) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::XinfoGroups, vec![key.into()]))).await?;
  protocol_utils::frame_to_results(frame)
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

pub async fn xadd(inner: &Arc<RedisClientInner>, key: RedisKey, nomkstream: bool) -> Result<RedisValue, RedisError> {
  unimplemented!()
}
