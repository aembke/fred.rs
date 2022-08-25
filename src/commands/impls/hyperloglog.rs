use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::sync::Arc;

pub async fn pfadd<C: ClientLike>(
  client: C,
  key: RedisKey,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + elements.len());
    args.push(key.into());

    for element in elements.inner().into_iter() {
      args.push(element);
    }
    Ok((RedisCommandKind::Pfadd, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn pfcount<C: ClientLike>(client: C, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = keys.inner().into_iter().map(|k| k.into()).collect();
  args_value_cmd(client, RedisCommandKind::Pfcount, args).await
}

pub async fn pfmerge<C: ClientLike>(
  client: C,
  dest: RedisKey,
  sources: MultipleKeys,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1 + sources.len());
    args.push(dest.into());

    for source in sources.inner().into_iter() {
      args.push(source.into());
    }
    Ok((RedisCommandKind::Pfmerge, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
