use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::sync::Arc;

pub async fn pfadd<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  elements: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
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

pub async fn pfcount<K>(inner: &Arc<RedisClientInner>, keys: K) -> Result<RedisValue, RedisError>
where
  K: Into<MultipleKeys>,
{
  let args: Vec<RedisValue> = keys.into().inner().into_iter().map(|k| k.into()).collect();
  args_value_cmd(inner, RedisCommandKind::Pfcount, args).await
}

pub async fn pfmerge<D, S>(inner: &Arc<RedisClientInner>, dest: D, sources: S) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  S: Into<MultipleKeys>,
{
  let (dest, sources) = (dest.into(), sources.into());
  let frame = utils::request_response(inner, move || {
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
