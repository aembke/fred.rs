use super::*;
use crate::{
  error::*,
  modules::inner::RedisClientInner,
  protocol::{types::*, utils as protocol_utils},
  types::*,
  utils,
};
use redis_protocol::resp3::types::Frame;
use std::sync::Arc;

pub async fn memory_doctor(inner: &Arc<RedisClientInner>) -> Result<String, RedisError> {
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::MemoryDoctor, vec![]))).await?;
  let response = protocol_utils::frame_to_single_result(frame)?;

  response
    .into_string()
    .ok_or(RedisError::new(RedisErrorKind::ProtocolError, "Expected string reply."))
}

pub async fn memory_malloc_stats(inner: &Arc<RedisClientInner>) -> Result<String, RedisError> {
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::MemoryMallocStats, vec![]))).await?;
  let response = protocol_utils::frame_to_single_result(frame)?;

  response
    .into_string()
    .ok_or(RedisError::new(RedisErrorKind::ProtocolError, "Expected string reply."))
}

ok_cmd!(memory_purge, MemoryPurge);

pub async fn memory_stats(inner: &Arc<RedisClientInner>) -> Result<MemoryStats, RedisError> {
  let response = utils::request_response(inner, || Ok((RedisCommandKind::MemoryStats, vec![]))).await?;

  let frame = protocol_utils::frame_map_or_set_to_nested_array(response)?;
  if let Frame::Array { data, .. } = frame {
    protocol_utils::parse_memory_stats(&data)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array response.",
    ))
  }
}

pub async fn memory_usage<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  samples: Option<u32>,
) -> Result<Option<u64>, RedisError>
where
  K: Into<RedisKey>, {
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(3);
    args.push(key.into());

    if let Some(samples) = samples {
      args.push(static_val!(SAMPLES));
      args.push(samples.into());
    }

    Ok((RedisCommandKind::MemoryUsage, args))
  })
  .await?;

  if let RedisValue::Integer(i) = protocol_utils::frame_to_single_result(frame)? {
    if i < 0 {
      Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Expected positive integer.",
      ))
    } else {
      Ok(Some(i as u64))
    }
  } else {
    Ok(None)
  }
}
