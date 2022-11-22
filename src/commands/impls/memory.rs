use super::*;
use crate::{
  error::*,
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};
use redis_protocol::resp3::types::Frame;

value_cmd!(memory_doctor, MemoryDoctor);
value_cmd!(memory_malloc_stats, MemoryMallocStats);
ok_cmd!(memory_purge, MemoryPurge);

pub async fn memory_stats<C: ClientLike>(client: &C) -> Result<MemoryStats, RedisError> {
  let response = utils::request_response(client, || Ok((RedisCommandKind::MemoryStats, vec![]))).await?;

  let frame = protocol_utils::frame_map_or_set_to_nested_array(response)?;
  if let Frame::Array { data, .. } = frame {
    protocol_utils::parse_memory_stats(&data)
  } else {
    Err(RedisError::new(RedisErrorKind::Protocol, "Expected array response."))
  }
}

pub async fn memory_usage<C: ClientLike>(
  client: &C,
  key: RedisKey,
  samples: Option<u32>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(3);
    args.push(key.into());

    if let Some(samples) = samples {
      args.push(static_val!(SAMPLES));
      args.push(samples.into());
    }

    Ok((RedisCommandKind::MemoryUsage, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
