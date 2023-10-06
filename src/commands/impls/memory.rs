use super::*;
use crate::{
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::*,
  utils,
};

value_cmd!(memory_doctor, MemoryDoctor);
value_cmd!(memory_malloc_stats, MemoryMallocStats);
ok_cmd!(memory_purge, MemoryPurge);

pub async fn memory_stats<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let response = utils::request_response(client, || Ok((RedisCommandKind::MemoryStats, vec![]))).await?;
  protocol_utils::frame_to_results(response)
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

  protocol_utils::frame_to_results(frame)
}
