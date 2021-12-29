use fred::prelude::*;

pub async fn should_xinfo_consumers(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xinfo_groups(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xinfo_streams(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_auto_id_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_manual_id_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_with_cap_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_nomkstream_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xtrim_a_stream_approx_cap(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xtrim_a_stream_eq_cap(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xdel_one_id_in_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xdel_multiple_ids_in_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrange_no_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrange_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrevrange_no_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrevrange_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_run_xlen_on_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_one_key_latest_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_multiple_keys_latest_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_manual_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_with_blocking(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_with_blocking_and_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_create_no_mkstream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_create_mkstream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_createconsumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_delconsumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_destroy(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_setid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_one_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_multiple_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_noack(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_block(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_block_and_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xack_one_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xack_multiple_ids(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_one_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_multiple_ids(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_idle(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_time(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_retrycount(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_force(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_justid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_default(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_justid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xpending_default(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xpending_with_idle_no_consumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_idle_and_consumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}
