use fred::prelude::*;

pub async fn should_run_memory_doctor(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.memory_doctor().await?;
  Ok(())
}

pub async fn should_run_memory_malloc_stats(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.memory_malloc_stats().await?;
  Ok(())
}

pub async fn should_run_memory_purge(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.memory_purge().await?;
  Ok(())
}

pub async fn should_run_memory_stats(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let stats = client.memory_stats().await?;
  assert!(stats.total_allocated > 0);

  Ok(())
}

pub async fn should_run_memory_usage(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.set("foo", "bar", None, None, false).await?;
  let amt = client.memory_usage("foo", None).await?;
  assert!(amt.unwrap() > 0);

  Ok(())
}
