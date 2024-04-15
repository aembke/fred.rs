use fred::{cmd, prelude::*, types::MemoryStats};

pub async fn should_run_memory_doctor(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.memory_doctor().await?;
  Ok(())
}

pub async fn should_run_memory_malloc_stats(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.memory_malloc_stats().await?;
  Ok(())
}

pub async fn should_run_memory_purge(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.memory_purge().await?;
  Ok(())
}

pub async fn should_run_memory_stats(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let stats: MemoryStats = client.memory_stats().await?;
  assert!(stats.total_allocated > 0);

  Ok(())
}

pub async fn should_run_memory_usage(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.custom(cmd!("SET"), vec!["foo", "bar"]).await?;
  assert!(client.memory_usage::<u64, _>("foo", None).await? > 0);

  Ok(())
}
