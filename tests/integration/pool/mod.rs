use fred::client::RedisClient;
use fred::error::{RedisError, RedisErrorKind};
use fred::pool::StaticRedisPool;
use fred::types::{RedisConfig, ServerConfig};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::sleep;

async fn create_and_ping_pool(config: &RedisConfig, count: usize) -> Result<(), RedisError> {
  let pool = StaticRedisPool::new(config.clone(), count)?;
  let _ = pool.connect(None);
  let _ = pool.wait_for_connect().await?;

  for client in pool.clients().iter() {
    let _ = client.ping().await?;
  }

  let _ = pool.ping().await?;
  let _ = pool.quit_pool().await;
  Ok(())
}

pub async fn should_connect_and_ping_static_pool_single_conn(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  create_and_ping_pool(&config, 1).await
}

pub async fn should_connect_and_ping_static_pool_two_conn(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  create_and_ping_pool(&config, 2).await
}

// this may require increasing the number of allowed file descriptors
pub async fn should_connect_and_ping_static_pool_many_conn(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  for count in 3..25 {
    let _ = create_and_ping_pool(&config, count).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
  }

  Ok(())
}

pub async fn should_connect_and_ping_static_pool_repeatedly(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  for _ in 1..1000 {
    let _ = create_and_ping_pool(&config, 1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
  }

  Ok(())
}
