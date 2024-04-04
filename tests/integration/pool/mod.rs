use fred::{
  clients::{RedisClient, RedisPool},
  error::RedisError,
  interfaces::*,
  types::RedisConfig,
};

async fn create_and_ping_pool(config: &RedisConfig, count: usize) -> Result<(), RedisError> {
  let pool = RedisPool::new(config.clone(), None, None, None, count)?;
  pool.init().await?;

  for client in pool.clients().iter() {
    client.ping().await?;
  }

  pool.ping().await?;
  pool.quit().await?;
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
