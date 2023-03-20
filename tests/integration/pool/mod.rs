use fred::{clients::RedisClient, error::RedisError, interfaces::*, pool::RedisPool, types::RedisConfig};

async fn create_and_ping_pool(config: &RedisConfig, count: usize) -> Result<(), RedisError> {
  let pool = RedisPool::new(config.clone(), None, None, count)?;
  let _ = pool.connect();
  let _ = pool.wait_for_connect().await?;

  for client in pool.clients().iter() {
    let _: () = client.ping().await?;
  }

  let _: () = pool.ping().await?;
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
