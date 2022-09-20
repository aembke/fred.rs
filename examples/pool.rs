use fred::{pool::RedisPool, prelude::*};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let pool = RedisPool::new(config, 5)?;
  let _ = pool.connect(None);
  pool.wait_for_connect().await?;

  // use the pool like any other RedisClient
  pool.get("foo").await?;
  pool.set("foo", "bar", None, None, false).await?;
  pool.get("foo").await?;

  pool.quit_pool().await;
  Ok(())
}
