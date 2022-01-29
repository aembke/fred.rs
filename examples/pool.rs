use fred::pool::RedisPool;
use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let pool = RedisPool::new(config, 5)?;
  let _ = pool.connect(None);
  let _ = pool.wait_for_connect().await?;

  // use the pool like any other RedisClient
  let _ = pool.get("foo").await?;
  let _ = pool.set("foo", "bar", None, None, false).await?;
  let _ = pool.get("foo").await?;

  let _ = pool.quit_pool().await;
  Ok(())
}
