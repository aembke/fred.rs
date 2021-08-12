use fred::pool::StaticRedisPool;
use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default_centralized();
  let pool = StaticRedisPool::new(config, 5)?;

  let jhs = pool.connect(None, false);
  let _ = pool.wait_for_connect().await?;

  // use the pool like any other RedisClient with the Deref trait
  let _ = pool.get("foo").await?;
  let _ = pool.set("foo", "bar", None, None, false).await?;
  let _ = pool.get("foo").await?;

  let _ = pool.quit_pool().await;
  // from here the pool can be restarted by calling `connect` again, if needed
  for jh in jhs.into_iter() {
    let _ = jh.await;
  }
  Ok(())
}
