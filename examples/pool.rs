use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let pool = RedisPool::new(config, None, None, 5)?;
  let _ = pool.connect();
  let _ = pool.wait_for_connect().await?;

  for client in pool.clients() {
    println!("{} connected to {:?}", client.id(), client.active_connections().await?);
  }

  // use the pool like any other RedisClient
  let _ = pool.get("foo").await?;
  let _ = pool.set("foo", "bar", None, None, false).await?;
  let _ = pool.get("foo").await?;

  let _ = pool.quit_pool().await;
  Ok(())
}
