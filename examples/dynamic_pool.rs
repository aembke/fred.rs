use fred::pool::DynamicRedisPool;
use fred::prelude::*;
use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  // the max size isn't a hard limit - it just determines the size of the client array when the pool is initialized
  let pool = DynamicRedisPool::new(config, None, 5, 10);

  let _ = pool.connect();
  let _ = pool.wait_for_connect().await?;

  // modify the size of the pool at runtime
  let (new_client, _) = pool.scale_up().await;
  if let Some(old_client) = pool.scale_down(true).await {
    assert_eq!(new_client.id(), old_client.id());
  }

  for client in pool.clients() {
    println!("Client ID {} in pool.", client.id());
  }

  // due to the locking required by the resizing operations the Deref trait cannot be used with this pool implementation.
  // if modifications to the pool are not required at runtime the static pool is usually easier to use
  let _ = pool.next().get("foo").await?;
  let _ = pool.next().set("foo", "bar", None, None, false).await?;
  let _ = pool.next().get("foo").await?;

  // if the pool can be empty a function exists that will lazily create a new client, if needed.
  // if the pool is not empty this just calls `next` without creating a new client.
  let _ = pool.next_connect(true).await.get("foo").await?;

  let _ = pool.quit_pool().await;
  Ok(())
}
