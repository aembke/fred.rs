#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let pool = Builder::default_centralized().build_pool(5)?;
  pool.init().await?;

  // all client types, including `RedisPool`, implement the same command interface traits so callers can often use
  // them interchangeably. in this example each command below will be sent round-robin to the underlying 5 clients.
  pool.get("foo").await?;
  pool.set("foo", "bar", None, None, false).await?;
  pool.get("foo").await?;

  // interact with specific clients via next(), last(), or clients()
  let pipeline = pool.next().pipeline();
  pipeline.incr("foo").await?;
  pipeline.incr("foo").await?;
  let _: i64 = pipeline.last().await?;

  for client in pool.clients() {
    println!("{} connected to {:?}", client.id(), client.active_connections().await?);
  }

  pool.quit().await?;
  Ok(())
}
