use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let pool = Builder::default_centralized().build_pool(5)?;
  let _ = pool.connect();
  let _ = pool.wait_for_connect().await?;

  // interact with specific clients via next(), last(), or clients()
  let pipeline = pool.next().pipeline();
  let _: () = pipeline.incr("foo").await?;
  let _: () = pipeline.incr("foo").await?;
  let _: i64 = pipeline.last().await?;

  for client in pool.clients() {
    println!("{} connected to {:?}", client.id(), client.active_connections().await?);

    // set up event listeners on each client
    client.on_error(|error| {
      println!("Connection error: {:?}", error);
      Ok(())
    });
  }

  // or use the pool like any other RedisClient
  let _: () = pool.get("foo").await?;
  let _: () = pool.set("foo", "bar", None, None, false).await?;
  let _: () = pool.get("foo").await?;

  let _ = pool.quit().await;
  Ok(())
}
