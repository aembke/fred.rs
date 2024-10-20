use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();
  client.init().await?;

  // convert responses to many common Rust types
  let foo: String = client.echo("hello world!").await?;
  println!("{:?}", foo);

  client.quit().await?;
  Ok(())
}
