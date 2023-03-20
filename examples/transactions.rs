use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();

  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let trx = client.multi();
  let result: RedisValue = trx.get("foo").await?;
  assert!(result.is_queued());
  let result: RedisValue = trx.set("foo", "bar", None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = trx.get("foo").await?;
  assert!(result.is_queued());

  let values: (Option<String>, (), String) = trx.exec(true).await?;
  println!("Transaction results: {:?}", values);

  let _ = client.quit().await?;
  Ok(())
}
