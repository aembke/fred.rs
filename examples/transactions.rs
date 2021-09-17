use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let client = RedisClient::new(config);

  let jh = client.connect(None);
  let _ = client.wait_for_connect().await?;

  // a special function to clear all data in a cluster
  let _ = client.flushall_cluster().await?;

  let trx = client.multi(true).await?;
  let res1: RedisValue = trx.get("foo").await?;
  assert!(res1.is_queued());
  let res2: RedisValue = trx.set("foo", "bar", None, None, false).await?;
  assert!(res2.is_queued());
  let res3: RedisValue = trx.get("foo").await?;
  assert!(res3.is_queued());

  if let RedisValue::Array(values) = trx.exec().await? {
    println!("Transaction results: {:?}", values);
  }

  let _ = client.quit().await?;
  Ok(())
}
