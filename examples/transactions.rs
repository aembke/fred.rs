use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default_clustered();
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let client = RedisClient::new(config);

  let jh = client.connect(Some(policy), false);
  let _ = client.wait_for_connect().await?;

  // a special function to clear all data in a cluster
  let _ = client.flushall_cluster().await?;

  let trx = client.multi(true).await?;
  let res1 = trx.get("foo").await?;
  assert!(res1.is_queued());
  let res2 = trx.set("foo", "bar", None, None, false).await?;
  assert!(res2.is_queued());
  let res3 = trx.get("foo").await?;
  assert!(res3.is_queued());

  if let RedisValue::Array(values) = trx.exec().await? {
    println!("Transaction results: {:?}", values);
  }

  let _ = client.quit().await?;
  let _ = jh.await;
  Ok(())
}
