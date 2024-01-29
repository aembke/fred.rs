#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();
  client.init().await?;

  // transactions are buffered in memory before calling `exec`
  let trx = client.multi();
  let result: RedisValue = trx.get("foo").await?;
  assert!(result.is_queued());
  let result: RedisValue = trx.set("foo", "bar", None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = trx.get("foo").await?;
  assert!(result.is_queued());

  // automatically send `WATCH ...` before `MULTI`
  trx.watch_before(vec!["foo", "bar"]);
  let values: (Option<String>, (), String) = trx.exec(true).await?;
  println!("Transaction results: {:?}", values);

  client.quit().await?;
  Ok(())
}
