use fred::client::RedisClient;
use fred::error::RedisError;
use fred::types::{RedisConfig, RedisValue};

pub async fn should_run_get_set_trx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi(true).await?;

  let _r1: () = trx.set("foo", "bar", None, None, false).await?;
  let _r2: () = trx.get("foo").await?;
  let results: Vec<String> = trx.exec().await?;

  assert_eq!(results, vec!["OK", "bar"]);
  Ok(())
}

pub async fn should_run_error_get_set_trx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.set("foo", "bar", None, None, false).await?;

  let trx = client.multi(true).await?;
  let _: () = trx.incr("foo").await?;
  let _: () = trx.exec().await?;

  Ok(())
}

pub async fn should_fail_with_hashslot_error(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi(true).await?;
  let _: () = client.set("foo", "bar", None, None, false).await?;
  let _: () = client.set("bar", "baz", None, None, false).await?;
  let _: () = trx.exec().await?;

  Ok(())
}

pub async fn should_fail_with_blocking_cmd(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi(true).await?;
  let _: () = client.blpop("foo", 100.0).await?;
  let _: () = trx.exec().await?;

  Ok(())
}

pub async fn should_use_cluster_slot_with_publish(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi(true).await?;

  let res: RedisValue = trx.set("foo1", "bar", None, None, false).await?;
  assert!(res.is_queued());
  let res2: RedisValue = trx.publish("foo2", "bar").await?;
  assert!(res2.is_queued());

  let _: () = trx.exec().await?;
  Ok(())
}
