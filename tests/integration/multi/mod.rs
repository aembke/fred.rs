use fred::{
  clients::RedisClient,
  error::RedisError,
  interfaces::*,
  types::{RedisConfig, RedisValue},
};

pub async fn should_run_get_set_trx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi();

  let _r1: () = trx.set("foo", "bar", None, None, false).await?;
  let _r2: () = trx.get("foo").await?;
  let results: Vec<String> = trx.exec(true).await?;

  assert_eq!(results, vec!["OK", "bar"]);
  Ok(())
}

pub async fn should_run_error_get_set_trx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.set("foo", "bar", None, None, false).await?;

  let trx = client.multi();
  let _: () = trx.incr("foo").await?;
  let _: () = trx.exec(true).await?;

  Ok(())
}

pub async fn should_fail_with_hashslot_error(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi();
  let _: () = client.set("foo", "bar", None, None, false).await?;
  let _: () = client.set("bar", "baz", None, None, false).await?;
  let _: () = trx.exec(true).await?;

  Ok(())
}
