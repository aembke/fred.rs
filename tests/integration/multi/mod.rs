use fred::{
  clients::RedisClient,
  error::RedisError,
  interfaces::*,
  types::{RedisConfig, RedisValue},
};

pub async fn should_run_get_set_trx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi();

  trx.set("foo", "bar", None, None, false).await?;
  trx.get("foo").await?;
  let results: Vec<String> = trx.exec(true).await?;

  assert_eq!(results, vec!["OK", "bar"]);
  Ok(())
}

pub async fn should_run_error_get_set_trx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", None, None, false).await?;

  let trx = client.multi();
  trx.incr("foo").await?;
  trx.exec(true).await?;

  Ok(())
}

pub async fn should_fail_with_hashslot_error(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let trx = client.multi();
  trx.set("foo", "bar", None, None, false).await?;
  trx.set("bar", "baz", None, None, false).await?;
  trx.exec(true).await?;

  Ok(())
}
