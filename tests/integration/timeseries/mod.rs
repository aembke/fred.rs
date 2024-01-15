use bytes_utils::Str;
use fred::{
  clients::RedisClient,
  error::RedisError,
  interfaces::*,
  prelude::RedisResult,
  types::{RedisConfig, RedisKey},
};

pub async fn should_ts_add_get_and_range(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let first_timestamp: i64 = client.ts_add("foo", "*", 41.0, None, None, None, None, ()).await?;
  assert!(first_timestamp > 0);
  let second_timestamp: i64 = client.ts_add("foo", "*", 42.0, None, None, None, None, ()).await?;
  assert!(second_timestamp > 0);
  assert!(second_timestamp > first_timestamp);
  let (timestamp, latest): (i64, f64) = client.ts_get("foo", true).await?;
  assert_eq!(latest, 42.0);
  assert_eq!(timestamp, second_timestamp);

  let range: Vec<(i64, f64)> = client.ts_range("foo", "-", "+", true, [], None, None, None).await?;
  assert_eq!(range, vec![(first_timestamp, 41.0), (second_timestamp, 42.0)]);
  Ok(())
}

pub async fn should_ts_add_to_multiple_and_mrange(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let foo_first_timestamp: i64 = client
    .ts_add("foo", "*", 1.1, None, None, None, None, ("a", "b"))
    .await?;
  let foo_second_timestamp: i64 = client
    .ts_add("foo", "*", 2.2, None, None, None, None, ("a", "b"))
    .await?;
  let bar_first_timestamp: i64 = client
    .ts_add("bar", "*", 3.3, None, None, None, None, ("a", "b"))
    .await?;
  let bar_second_timestamp: i64 = client
    .ts_add("bar", "*", 4.4, None, None, None, None, ("a", "b"))
    .await?;

  let ranges: Vec<(RedisKey, Vec<(Str, Str)>, Vec<(i64, f64)>)> = client
    .ts_mrange("-", "+", true, [], None, None, None, None, ["a=b"], None)
    .await?;

  // TODO see what ranges looks like
  Ok(())
}

pub async fn should_create_alter_and_del_timeseries(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // create, info, alter, and del
  unimplemented!()
}

pub async fn should_create_and_query_multiple(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // queryindex multiple
  unimplemented!()
}

pub async fn should_madd_and_mget(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_incr_and_decr(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_create_and_delete_rules(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_mrange_and_mrevrange(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}
