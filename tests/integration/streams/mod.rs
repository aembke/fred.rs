use fred::prelude::*;
use std::collections::HashMap;

async fn create_fake_group_and_stream(client: &RedisClient) -> Result<(), RedisError> {
  client.xgroup_create("foo{1}", "group1", "$", true).await
}

pub async fn should_xinfo_consumers(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Result<(), RedisError> = client.xinfo_consumers("foo{1}", "group1").await;
  assert!(result.is_err());

  let _ = create_fake_group_and_stream(&client).await?;
  let _: () = client.xgroup_createconsumer("foo{1}", "group1", "consumer1").await?;
  let consumers: Vec<HashMap<String, String>> = client.xinfo_consumers("foo{1}", "group1").await?;
  assert_eq!(consumers.len(), 1);
  assert_eq!(consumers[0].get("name"), Some(&"consumer1".to_owned()));

  let _: () = client.xgroup_createconsumer("foo{1}", "group1", "consumer2").await?;
  let consumers: Vec<HashMap<String, String>> = client.xinfo_consumers("foo{1}", "group1").await?;
  assert_eq!(consumers.len(), 2);
  assert_eq!(consumers[0].get("name"), Some(&"consumer1".to_owned()));
  assert_eq!(consumers[1].get("name"), Some(&"consumer2".to_owned()));

  Ok(())
}

pub async fn should_xinfo_groups(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Result<(), RedisError> = client.xinfo_groups("foo{1}").await;
  assert!(result.is_err());

  let _ = create_fake_group_and_stream(&client).await?;
  let result: Vec<HashMap<String, String>> = client.xinfo_groups("foo{1}").await?;
  assert_eq!(result.len(), 1);
  assert_eq!(result[0].get("name"), Some(&"group1".to_owned()));

  let _: () = client.xgroup_create("foo{1}", "group2", "$", true).await?;
  let result: Vec<HashMap<String, String>> = client.xinfo_groups("foo{1}").await?;
  assert_eq!(result.len(), 2);
  assert_eq!(result[0].get("name"), Some(&"group1".to_owned()));
  assert_eq!(result[1].get("name"), Some(&"group2".to_owned()));

  Ok(())
}

pub async fn should_xinfo_streams(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Result<(), RedisError> = client.xinfo_stream("foo{1}", true, None).await;
  assert!(result.is_err());

  let _ = create_fake_group_and_stream(&client).await?;
  let mut result: HashMap<String, RedisValue> = client.xinfo_stream("foo{1}", true, None).await?;
  assert_eq!(result.len(), 6);
  assert_eq!(result.get("length"), Some(&RedisValue::Integer(0)));

  let groups: HashMap<String, RedisValue> = result.remove("groups").unwrap().convert()?;
  assert_eq!(groups.get("name"), Some(&RedisValue::from("group1")));

  Ok(())
}

pub async fn should_xadd_auto_id_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_manual_id_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_with_cap_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xadd_nomkstream_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xtrim_a_stream_approx_cap(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xtrim_a_stream_eq_cap(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xdel_one_id_in_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xdel_multiple_ids_in_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrange_no_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrange_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrevrange_no_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xrevrange_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_run_xlen_on_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_one_key_latest_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_multiple_keys_latest_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_manual_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_with_blocking(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xread_with_blocking_and_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_create_no_mkstream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_create_mkstream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_createconsumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_delconsumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_destroy(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xgroup_setid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_one_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_multiple_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_noack(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_block(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_block_and_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xack_one_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xack_multiple_ids(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_one_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_multiple_ids(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_idle(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_time(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_retrycount(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_force(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_justid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_default(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_justid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xpending_default(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xpending_with_idle_no_consumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_idle_and_consumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}
