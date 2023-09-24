use fred::{clients::RedisClient, error::RedisError, interfaces::RedisJsonInterface, types::RedisConfig, util::NONE};
use serde_json::{json, Value};

pub async fn should_get_and_set_basic_obj(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let value: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value, Value::Null);

  let value = json!({
    "a": "b",
    "c": 1
  });
  let _: () = client.json_set("foo", "$", value.clone(), None).await?;
  let result: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value, result[0]);

  Ok(())
}

pub async fn should_get_and_set_stringified_obj(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let value: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value, Value::Null);

  let value = json!({
    "a": "b",
    "c": 1
  });
  let _: () = client
    .json_set("foo", "$", serde_json::to_string(&value)?, None)
    .await?;
  let result: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value, result[0]);

  Ok(())
}

pub async fn should_array_append(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // arrayappend and arrlen
  unimplemented!()
}

pub async fn should_modify_arrays(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // arrayinsert and arrayindex and arrlen
  unimplemented!()
}

pub async fn should_pop_and_trim_arrays(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_get_set_del_obj(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_merge_objects(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_mset_and_mget(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_incr_numbers(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_inspect_objects(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // objkeys and objlen
  unimplemented!()
}

pub async fn should_modify_strings(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // strappend and strlen
  unimplemented!()
}

pub async fn should_toggle_boolean(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_get_value_type(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}
