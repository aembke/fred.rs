use fred::{
  clients::RedisClient,
  error::RedisError,
  interfaces::RedisJsonInterface,
  json_quote,
  types::{RedisConfig, RedisValue},
  util::NONE,
};
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
  let _: () = client.json_set("foo", "$", json!(["a", "b"]), None).await?;

  // need to double quote string values
  let size: i64 = client
    .json_arrappend("foo", Some("$"), vec![json_quote!("c"), json_quote!("d")])
    .await?;
  assert_eq!(size, 4);
  let size: i64 = client.json_arrappend("foo", Some("$"), vec![json!({"e": "f"})]).await?;
  assert_eq!(size, 5);
  let len: i64 = client.json_arrlen("foo", NONE).await?;
  assert_eq!(len, 5);

  let result: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(result[0], json!(["a", "b", "c", "d", {"e": "f"}]));

  Ok(())
}

pub async fn should_modify_arrays(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.json_set("foo", "$", json!(["a", "d"]), None).await?;
  let len: i64 = client
    .json_arrinsert("foo", "$", 1, vec![json_quote!("b"), json_quote!("c")])
    .await?;
  assert_eq!(len, 4);
  let idx: usize = client.json_arrindex("foo", "$", json_quote!("b"), None, None).await?;
  assert_eq!(idx, 1);
  let len: usize = client.json_arrlen("foo", NONE).await?;
  assert_eq!(len, 4);

  Ok(())
}

pub async fn should_pop_and_trim_arrays(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.json_set("foo", "$", json!(["a", "b"]), None).await?;
  let val: Value = client.json_arrpop("foo", NONE, None).await?;
  assert_eq!(val, json!("b"));

  let _: () = client.json_set("foo", "$", json!(["a", "b", "c", "d"]), None).await?;
  let len: usize = client.json_arrtrim("foo", "$", 0, -2).await?;
  assert_eq!(len, 3);

  let vals: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(vals[0], json!(["a", "b", "c"]));

  Ok(())
}

pub async fn should_get_set_del_obj(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let value = json!({
    "a": "b",
    "c": 1,
    "d": true
  });
  let _: () = client.json_set("foo", "$", value.clone(), None).await?;
  let result: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value, result[0]);

  let count: i64 = client.json_del("foo", "$..c").await?;
  assert_eq!(count, 1);

  let result: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(result[0], json!({ "a": "b", "d": true }));

  Ok(())
}

pub async fn should_merge_objects(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_mset_and_mget(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  unimplemented!()
}

pub async fn should_incr_numbers(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.json_set("foo", "$", json!({ "a": 1 }), None).await?;
  let vals: Value = client.json_numincrby("foo", "$.a", 2).await?;
  assert_eq!(vals[0], 3);

  Ok(())
}

pub async fn should_inspect_objects(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let value = json!({
    "a": "b",
    "e": {
      "f": "g",
      "h": "i",
      "j": [{ "k": "l" }]
    }
  });
  let _: () = client.json_set("foo", "$", value.clone(), None).await?;
  let keys: Vec<Vec<String>> = client.json_objkeys("foo", Some("$")).await?;
  assert_eq!(keys[0], vec!["a".to_string(), "e".to_string()]);
  let keys: Vec<Vec<String>> = client.json_objkeys("foo", Some("$.e")).await?;
  assert_eq!(keys[0], vec!["f".to_string(), "h".to_string(), "j".to_string()]);

  let len: usize = client.json_objlen("foo", NONE).await?;
  assert_eq!(len, 2);
  let len: usize = client.json_objlen("foo", Some("$.e")).await?;
  assert_eq!(len, 3);

  Ok(())
}

pub async fn should_modify_strings(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.json_set("foo", "$", json!({ "a": "abc123" }), None).await?;
  let len: usize = client.json_strlen("foo", Some("$.a")).await?;
  assert_eq!(len, 6);

  let len: usize = client.json_strappend("foo", Some("$.a"), json_quote!("456")).await?;
  assert_eq!(len, 9);
  let len: usize = client.json_strlen("foo", Some("$.a")).await?;
  assert_eq!(len, 9);
  let value: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value[0], json!({ "a": "abc123456" }));

  Ok(())
}

pub async fn should_toggle_boolean(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.json_set("foo", "$", json!({ "a": 1, "b": true }), None).await?;
  let new_val: bool = client.json_toggle("foo", "$.b").await?;
  assert_eq!(new_val, false);

  Ok(())
}

pub async fn should_get_value_type(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.json_set("foo", "$", json!({ "a": 1, "b": true }), None).await?;
  let val: String = client.json_type("foo", NONE).await?;
  assert_eq!(val, "object");
  let val: String = client.json_type("foo", Some("$.a")).await?;
  assert_eq!(val, "integer");
  let val: String = client.json_type("foo", Some("$.b")).await?;
  assert_eq!(val, "boolean");

  Ok(())
}
