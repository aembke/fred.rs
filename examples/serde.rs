use fred::prelude::*;
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let value = json!({
    "foo": "a",
    "bar": "b"
  });
  // json `Value` objects can also be used interchangeably with `RedisMap` type arguments.
  let _: () = client.hset("wobble", value.clone()).await?;
  let _: () = client.set("wibble", value.to_string(), None, None, false).await?;

  // converting back to a json `Value` will also try to parse nested json strings, if possible.
  // the type conversion logic will not attempt the json parsing if the value doesn't look like json.
  // if a value looks like json, but cannot be parsed as json, then it will be returned as a string.
  let get_result: Value = client.get("wibble").await?;
  println!("GET Result: {}", get_result);
  let hget_result: Value = client.hgetall("wobble").await?;
  println!("HGETALL Result: {}", hget_result);

  assert_eq!(value, get_result);
  assert_eq!(value, hget_result);
  let _ = client.quit().await;
  Ok(())
}
