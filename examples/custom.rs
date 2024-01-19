#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{
  cmd,
  prelude::*,
  types::{ClusterHash, CustomCommand},
};
use std::convert::TryInto;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = Builder::default_centralized().build()?;
  let _ = client.connect();
  client.wait_for_connect().await?;
  client.lpush("foo", vec![1, 2, 3]).await?;

  // some types require TryInto
  let args: Vec<RedisValue> = vec!["foo".into(), 0.into(), 3_u64.try_into()?];
  // returns a frame (https://docs.rs/redis-protocol/latest/redis_protocol/resp3/types/enum.Frame.html)
  let frame = client.custom_raw(cmd!("LRANGE"), args.clone()).await?;
  // or convert back to client types
  let value: RedisValue = frame.try_into()?;
  // and/or use the type conversion shorthand
  let value: Vec<String> = value.convert()?;
  println!("LRANGE Values: {:?}", value);

  // or customize routing and blocking parameters
  let _ = cmd!("LRANGE", blocking: false);
  let _ = cmd!("LRANGE", hash: ClusterHash::FirstKey);
  let _ = cmd!("LRANGE", hash: ClusterHash::FirstKey, blocking: false);
  // which is shorthand for
  let command = CustomCommand::new("LRANGE", ClusterHash::FirstKey, false);

  // convert to `FromRedis` types
  let _: Vec<i64> = client
    .custom_raw(command, args)
    .await
    .and_then(|frame| frame.try_into())
    .and_then(|value: RedisValue| value.convert())?;
  Ok(())
}
