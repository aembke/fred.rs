use fred::{
  prelude::*,
  types::{ClusterHash, CustomCommand},
};
use std::convert::TryInto;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = Builder::default_centralized().build()?;
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;
  let _: () = client.lpush("foo", vec![1, 2, 3]).await?;

  let cmd = CustomCommand::new_static("LRANGE", ClusterHash::FirstKey, false);
  // some types require TryInto
  let args: Vec<RedisValue> = vec!["foo".into(), 0.into(), 3_u64.try_into()?];
  // returns a frame (https://docs.rs/redis-protocol/latest/redis_protocol/resp3/types/enum.Frame.html)
  let frame = client.custom_raw(cmd, args).await?;
  // or convert back to client types
  let value: RedisValue = frame.try_into()?;
  // and/or use the type conversion shorthand
  let value: Vec<String> = value.convert()?;
  println!("LRANGE Values: {:?}", value);

  Ok(())
}
