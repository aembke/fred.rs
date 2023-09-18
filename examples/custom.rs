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
  let _ = client.wait_for_connect().await?;
  let _: () = client.lpush("foo", vec![1, 2, 3]).await?;

  // some types require TryInto
  let args: Vec<RedisValue> = vec!["foo".into(), 0.into(), 3_u64.try_into()?];
  // returns a frame (https://docs.rs/redis-protocol/latest/redis_protocol/resp3/types/enum.Frame.html)
  let frame = client.custom_raw(cmd!("LRANGE"), args).await?;
  // or convert back to client types
  let value: RedisValue = frame.try_into()?;
  // and/or use the type conversion shorthand
  let value: Vec<String> = value.convert()?;
  println!("LRANGE Values: {:?}", value);

  // or customize routing and blocking parameters
  let _command = cmd!("FOO.BAR", blocking: true);
  let _command = cmd!("FOO.BAR", hash: ClusterHash::FirstKey);
  let _command = cmd!("FOO.BAR", hash: ClusterHash::FirstKey, blocking: true);
  // which is shorthand for
  let _command = CustomCommand::new("FOO.BAR", ClusterHash::FirstKey, true);

  Ok(())
}
