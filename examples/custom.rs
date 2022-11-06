use fred::{
  prelude::*,
  types::{ClusterHash, CustomCommand, RedisKey},
};
use redis_protocol::resp3::types::Frame;
use std::convert::TryInto;

fn get_hash_slot(client: &RedisClient, key: &'static str) -> (RedisKey, Option<u16>) {
  let key = RedisKey::from_static_str(key);
  let hash_slot = if client.is_clustered() {
    // or use redis_protocol::redis_keyslot(key.as_bytes())
    Some(key.cluster_hash())
  } else {
    None
  };

  (key, hash_slot)
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let client = RedisClient::new(RedisConfig::default(), None, None);
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let (key, hash_slot) = get_hash_slot(&client, "ts:carbon_monoxide");
  let args: Vec<RedisValue> = vec![key.into(), 1112596200.into(), 1112603400.into()];
  let cmd = CustomCommand::new_static("TS.RANGE", hash_slot, false);
  // >> TS.RANGE ts:carbon_monoxide 1112596200 1112603400
  // 1) 1) (integer) 1112596200
  // 2) "2.4"
  // 2) 1) (integer) 1112599800
  // 2) "2.1"
  // 3) 1) (integer) 1112603400
  // 2) "2.2"
  let values: Vec<(i64, f64)> = client.custom(cmd, args).await?;
  println!("TS.RANGE Values: {:?}", values);

  let _: () = client.lpush("foo", vec![1, 2, 3]).await?;
  let (key, hash_slot) = get_hash_slot(&client, "foo");
  let cmd = CustomCommand::new_static("LRANGE", hash_slot, false);
  // some types require TryInto
  let args: Vec<RedisValue> = vec![key.into(), 0.into(), 3_u64.try_into()?];
  // returns a frame (https://docs.rs/redis-protocol/latest/redis_protocol/resp3/types/enum.Frame.html)
  let frame = client.custom_raw(cmd, args).await?;
  // or convert back to client types
  let value: RedisValue = frame.try_into()?;
  // and/or use the type conversion shorthand
  let value: Vec<String> = value.convert()?;
  println!("LRANGE Values: {:?}", value);

  Ok(())
}
