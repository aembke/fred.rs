use fred::prelude::*;

#[cfg(feature = "i-client")]
pub async fn should_echo_message(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let res: String = client.echo("hello world!").await?;
  assert_eq!(res, "hello world!");
  Ok(())
}
