use fred::client::util;
use fred::prelude::*;

static ECHO_SCRIPT: &'static str = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";
static GET_SCRIPT: &'static str = "return redis.call('get', KEYS[1])";

pub async fn load_script(client: &RedisClient, script: &str) -> Result<String, RedisError> {
  if client.is_clustered() {
    client.script_load_cluster(script).await
  } else {
    client.script_load(script).await
  }
}

pub async fn flush_scripts(client: &RedisClient) -> Result<(), RedisError> {
  if client.is_clustered() {
    client.script_flush_cluster(false).await
  } else {
    client.script_flush(false).await
  }
}

pub async fn should_load_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script_hash = util::sha1_hash(ECHO_SCRIPT);
  let hash = client.script_load(ECHO_SCRIPT).await?;
  assert_eq!(hash, script_hash);

  Ok(())
}

pub async fn should_load_script_cluster(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script_hash = util::sha1_hash(ECHO_SCRIPT);
  let hash = client.script_load_cluster(ECHO_SCRIPT).await?;
  assert_eq!(hash, script_hash);

  Ok(())
}

pub async fn should_evalsha_echo_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let hash = load_script(&client, ECHO_SCRIPT).await?;

  let result: Vec<String> = client.evalsha(hash, vec!["a{1}", "b{1}"], vec!["c{1}", "d{1}"]).await?;
  assert_eq!(result, vec!["a{1}", "b{1}", "c{1}", "d{1}"]);

  let _ = flush_scripts(&client).await?;
  Ok(())
}

pub async fn should_evalsha_get_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script_hash = util::sha1_hash(GET_SCRIPT);
  let hash = load_script(&client, GET_SCRIPT).await?;
  assert_eq!(hash, script_hash);

  let result: Option<String> = client.evalsha(&script_hash, vec!["foo"], ()).await?;
  assert!(result.is_none());

  let _: () = client.set("foo", "bar", None, None, false).await?;
  let result: String = client.evalsha(&script_hash, vec!["foo"], ()).await?;
  assert_eq!(result, "bar");

  let _ = flush_scripts(&client).await?;
  Ok(())
}

pub async fn should_eval_echo_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result: Vec<String> = client
    .eval(ECHO_SCRIPT, vec!["a{1}", "b{1}"], vec!["c{1}", "d{1}"])
    .await?;
  assert_eq!(result, vec!["a{1}", "b{1}", "c{1}", "d{1}"]);

  let _ = flush_scripts(&client).await?;
  Ok(())
}

pub async fn should_eval_get_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result: Option<String> = client.eval(GET_SCRIPT, vec!["foo"], ()).await?;
  assert!(result.is_none());

  let hash = util::sha1_hash(GET_SCRIPT);
  let result: Option<String> = client.evalsha(&hash, vec!["foo"], ()).await?;
  assert!(result.is_none());

  let _: () = client.set("foo", "bar", None, None, false).await?;
  let result: String = client.eval(GET_SCRIPT, vec!["foo"], ()).await?;
  assert_eq!(result, "bar");

  let result: String = client.evalsha(&hash, vec!["foo"], ()).await?;
  assert_eq!(result, "bar");

  let _ = flush_scripts(&client).await?;
  Ok(())
}
