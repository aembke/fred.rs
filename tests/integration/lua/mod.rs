use bytes::Bytes;
use fred::{
  prelude::*,
  types::{FnPolicy, Function, Library, Script},
  util,
};
use std::{
  collections::{BTreeSet, HashMap},
  ops::Deref,
};

static ECHO_SCRIPT: &str = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";
#[cfg(feature = "sha-1")]
static GET_SCRIPT: &str = "return redis.call('get', KEYS[1])";

#[cfg(feature = "sha-1")]
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

#[cfg(feature = "sha-1")]
pub async fn should_load_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script_hash = util::sha1_hash(ECHO_SCRIPT);
  let hash: String = client.script_load(ECHO_SCRIPT).await?;
  assert_eq!(hash, script_hash);

  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_load_script_cluster(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script_hash = util::sha1_hash(ECHO_SCRIPT);
  let hash: String = client.script_load_cluster(ECHO_SCRIPT).await?;
  assert_eq!(hash, script_hash);

  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_evalsha_echo_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let hash = load_script(&client, ECHO_SCRIPT).await?;

  let result: Vec<String> = client.evalsha(hash, vec!["a{1}", "b{1}"], vec!["c{1}", "d{1}"]).await?;
  assert_eq!(result, vec!["a{1}", "b{1}", "c{1}", "d{1}"]);

  flush_scripts(&client).await?;
  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_evalsha_with_reload_echo_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script = Script::from_lua(ECHO_SCRIPT);

  let result: Vec<String> = script
    .evalsha_with_reload(&client, vec!["a{1}", "b{1}"], vec!["c{1}", "d{1}"])
    .await?;
  assert_eq!(result, vec!["a{1}", "b{1}", "c{1}", "d{1}"]);

  flush_scripts(&client).await?;
  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_evalsha_get_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let script_hash = util::sha1_hash(GET_SCRIPT);
  let hash = load_script(&client, GET_SCRIPT).await?;
  assert_eq!(hash, script_hash);

  let result: Option<String> = client.evalsha(&script_hash, vec!["foo"], ()).await?;
  assert!(result.is_none());

  client.set("foo", "bar", None, None, false).await?;
  let result: String = client.evalsha(&script_hash, vec!["foo"], ()).await?;
  assert_eq!(result, "bar");

  flush_scripts(&client).await?;
  Ok(())
}

pub async fn should_eval_echo_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result: Vec<String> = client
    .eval(ECHO_SCRIPT, vec!["a{1}", "b{1}"], vec!["c{1}", "d{1}"])
    .await?;
  assert_eq!(result, vec!["a{1}", "b{1}", "c{1}", "d{1}"]);

  flush_scripts(&client).await?;
  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_eval_get_script(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result: Option<String> = client.eval(GET_SCRIPT, vec!["foo"], ()).await?;
  assert!(result.is_none());

  let hash = util::sha1_hash(GET_SCRIPT);
  let result: Option<String> = client.evalsha(&hash, vec!["foo"], ()).await?;
  assert!(result.is_none());

  client.set("foo", "bar", None, None, false).await?;
  let result: String = client.eval(GET_SCRIPT, vec!["foo"], ()).await?;
  assert_eq!(result, "bar");

  let result: String = client.evalsha(&hash, vec!["foo"], ()).await?;
  assert_eq!(result, "bar");

  flush_scripts(&client).await?;
  Ok(())
}

pub async fn should_function_load_scripts(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  let getset_fn = include_str!("../../scripts/lua/getset.lua");

  let echo: String = client.function_load(true, echo_fn).await?;
  assert_eq!(echo, "echolib");
  let getset: String = client.function_load(true, getset_fn).await?;
  assert_eq!(getset, "getsetlib");
  client.function_load_cluster(true, echo_fn).await?;

  Ok(())
}

pub async fn should_function_dump_and_restore(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;

  let fns: Bytes = client.function_dump().await?;
  client.function_flush_cluster(false).await?;
  client.function_restore_cluster(fns, FnPolicy::default()).await?;

  let mut fns: Vec<HashMap<String, RedisValue>> = client.function_list(Some("echolib"), false).await?;
  assert_eq!(fns.len(), 1);
  let fns = fns.pop().expect("Failed to pop function");
  assert_eq!(fns.get("library_name"), Some(&RedisValue::String("echolib".into())));

  Ok(())
}

pub async fn should_function_flush(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;
  let fns: RedisValue = client.function_list(Some("echolib"), false).await?;
  assert!(!fns.is_null());

  client.function_flush_cluster(false).await?;
  let fns: RedisValue = client.function_list(Some("echolib"), false).await?;
  assert!(fns.is_null() || fns.array_len() == Some(0));

  Ok(())
}

pub async fn should_function_delete(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;
  let fns: RedisValue = client.function_list(Some("echolib"), false).await?;
  assert!(!fns.is_null());

  client.function_delete_cluster("echolib").await?;
  let fns: RedisValue = client.function_list(Some("echolib"), false).await?;
  assert!(fns.is_null() || fns.array_len() == Some(0));

  Ok(())
}

pub async fn should_function_list(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;
  let getset_fn = include_str!("../../scripts/lua/getset.lua");
  client.function_load_cluster(true, getset_fn).await?;

  let mut fns: Vec<HashMap<String, RedisValue>> = client.function_list(Some("echolib"), false).await?;
  assert_eq!(fns.len(), 1);
  let fns = fns.pop().expect("Failed to pop function");
  assert_eq!(fns.get("library_name"), Some(&RedisValue::String("echolib".into())));

  Ok(())
}

pub async fn should_function_list_multiple(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;
  let getset_fn = include_str!("../../scripts/lua/getset.lua");
  client.function_load_cluster(true, getset_fn).await?;

  let fns: Vec<HashMap<String, RedisValue>> = client.function_list(None::<String>, false).await?;

  // ordering is not deterministic, so convert to a set of library names
  let fns: BTreeSet<String> = fns
    .into_iter()
    .map(|lib| {
      lib
        .get("library_name")
        .expect("Failed to read library name")
        .as_string()
        .expect("Failed to convert to string.")
    })
    .collect();
  let mut expected = BTreeSet::new();
  expected.insert("echolib".into());
  expected.insert("getsetlib".into());

  assert_eq!(fns, expected);
  Ok(())
}

pub async fn should_function_fcall_getset(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let getset_fn = include_str!("../../scripts/lua/getset.lua");
  client.function_load_cluster(true, getset_fn).await?;

  client.set("foo{1}", "bar", None, None, false).await?;
  let old: String = client.fcall("getset", vec!["foo{1}"], vec!["baz"]).await?;
  assert_eq!(old, "bar");
  let new: String = client.get("foo{1}").await?;
  assert_eq!(new, "baz");

  Ok(())
}

pub async fn should_function_fcall_echo(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;

  let result: Vec<String> = client
    .fcall("echo", vec!["key1{1}", "key2{1}"], vec!["arg1", "arg2"])
    .await?;
  assert_eq!(result, vec!["key1{1}", "key2{1}", "arg1", "arg2"]);

  Ok(())
}

pub async fn should_function_fcall_ro_echo(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);

  let echo_fn = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_fn).await?;

  let result: Vec<String> = client
    .fcall_ro("echo", vec!["key1{1}", "key2{1}"], vec!["arg1", "arg2"])
    .await?;
  assert_eq!(result, vec!["key1{1}", "key2{1}", "arg1", "arg2"]);

  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_create_lua_script_helper_from_code(
  client: RedisClient,
  _: RedisConfig,
) -> Result<(), RedisError> {
  let script = Script::from_lua(ECHO_SCRIPT);
  script.load(&client).await?;

  let result: Vec<RedisValue> = script
    .evalsha(&client, vec!["foo{1}", "bar{1}"], vec!["3", "4"])
    .await?;
  assert_eq!(result, vec!["foo{1}".into(), "bar{1}".into(), "3".into(), "4".into()]);
  Ok(())
}

#[cfg(feature = "sha-1")]
pub async fn should_create_lua_script_helper_from_hash(
  client: RedisClient,
  _: RedisConfig,
) -> Result<(), RedisError> {
  let hash: String = client.script_load_cluster(ECHO_SCRIPT).await?;

  let script = Script::from_hash(hash);
  let result: Vec<RedisValue> = script
    .evalsha(&client, vec!["foo{1}", "bar{1}"], vec!["3", "4"])
    .await?;
  assert_eq!(result, vec!["foo{1}".into(), "bar{1}".into(), "3".into(), "4".into()]);
  Ok(())
}

pub async fn should_create_function_from_code(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);
  let echo_lib = include_str!("../../scripts/lua/echo.lua");

  let lib = Library::from_code(&client, echo_lib).await?;
  assert_eq!(lib.name().deref(), "echolib");
  let func = lib.functions().get("echo").expect("Failed to read echo function");

  let result: Vec<RedisValue> = func.fcall(&client, vec!["foo{1}", "bar{1}"], vec!["3", "4"]).await?;
  assert_eq!(result, vec!["foo{1}".into(), "bar{1}".into(), "3".into(), "4".into()]);
  Ok(())
}

pub async fn should_create_function_from_name(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_redis_7!(client);
  let echo_lib = include_str!("../../scripts/lua/echo.lua");
  client.function_load_cluster(true, echo_lib).await?;

  let lib = Library::from_name(&client, "echolib").await?;
  let func = lib.functions().get("echo").expect("Failed to read echo function");

  let result: Vec<RedisValue> = func.fcall(&client, vec!["foo{1}", "bar{1}"], vec!["3", "4"]).await?;
  assert_eq!(result, vec!["foo{1}".into(), "bar{1}".into(), "3".into(), "4".into()]);
  Ok(())
}
