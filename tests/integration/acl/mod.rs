use super::utils::read_env_var;
use fred::{
  clients::RedisClient,
  error::RedisError,
  interfaces::*,
  types::{AclUserFlag, RedisConfig},
};

// the docker image we use for sentinel tests doesn't allow for configuring users, just passwords,
// so for the tests here we just use an empty username so it uses the `default` user
#[cfg(feature = "sentinel-tests")]
fn read_redis_username() -> Option<String> {
  None
}

#[cfg(not(feature = "sentinel-tests"))]
fn read_redis_username() -> Option<String> {
  read_env_var("REDIS_USERNAME")
}

fn check_env_creds() -> (Option<String>, Option<String>) {
  (read_redis_username(), read_env_var("REDIS_PASSWORD"))
}

// note: currently this only works in CI against the centralized server
pub async fn should_auth_as_test_user(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let (username, password) = check_env_creds();
  if let Some(password) = password {
    client.auth(username, password).await?;
    client.get("foo").await?;
  }

  Ok(())
}

// note: currently this only works in CI against the centralized server
pub async fn should_auth_as_test_user_via_config(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  let (username, password) = check_env_creds();
  if let Some(password) = password {
    config.username = username;
    config.password = Some(password);
    let client = RedisClient::new(config);
    let _ = client.connect(None);
    client.wait_for_connect().await?;
    client.get("foo").await?;
  }

  Ok(())
}

pub async fn should_run_acl_getuser(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let user = client.acl_getuser("default").await?.unwrap();
  assert!(user.flags.contains(&AclUserFlag::On));

  Ok(())
}
