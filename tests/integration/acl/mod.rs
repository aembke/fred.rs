use fred::client::RedisClient;
use fred::error::RedisError;
use fred::types::RedisConfig;
use super::utils::read_env_var;

fn check_env_creds() -> Option<(String, String)> {
  let username = read_env_var("REDIS_USERNAME");
  let password = read_env_var("REDIS_PASSWORD");

  if username.is_some() && password.is_some() {
    Some((username.unwrap(), password.unwrap()))
  }else{
    None
  }
}

// note: currently this only works in CI against the centralized server
pub async fn should_auth_as_test_user(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  if let Some((username, password)) = check_env_creds() {
    let _ = client.auth(Some(username.clone()), &password).await?;
    let _: () = client.get("foo").await?;
  }

  Ok(())
}

// note: currently this only works in CI against the centralized server
pub async fn should_auth_as_test_user_via_config(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  if let Some((username, password)) = check_env_creds() {
    config.username = Some(username);
    config.password = Some(password);
    let client = RedisClient::new(config);
    let _ = client.connect(None);
    let _ = client.wait_for_connect().await?;
    let _: () = client.get("foo").await?;
  }

  Ok(())
}