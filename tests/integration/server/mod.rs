use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

pub async fn should_flushall(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.set("foo{1}", "bar", None, None, false).await?;
  if client.is_clustered() {
    let _ = client.flushall_cluster().await?;
  } else {
    let _ = client.flushall(false).await?;
  };

  let result = client.get("foo{1}").await?;
  assert!(result.is_null());

  Ok(())
}

pub async fn should_read_server_info(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let info = client.info(None).await?;
  assert!(info.as_str().is_some());

  Ok(())
}

pub async fn should_ping_server(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.ping().await?;

  Ok(())
}

pub async fn should_run_custom_command(_client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  // TODO find a good third party module to test

  Ok(())
}

pub async fn should_read_last_save(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let lastsave = client.lastsave().await?;
  assert!(lastsave.as_i64().is_some());

  Ok(())
}

pub async fn should_read_db_size(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..50 {
    let _ = client.set(format!("foo-{}", idx), idx, None, None, false).await?;
  }

  // this is tricky to assert b/c the dbsize command isnt linked to a specific server in the cluster, hence the loop above
  let db_size = client.dbsize().await?;
  assert!(db_size.as_i64().unwrap() > 0);

  Ok(())
}

pub async fn should_start_bgsave(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let save_result = client.bgsave().await?;
  assert_eq!(save_result.as_str().unwrap(), "Background saving started");

  // need to ensure this finishes before it runs again or it'll return an error
  sleep(Duration::from_millis(1000)).await;
  Ok(())
}

pub async fn should_do_bgrewriteaof(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = client.bgrewriteaof().await?;
  // not much we can assert here aside from the command not failing

  // need to ensure this finishes before it runs again or it'll return an error
  sleep(Duration::from_millis(1000)).await;
  Ok(())
}
