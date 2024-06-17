use fred::{
  clients::{RedisClient, RedisPool},
  error::RedisError,
  interfaces::*,
  types::{Builder, ReconnectPolicy, RedisConfig},
};
use futures::future::try_join_all;

async fn create_and_ping_pool(config: &RedisConfig, count: usize) -> Result<(), RedisError> {
  let pool = RedisPool::new(config.clone(), None, None, None, count)?;
  pool.init().await?;

  for client in pool.clients().iter() {
    client.ping().await?;
  }

  pool.ping().await?;
  pool.quit().await?;
  Ok(())
}

pub async fn should_connect_and_ping_static_pool_single_conn(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  create_and_ping_pool(&config, 1).await
}

pub async fn should_connect_and_ping_static_pool_two_conn(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  create_and_ping_pool(&config, 2).await
}

pub async fn should_incr_exclusive_pool(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  let perf = client.perf_config();
  let policy = client
    .client_reconnect_policy()
    .unwrap_or(ReconnectPolicy::new_linear(0, 1000, 100));
  let connection = client.connection_config().clone();
  let pool = Builder::from_config(config)
    .set_performance_config(perf)
    .set_policy(policy)
    .set_connection_config(connection)
    .build_exclusive_pool(5)?;
  pool.init().await?;

  for _ in 0 .. 10 {
    let client = pool.next().await;
    client.incr("foo").await?;
  }
  assert_eq!(client.get::<i64, _>("foo").await?, 10);
  client.del("foo").await?;

  let mut fts = Vec::with_capacity(10);
  for _ in 0 .. 10 {
    let pool = pool.clone();
    fts.push(async move {
      let client = pool.next().await;
      client.incr::<i64, _>("foo").await
    });
  }
  try_join_all(fts).await?;
  assert_eq!(client.get::<i64, _>("foo").await?, 10);

  Ok(())
}

pub async fn should_watch_and_trx_exclusive_pool(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  let perf = client.perf_config();
  let policy = client
    .client_reconnect_policy()
    .unwrap_or(ReconnectPolicy::new_linear(0, 1000, 100));
  let connection = client.connection_config().clone();
  let pool = Builder::from_config(config)
    .set_performance_config(perf)
    .set_policy(policy)
    .set_connection_config(connection)
    .build_exclusive_pool(5)?;
  pool.init().await?;

  client.set("foo{1}", 1, None, None, false).await?;

  let results: Option<(i64, i64, i64)> = {
    let client = pool.next().await;

    client.watch("foo").await?;
    if let Some(1) = client.get::<Option<i64>, _>("foo{1}").await? {
      let trx = client.multi();
      trx.incr("foo{1}").await?;
      trx.incr("bar{1}").await?;
      trx.incr("baz{1}").await?;
      Some(trx.exec(true).await?)
    } else {
      None
    }
  };
  assert_eq!(results, Some((2, 1, 1)));
  assert_eq!(client.get::<i64, _>("bar{1}").await?, 1);
  assert_eq!(client.get::<i64, _>("baz{1}").await?, 1);

  Ok(())
}
