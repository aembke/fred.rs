use fred::client::RedisClient;
use fred::error::{RedisError, RedisErrorKind};
use fred::prelude::Blocking;
use fred::types::{ClientUnblockFlag, RedisConfig, ServerConfig};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::sleep;

pub async fn should_automatically_unblock(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  config.blocking = Blocking::Interrupt;
  let client = RedisClient::new(config);
  let _ = client.connect(None);
  let _ = client.wait_for_connect().await?;

  let unblock_client = client.clone();
  let _ = tokio::spawn(async move {
    sleep(Duration::from_secs(1)).await;
    let _ = unblock_client.ping().await;
  });

  let result = client.blpop::<(), _>("foo", 60.0).await;
  assert!(result.is_err());
  assert_ne!(*result.unwrap_err().kind(), RedisErrorKind::Timeout);
  Ok(())
}

pub async fn should_manually_unblock(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let connections_ids = client.connection_ids().await?;
  let unblock_client = client.clone();

  let _ = tokio::spawn(async move {
    sleep(Duration::from_secs(1)).await;

    for (_, id) in connections_ids.into_iter() {
      let _ = unblock_client
        .client_unblock::<(), _>(id, Some(ClientUnblockFlag::Error))
        .await;
    }
  });

  let result = client.blpop::<(), _>("foo", 60.0).await;
  assert!(result.is_err());
  assert_ne!(*result.unwrap_err().kind(), RedisErrorKind::Timeout);
  Ok(())
}

pub async fn should_error_when_blocked(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  config.blocking = Blocking::Error;
  let client = RedisClient::new(config);
  let _ = client.connect(None);
  let _ = client.wait_for_connect().await?;
  let error_client = client.clone();

  let _ = tokio::spawn(async move {
    sleep(Duration::from_secs(1)).await;

    let result = error_client.ping().await;
    assert!(result.is_err());
    assert_eq!(*result.unwrap_err().kind(), RedisErrorKind::InvalidCommand);

    let _ = error_client.unblock_self(None).await;
  });

  let result = client.blpop::<(), _>("foo", 60.0).await;
  assert!(result.is_err());
  Ok(())
}

pub async fn should_split_clustered_connection(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let clients = client.split_cluster().await?;

  let actual = clients
    .iter()
    .map(|client| client.client_config())
    .fold(BTreeSet::new(), |mut set, config| {
      if let ServerConfig::Centralized { host, port, .. } = config.server {
        set.insert(format!("{}:{}", host, port));
      } else {
        panic!("expected centralized config");
      }

      set
    });

  let mut expected = BTreeSet::new();
  expected.insert("127.0.0.1:30001".to_owned());
  expected.insert("127.0.0.1:30002".to_owned());
  expected.insert("127.0.0.1:30003".to_owned());

  assert_eq!(actual, expected);

  Ok(())
}

#[cfg(feature = "metrics")]
pub async fn should_track_size_stats(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let _ = client.take_res_size_metrics();
  let _ = client.take_req_size_metrics();

  let _ = client
    .set("foo", "abcdefghijklmnopqrstuvxyz", None, None, false)
    .await?;
  let req_stats = client.take_req_size_metrics();
  let res_stats = client.take_res_size_metrics();

  // manually calculated with the redis_protocol crate `encode` function (not shown here)
  let expected_req_size = 54;
  let expected_res_size = 5;

  assert_eq!(req_stats.sum, expected_req_size);
  assert_eq!(req_stats.samples, 1);
  assert_eq!(res_stats.sum, expected_res_size);
  assert_eq!(res_stats.samples, 1);

  Ok(())
}

pub async fn should_run_flushall_cluster(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let count: i64 = 200;

  for idx in 0..count {
    let _: () = client.set(format!("foo-{}", idx), idx, None, None, false).await?;
  }
  let _ = client.flushall_cluster().await?;

  for idx in 0..count {
    let value: Option<i64> = client.get(format!("foo-{}", idx)).await?;
    assert!(value.is_none());
  }

  Ok(())
}
