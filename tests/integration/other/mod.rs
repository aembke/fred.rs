use fred::client::RedisClient;
use fred::error::RedisError;
use fred::types::RedisConfig;
use std::collections::BTreeSet;

pub async fn should_split_clustered_connection(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let clients = client.split_cluster().await?;

  let actual = clients
    .iter()
    .map(|client| client.client_config())
    .fold(BTreeSet::new(), |mut set, config| {
      if let RedisConfig::Centralized { host, port, .. } = config {
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
    let _ = client.set(format!("foo-{}", idx), idx, None, None, false).await?;
  }
  let _ = client.flushall_cluster().await?;

  for idx in 0..count {
    let value = client.get(format!("foo-{}", idx)).await?;
    assert!(value.is_null());
  }

  Ok(())
}
