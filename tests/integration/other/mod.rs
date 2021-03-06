use fred::clients::RedisClient;
use fred::error::{RedisError, RedisErrorKind};
use fred::interfaces::*;
use fred::prelude::{Blocking, RedisValue};
use fred::types::{ClientUnblockFlag, RedisConfig, RedisKey, RedisMap, ServerConfig};
use parking_lot::RwLock;
use redis_protocol::resp3::types::RespVersion;
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn hash_to_btree(vals: &RedisMap) -> BTreeMap<RedisKey, u16> {
  vals
    .iter()
    .map(|(key, value)| (key.clone(), value.as_u64().unwrap() as u16))
    .collect()
}

fn array_to_set<T: Ord>(vals: Vec<T>) -> BTreeSet<T> {
  vals.into_iter().collect()
}

pub async fn should_smoke_test_from_redis_impl(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let nested_values: RedisMap = vec![("a", 1), ("b", 2)].try_into()?;
  let _ = client.set("foo", "123", None, None, false).await?;
  let _ = client.set("baz", "456", None, None, false).await?;
  let _ = client.hset("bar", &nested_values).await?;

  let foo: usize = client.get("foo").await?;
  assert_eq!(foo, 123);
  let foo: i64 = client.get("foo").await?;
  assert_eq!(foo, 123);
  let foo: String = client.get("foo").await?;
  assert_eq!(foo, "123");
  let foo: Vec<u8> = client.get("foo").await?;
  assert_eq!(foo, "123".as_bytes());
  let foo: Vec<String> = client.hvals("bar").await?;
  assert_eq!(array_to_set(foo), array_to_set(vec!["1".to_owned(), "2".to_owned()]));
  let foo: BTreeSet<String> = client.hvals("bar").await?;
  assert_eq!(foo, array_to_set(vec!["1".to_owned(), "2".to_owned()]));
  let foo: HashMap<String, u16> = client.hgetall("bar").await?;
  assert_eq!(foo, RedisValue::Map(nested_values.clone()).convert()?);
  let foo: BTreeMap<RedisKey, u16> = client.hgetall("bar").await?;
  assert_eq!(foo, hash_to_btree(&nested_values));
  let foo: (String, i64) = client.mget(vec!["foo", "baz"]).await?;
  assert_eq!(foo, ("123".into(), 456));
  let foo: Vec<(String, i64)> = client.hgetall("bar").await?;
  assert_eq!(array_to_set(foo), array_to_set(vec![("a".into(), 1), ("b".into(), 2)]));

  Ok(())
}

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

pub async fn should_safely_change_protocols_repeatedly(
  client: RedisClient,
  _: RedisConfig,
) -> Result<(), RedisError> {
  let done = Arc::new(RwLock::new(false));
  let other = client.clone();
  let other_done = done.clone();

  let jh = tokio::spawn(async move {
    loop {
      if *other_done.read() {
        return Ok::<_, RedisError>(());
      }

      let _ = other.incr("foo").await?;
      sleep(Duration::from_millis(10)).await;
    }
  });

  // switch protocols every half second
  for idx in 0..20 {
    let version = if idx % 2 == 0 {
      RespVersion::RESP2
    } else {
      RespVersion::RESP3
    };
    let _ = client.hello(version, None).await?;
    sleep(Duration::from_millis(500)).await;
  }
  let _ = mem::replace(&mut *done.write(), true);

  let _ = jh.await?;
  Ok(())
}
