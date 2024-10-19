use super::utils;
use async_trait::async_trait;
use fred::{
  clients::{RedisClient, RedisPool},
  cmd,
  error::{RedisError, RedisErrorKind},
  interfaces::*,
  prelude::{Blocking, RedisValue},
  types::{
    BackpressureConfig,
    Builder,
    ClientUnblockFlag,
    ClusterDiscoveryPolicy,
    ClusterHash,
    Options,
    PerformanceConfig,
    RedisConfig,
    RedisKey,
    RedisMap,
    ServerConfig,
  },
};
use futures::future::try_join;
use parking_lot::RwLock;
use redis_protocol::resp3::types::RespVersion;
use std::{
  collections::{BTreeMap, BTreeSet, HashMap},
  convert::TryInto,
  mem,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};
use tokio::time::sleep;

#[cfg(feature = "subscriber-client")]
use fred::clients::SubscriberClient;
use fred::prelude::Server;
#[cfg(feature = "credential-provider")]
use fred::types::CredentialProvider;
#[cfg(feature = "replicas")]
use fred::types::ReplicaConfig;
#[cfg(feature = "dns")]
use fred::types::Resolve;
#[cfg(feature = "partial-tracing")]
use fred::types::TracingConfig;
#[cfg(feature = "dns")]
use hickory_resolver::{config::*, TokioAsyncResolver};
#[cfg(feature = "dns")]
use std::net::{IpAddr, SocketAddr};

#[cfg(all(feature = "i-keys", feature = "i-hashes"))]
fn hash_to_btree(vals: &RedisMap) -> BTreeMap<RedisKey, u16> {
  vals
    .iter()
    .map(|(key, value)| (key.clone(), value.as_u64().unwrap() as u16))
    .collect()
}

#[cfg(all(feature = "i-keys", feature = "i-hashes"))]
fn array_to_set<T: Ord>(vals: Vec<T>) -> BTreeSet<T> {
  vals.into_iter().collect()
}

#[cfg(feature = "i-keys")]
pub fn incr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_add(1, Ordering::AcqRel).saturating_add(1)
}

#[cfg(all(feature = "i-keys", feature = "i-hashes"))]
pub async fn should_smoke_test_from_redis_impl(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let nested_values: RedisMap = vec![("a", 1), ("b", 2)].try_into()?;
  client.set("foo", "123", None, None, false).await?;
  client.set("baz", "456", None, None, false).await?;
  client.hset("bar", &nested_values).await?;

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

#[cfg(all(feature = "i-client", feature = "i-lists"))]
pub async fn should_automatically_unblock(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  config.blocking = Blocking::Interrupt;
  let client = RedisClient::new(config, None, None, None);
  client.connect();
  client.wait_for_connect().await?;

  let unblock_client = client.clone();
  tokio::spawn(async move {
    sleep(Duration::from_secs(1)).await;
    let _: () = unblock_client.ping().await.expect("Failed to ping");
  });

  let result = client.blpop::<(), _>("foo", 60.0).await;
  assert!(result.is_err());
  assert_ne!(*result.unwrap_err().kind(), RedisErrorKind::Timeout);
  Ok(())
}

#[cfg(all(feature = "i-client", feature = "i-lists"))]
pub async fn should_manually_unblock(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let connections_ids = client.connection_ids().await;
  let unblock_client = client.clone();

  tokio::spawn(async move {
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

#[cfg(all(feature = "i-client", feature = "i-lists"))]
pub async fn should_error_when_blocked(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  config.blocking = Blocking::Error;
  let client = RedisClient::new(config, None, None, None);
  client.connect();
  client.wait_for_connect().await?;
  let error_client = client.clone();

  tokio::spawn(async move {
    sleep(Duration::from_secs(1)).await;

    let result = error_client.ping::<()>().await;
    assert!(result.is_err());
    assert_eq!(*result.unwrap_err().kind(), RedisErrorKind::InvalidCommand);

    let _ = error_client.unblock_self(None).await;
  });

  let result = client.blpop::<(), _>("foo", 60.0).await;
  assert!(result.is_err());
  Ok(())
}

pub async fn should_split_clustered_connection(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let actual = client
    .split_cluster()?
    .iter()
    .map(|client| client.client_config())
    .fold(BTreeSet::new(), |mut set, config| {
      if let ServerConfig::Centralized { server } = config.server {
        set.insert(server);
      } else {
        panic!("expected centralized config");
      }

      set
    });

  assert_eq!(actual.len(), 3);
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

#[cfg(feature = "i-server")]
pub async fn should_run_flushall_cluster(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let count: i64 = 200;

  for idx in 0 .. count {
    client
      .custom(cmd!("SET"), vec![format!("foo-{}", idx), idx.to_string()])
      .await?;
  }
  client.flushall_cluster().await?;

  for idx in 0 .. count {
    let value: Option<i64> = client.custom(cmd!("GET"), vec![format!("foo-{}", idx)]).await?;
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
      other.ping().await?;
      sleep(Duration::from_millis(10)).await;
    }
  });

  // switch protocols every half second
  for idx in 0 .. 15 {
    let version = if idx % 2 == 0 {
      RespVersion::RESP2
    } else {
      RespVersion::RESP3
    };
    client.hello(version, None, None).await?;
    sleep(Duration::from_millis(500)).await;
  }
  let _ = mem::replace(&mut *done.write(), true);

  let _ = jh.await?;
  Ok(())
}

// test to repro an intermittent race condition found while stress testing the client
#[allow(dead_code)]
#[cfg(feature = "i-keys")]
pub async fn should_test_high_concurrency_pool(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  config.blocking = Blocking::Block;
  let perf = PerformanceConfig {
    auto_pipeline: true,
    backpressure: BackpressureConfig {
      max_in_flight_commands: 100_000_000,
      ..Default::default()
    },
    ..Default::default()
  };
  let pool = RedisPool::new(config, Some(perf), None, None, 28)?;
  pool.connect();
  pool.wait_for_connect().await?;

  let num_tasks = 11641;
  let mut tasks = Vec::with_capacity(num_tasks);
  let counter = Arc::new(AtomicUsize::new(0));

  for idx in 0 .. num_tasks {
    let client = pool.next().clone();
    let counter = counter.clone();

    tasks.push(tokio::spawn(async move {
      let key = format!("foo-{}", idx);

      let mut expected = 0;
      while incr_atomic(&counter) < 50_000_000 {
        let actual: i64 = client.incr(&key).await?;
        expected += 1;
        if actual != expected {
          return Err(RedisError::new(
            RedisErrorKind::Unknown,
            format!("Expected {}, found {}", expected, actual),
          ));
        }
      }

      // println!("Task {} finished.", idx);
      Ok::<_, RedisError>(())
    }));
  }
  let _ = futures::future::try_join_all(tasks).await?;

  Ok(())
}

#[cfg(feature = "i-keys")]
pub async fn should_pipeline_all(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let pipeline = client.pipeline();

  let result: RedisValue = pipeline.set("foo", 1, None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.set("bar", 2, None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.incr("foo").await?;
  assert!(result.is_queued());

  let result: ((), (), i64) = pipeline.all().await?;
  assert_eq!(result.2, 2);
  Ok(())
}

#[cfg(all(feature = "i-keys", feature = "i-hashes"))]
pub async fn should_pipeline_all_error_early(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let pipeline = client.pipeline();

  let result: RedisValue = pipeline.set("foo", 1, None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.hgetall("foo").await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.incr("foo").await?;
  assert!(result.is_queued());

  if let Err(e) = pipeline.all::<RedisValue>().await {
    // make sure we get the expected error from the server rather than a parsing error
    assert_eq!(*e.kind(), RedisErrorKind::InvalidArgument);
  } else {
    panic!("Expected pipeline error.");
  }

  Ok(())
}

#[cfg(feature = "i-keys")]
pub async fn should_pipeline_last(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let pipeline = client.pipeline();

  let result: RedisValue = pipeline.set("foo", 1, None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.set("bar", 2, None, None, false).await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.incr("foo").await?;
  assert!(result.is_queued());

  let result: i64 = pipeline.last().await?;
  assert_eq!(result, 2);
  Ok(())
}

#[cfg(all(feature = "i-keys", feature = "i-hashes"))]
pub async fn should_pipeline_try_all(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let pipeline = client.pipeline();

  pipeline.incr("foo").await?;
  pipeline.hgetall("foo").await?;
  let results = pipeline.try_all::<i64>().await;

  assert_eq!(results[0].clone().unwrap(), 1);
  assert!(results[1].is_err());

  Ok(())
}

#[cfg(feature = "i-server")]
pub async fn should_use_all_cluster_nodes_repeatedly(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let other = client.clone();
  let jh1 = tokio::spawn(async move {
    for _ in 0 .. 200 {
      other.flushall_cluster().await?;
    }

    Ok::<_, RedisError>(())
  });
  let jh2 = tokio::spawn(async move {
    for _ in 0 .. 200 {
      client.flushall_cluster().await?;
    }

    Ok::<_, RedisError>(())
  });

  let _ = try_join(jh1, jh2).await?;
  Ok(())
}

#[cfg(all(feature = "partial-tracing", feature = "i-keys"))]
pub async fn should_use_tracing_get_set(client: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  config.tracing = TracingConfig::new(true);
  let (perf, policy) = (client.perf_config(), client.client_reconnect_policy());
  let client = RedisClient::new(config, Some(perf), None, policy);
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let _: () = client.set("foo", "bar", None, None, false).await?;
  assert_eq!(client.get::<String, _>("foo").await?, "bar");
  Ok(())
}

// #[cfg(feature = "dns")]
// pub struct TrustDnsResolver(TokioAsyncResolver);
//
// #[cfg(feature = "dns")]
// impl TrustDnsResolver {
// fn new() -> Self {
// TrustDnsResolver(TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default()).unwrap())
// }
// }
//
// #[cfg(feature = "dns")]
// #[async_trait]
// impl Resolve for TrustDnsResolver {
// async fn resolve(&self, host: String, port: u16) -> Result<SocketAddr, RedisError> {
// println!("Looking up {}", host);
// self.0.lookup_ip(&host).await.map_err(|e| e.into()).and_then(|ips| {
// let ip = match ips.iter().next() {
// Some(ip) => ip,
// None => return Err(RedisError::new(RedisErrorKind::IO, "Failed to lookup IP address.")),
// };
//
// debug!("Mapped {}:{} to {}:{}", host, port, ip, port);
// Ok(SocketAddr::new(ip, port))
// })
// }
// }
//
// #[cfg(feature = "dns")]
// TODO fix the DNS configuration in docker so trustdns works
// pub async fn should_use_trust_dns(client: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
// let perf = client.perf_config();
// let policy = client.client_reconnect_policy();
//
// if let ServerConfig::Centralized { ref mut host, .. } = config.server {
// host = utils::read_redis_centralized_host().0;
// }
// if let ServerConfig::Clustered { ref mut hosts } = config.server {
// hosts[0].0 = utils::read_redis_cluster_host().0;
// }
//
// println!("Trust DNS host: {:?}", config.server.hosts());
// let client = RedisClient::new(config, Some(perf), policy);
// client.set_resolver(Arc::new(TrustDnsResolver::new())).await;
//
// let _ = client.connect();
// let _ = client.wait_for_connect().await?;
// let _: () = client.ping().await?;
// let _ = client.quit().await?;
// Ok(())
// }

#[cfg(feature = "subscriber-client")]
pub async fn should_ping_with_subscriber_client(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  let (perf, policy) = (client.perf_config(), client.client_reconnect_policy());
  let client = SubscriberClient::new(config, Some(perf), None, policy);
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let _: () = client.ping().await?;
  let _: () = client.subscribe("foo").await?;
  let _: () = client.ping().await?;
  let _ = client.quit().await?;
  Ok(())
}

#[cfg(all(feature = "replicas", feature = "i-keys"))]
pub async fn should_replica_set_and_get(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  let policy = client.client_reconnect_policy();
  let mut connection = client.connection_config().clone();
  connection.replica = ReplicaConfig::default();
  let client = RedisClient::new(config, None, Some(connection), policy);
  client.init().await?;

  let _: () = client.set("foo", "bar", None, None, false).await?;
  let result: String = client.replicas().get("foo").await?;
  assert_eq!(result, "bar");

  Ok(())
}

#[cfg(all(feature = "replicas", feature = "i-keys"))]
pub async fn should_replica_set_and_get_not_lazy(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  let policy = client.client_reconnect_policy();
  let mut connection = client.connection_config().clone();
  connection.replica.lazy_connections = false;
  let client = RedisClient::new(config, None, Some(connection), policy);
  client.init().await?;

  let _: () = client.set("foo", "bar", None, None, false).await?;
  let result: String = client.replicas().get("foo").await?;
  assert_eq!(result, "bar");

  Ok(())
}

#[cfg(all(feature = "replicas", feature = "i-keys"))]
pub async fn should_pipeline_with_replicas(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.set("foo", 1, None, None, false).await?;
  let _: () = client.set("bar", 2, None, None, false).await?;

  let pipeline = client.replicas().pipeline();
  let _: () = pipeline.get("foo").await?;
  let _: () = pipeline.get("bar").await?;
  let result: (i64, i64) = pipeline.all().await?;

  assert_eq!(result, (1, 2));
  Ok(())
}

#[cfg(all(feature = "replicas", feature = "i-keys"))]
pub async fn should_use_cluster_replica_without_redirection(
  client: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  let mut connection = client.connection_config().clone();
  connection.replica = ReplicaConfig {
    lazy_connections: true,
    primary_fallback: false,
    ignore_reconnection_errors: true,
    ..ReplicaConfig::default()
  };
  connection.max_redirections = 0;
  let policy = client.client_reconnect_policy();

  let client = RedisClient::new(config, None, Some(connection), policy);
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let _: () = client.replicas().get("foo").await?;
  let _: () = client.incr("foo").await?;

  Ok(())
}

pub async fn should_gracefully_quit(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let client = client.clone_new();
  let connection = client.connect();
  client.wait_for_connect().await?;

  client.ping().await?;
  client.quit().await?;
  let _ = connection.await;

  Ok(())
}

#[cfg(feature = "i-lists")]
pub async fn should_support_options_with_pipeline(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let options = Options {
    timeout: Some(Duration::from_millis(100)),
    max_attempts: Some(42),
    max_redirections: Some(43),
    ..Default::default()
  };

  let pipeline = client.pipeline().with_options(&options);
  pipeline.blpop("foo", 2.0).await?;
  let results = pipeline.try_all::<RedisValue>().await;
  assert_eq!(results[0].clone().unwrap_err().kind(), &RedisErrorKind::Timeout);

  Ok(())
}

#[cfg(feature = "i-keys")]
pub async fn should_reuse_pipeline(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let pipeline = client.pipeline();
  pipeline.incr("foo").await?;
  pipeline.incr("foo").await?;
  assert_eq!(pipeline.last::<i64>().await?, 2);
  assert_eq!(pipeline.last::<i64>().await?, 4);
  Ok(())
}

#[cfg(all(feature = "transactions", feature = "i-keys"))]
pub async fn should_support_options_with_trx(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let options = Options {
    max_attempts: Some(1),
    timeout: Some(Duration::from_secs(1)),
    ..Default::default()
  };
  let trx = client.multi().with_options(&options);

  trx.get("foo{1}").await?;
  trx.set("foo{1}", "bar", None, None, false).await?;
  trx.get("foo{1}").await?;
  let (first, second, third): (Option<RedisValue>, bool, String) = trx.exec(true).await?;

  assert_eq!(first, None);
  assert!(second);
  assert_eq!(third, "bar");
  Ok(())
}

#[cfg(all(feature = "transactions", feature = "i-keys"))]
pub async fn should_pipeline_transaction(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.incr("foo{1}").await?;
  client.incr("bar{1}").await?;

  let trx = client.multi();
  trx.pipeline(true);
  trx.get("foo{1}").await?;
  trx.incr("bar{1}").await?;
  let (foo, bar): (i64, i64) = trx.exec(true).await?;
  assert_eq!((foo, bar), (1, 2));

  Ok(())
}

#[cfg(all(feature = "transactions", feature = "i-keys", feature = "i-hashes"))]
pub async fn should_fail_pipeline_transaction_error(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.incr("foo{1}").await?;
  client.incr("bar{1}").await?;

  let trx = client.multi();
  trx.pipeline(true);
  trx.get("foo{1}").await?;
  trx.hgetall("bar{1}").await?;
  trx.get("foo{1}").await?;

  if let Err(e) = trx.exec::<RedisValue>(false).await {
    assert_eq!(*e.kind(), RedisErrorKind::InvalidArgument);
  } else {
    panic!("Expected error from transaction.");
  }

  Ok(())
}

#[cfg(all(feature = "i-keys", feature = "i-lists"))]
pub async fn should_manually_connect_twice(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let client = client.clone_new();
  let _old_connection = client.connect();
  client.wait_for_connect().await?;

  let _blpop_jh = tokio::spawn({
    let client = client.clone();
    async move { client.blpop::<Option<i64>, _>("foo", 5.0).await }
  });

  sleep(Duration::from_millis(100)).await;
  let new_connection = client.connect();
  client.wait_for_connect().await?;

  assert_eq!(client.incr::<i64, _>("bar").await?, 1);
  client.quit().await?;
  let _ = new_connection.await?;
  Ok(())
}

pub async fn pool_should_connect_correctly_via_init_interface(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  let pool = Builder::from_config(config).build_pool(5)?;
  let task = pool.init().await?;

  pool.ping().await?;
  pool.quit().await?;
  task.await??;
  Ok(())
}

pub async fn pool_should_fail_with_bad_host_via_init_interface(
  _: RedisClient,
  mut config: RedisConfig,
) -> Result<(), RedisError> {
  config.fail_fast = true;
  config.server = ServerConfig::new_centralized("incorrecthost", 1234);
  let pool = Builder::from_config(config).build_pool(5)?;
  assert!(pool.init().await.is_err());
  Ok(())
}

pub async fn pool_should_connect_correctly_via_wait_interface(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  let pool = Builder::from_config(config).build_pool(5)?;
  let task = pool.connect();
  pool.wait_for_connect().await?;

  pool.ping().await?;
  pool.quit().await?;
  task.await??;
  Ok(())
}

pub async fn pool_should_fail_with_bad_host_via_wait_interface(
  _: RedisClient,
  mut config: RedisConfig,
) -> Result<(), RedisError> {
  config.fail_fast = true;
  config.server = ServerConfig::new_centralized("incorrecthost", 1234);
  let pool = Builder::from_config(config).build_pool(5)?;
  let task = pool.connect();
  assert!(pool.wait_for_connect().await.is_err());

  let _ = task.await;
  Ok(())
}

pub async fn should_connect_correctly_via_init_interface(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  let client = Builder::from_config(config).build()?;
  let task = client.init().await?;

  client.ping().await?;
  client.quit().await?;
  task.await??;
  Ok(())
}

pub async fn should_fail_with_bad_host_via_init_interface(
  _: RedisClient,
  mut config: RedisConfig,
) -> Result<(), RedisError> {
  config.fail_fast = true;
  config.server = ServerConfig::new_centralized("incorrecthost", 1234);
  let client = Builder::from_config(config).build()?;
  assert!(client.init().await.is_err());
  Ok(())
}

pub async fn should_connect_correctly_via_wait_interface(
  _: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  let client = Builder::from_config(config).build()?;
  let task = client.connect();
  client.wait_for_connect().await?;

  client.ping().await?;
  client.quit().await?;
  task.await??;
  Ok(())
}

pub async fn should_fail_with_bad_host_via_wait_interface(
  _: RedisClient,
  mut config: RedisConfig,
) -> Result<(), RedisError> {
  config.fail_fast = true;
  config.server = ServerConfig::new_centralized("incorrecthost", 1234);
  let client = Builder::from_config(config).build()?;
  let task = client.connect();
  assert!(client.wait_for_connect().await.is_err());

  let _ = task.await;
  Ok(())
}

// TODO this will require a breaking change to support. The `Replicas` struct assumes that it's operating on a
// `RedisClient` and is not generic for other client or decorator types. `Replicas` must become `Replicas<C:
// ClientLike>` first.
#[allow(dead_code)]
#[cfg(all(feature = "replicas", feature = "i-keys"))]
pub async fn should_combine_options_and_replicas(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  let mut connection = client.connection_config().clone();
  connection.replica = ReplicaConfig {
    lazy_connections: true,
    primary_fallback: false,
    ignore_reconnection_errors: true,
    ..ReplicaConfig::default()
  };
  connection.max_redirections = 0;
  let policy = client.client_reconnect_policy();
  let client = RedisClient::new(config, None, Some(connection), policy);
  client.init().await?;

  // change the cluster hash policy such that we get a routing error if both replicas and options are correctly
  // applied
  let key = RedisKey::from_static_str("foo");
  let (servers, foo_owner) = client
    .cached_cluster_state()
    .map(|s| {
      (
        s.unique_primary_nodes(),
        s.get_server(key.cluster_hash()).unwrap().clone(),
      )
    })
    .unwrap();
  let wrong_owner = servers.iter().find(|s| foo_owner != **s).unwrap().clone();

  let options = Options {
    max_redirections: Some(0),
    max_attempts: Some(1),
    cluster_node: Some(wrong_owner),
    ..Default::default()
  };

  let error = client
    .with_options(&options)
    .replicas()
    .get::<Option<String>, _>(key)
    .await
    .err()
    .unwrap();

  // not ideal
  assert_eq!(error.details(), "Too many redirections.");
  Ok(())
}

pub async fn should_fail_on_centralized_connect(_: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  if let ServerConfig::Centralized { server } = config.server {
    config.server = ServerConfig::Clustered {
      hosts:  vec![server],
      policy: ClusterDiscoveryPolicy::default(),
    };
  } else {
    // skip for unix socket and sentinel tests
    return Ok(());
  }

  let client = RedisClient::new(config, None, None, None);
  client.connect();

  if let Err(err) = client.wait_for_connect().await {
    assert_eq!(*err.kind(), RedisErrorKind::Config, "err = {:?}", err);
    return Ok(());
  }

  Err(RedisError::new(RedisErrorKind::Unknown, "Expected a config error."))
}

#[derive(Debug, Default)]
#[cfg(feature = "credential-provider")]
pub struct FakeCreds {}

#[async_trait]
#[cfg(feature = "credential-provider")]
impl CredentialProvider for FakeCreds {
  async fn fetch(&self, _: Option<&Server>) -> Result<(Option<String>, Option<String>), RedisError> {
    use super::utils::{read_redis_password, read_redis_username};
    Ok((Some(read_redis_username()), Some(read_redis_password())))
  }
}
#[cfg(feature = "credential-provider")]
pub async fn should_use_credential_provider(_client: RedisClient, mut config: RedisConfig) -> Result<(), RedisError> {
  let (perf, connection) = (_client.perf_config(), _client.connection_config().clone());
  config.username = None;
  config.password = None;
  config.credential_provider = Some(Arc::new(FakeCreds::default()));
  let client = Builder::from_config(config)
    .set_connection_config(connection)
    .set_performance_config(perf)
    .build()?;

  client.init().await?;
  client.ping().await?;
  client.quit().await?;
  Ok(())
}

#[cfg(feature = "i-pubsub")]
pub async fn should_exit_event_task_with_error(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let task = client.on_message(|_| Err(RedisError::new_canceled()));
  client.subscribe("foo").await?;

  let publisher = client.clone_new();
  publisher.init().await?;
  publisher.publish("foo", "bar").await?;

  let result = task.await.unwrap();
  assert_eq!(result, Err(RedisError::new_canceled()));
  Ok(())
}
