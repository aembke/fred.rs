#![allow(unused_macros)]

use crate::chaos_monkey::set_test_kind;
use fred::{
  clients::RedisClient,
  error::RedisError,
  interfaces::*,
  types::{PerformanceConfig, ReconnectPolicy, RedisConfig, ServerConfig},
};
use redis_protocol::resp3::prelude::RespVersion;
use std::{default::Default, env, future::Future};

#[cfg(feature = "chaos-monkey")]
const RECONNECT_DELAY: u32 = 500;
#[cfg(not(feature = "chaos-monkey"))]
const RECONNECT_DELAY: u32 = 1000;

pub fn read_env_var(name: &str) -> Option<String> {
  env::var_os(name).and_then(|s| s.into_string().ok())
}

fn read_fail_fast_env() -> bool {
  match env::var_os("FRED_FAIL_FAST") {
    Some(s) => match s.into_string() {
      Ok(s) => match s.as_ref() {
        "f" | "false" | "FALSE" | "0" => false,
        _ => true,
      },
      Err(_) => true,
    },
    None => true,
  }
}

fn read_redis_centralized_host() -> (String, u16) {
  let host = read_env_var("FRED_REDIS_CENTRALIZED_HOST").unwrap_or("127.0.0.1".into());
  let port = read_env_var("FRED_REDIS_CENTRALIZED_PORT")
    .and_then(|s| s.parse::<u16>().ok())
    .unwrap_or(6379);

  (host, port)
}

fn read_redis_cluster_host() -> (String, u16) {
  let host = read_env_var("FRED_REDIS_CLUSTER_HOST").unwrap_or("127.0.0.1".into());
  let port = read_env_var("FRED_REDIS_CLUSTER_PORT")
    .and_then(|s| s.parse::<u16>().ok())
    .unwrap_or(30001);

  (host, port)
}

#[cfg(feature = "sentinel-auth")]
fn read_redis_password() -> String {
  read_env_var("REDIS_PASSWORD").expect("Failed to read REDIS_PASSWORD env")
}

#[cfg(feature = "sentinel-auth")]
fn read_sentinel_password() -> String {
  read_env_var("REDIS_SENTINEL_PASSWORD").expect("Failed to read REDIS_SENTINEL_PASSWORD env")
}

#[cfg(feature = "sentinel-tests")]
fn read_sentinel_hostname() -> String {
  if read_env_var("CIRCLECI_TESTS").is_some() {
    "redis-sentinel-1".to_owned()
  } else {
    "127.0.0.1".to_owned()
  }
}

#[cfg(all(feature = "sentinel-tests", not(feature = "chaos-monkey")))]
pub async fn run_sentinel<F, Fut>(func: F, pipeline: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  set_test_kind(false);

  let policy = ReconnectPolicy::new_constant(300, RECONNECT_DELAY);
  let config = RedisConfig {
    fail_fast: read_fail_fast_env(),
    server: ServerConfig::Sentinel {
      hosts:        vec![
        (read_sentinel_hostname(), 26379),
        (read_sentinel_hostname(), 26380),
        (read_sentinel_hostname(), 26381),
      ],
      service_name: "redis-sentinel-main".into(),
      // TODO fix this so sentinel-tests can run without sentinel-auth
      username:     None,
      password:     Some(read_sentinel_password()),
    },
    password: Some(read_redis_password()),
    ..Default::default()
  };
  let perf = PerformanceConfig {
    auto_pipeline: pipeline,
    default_command_timeout_ms: 10_000,
    ..Default::default()
  };
  let client = RedisClient::new(config.clone(), Some(perf), Some(policy));
  let _client = client.clone();

  let _jh = client.connect();
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _: () = client.flushall(false).await.expect("Failed to flushall");
  func(_client, config.clone()).await.expect("Failed to run test");
  let _ = client.quit().await;
}

pub async fn run_cluster<F, Fut>(func: F, pipeline: bool, resp3: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  set_test_kind(true);

  let policy = ReconnectPolicy::new_constant(300, RECONNECT_DELAY);
  let (host, port) = read_redis_cluster_host();
  let config = RedisConfig {
    fail_fast: read_fail_fast_env(),
    server: ServerConfig::Clustered {
      hosts: vec![(host, port)],
    },
    version: if resp3 { RespVersion::RESP3 } else { RespVersion::RESP2 },
    ..Default::default()
  };
  let perf = PerformanceConfig {
    auto_pipeline: pipeline,
    default_command_timeout_ms: 10_000,
    ..Default::default()
  };

  let client = RedisClient::new(config.clone(), Some(perf), Some(policy));
  let _client = client.clone();

  let _jh = client.connect();
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _: () = client.flushall_cluster().await.expect("Failed to flushall");
  func(_client, config.clone()).await.expect("Failed to run test");
  let _ = client.quit().await;
}

pub async fn run_centralized<F, Fut>(func: F, pipeline: bool, resp3: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  set_test_kind(false);

  let policy = ReconnectPolicy::new_constant(300, RECONNECT_DELAY);
  let (host, port) = read_redis_centralized_host();
  let config = RedisConfig {
    fail_fast: read_fail_fast_env(),
    server: ServerConfig::Centralized { host, port },
    version: if resp3 { RespVersion::RESP3 } else { RespVersion::RESP2 },
    ..Default::default()
  };
  let perf = PerformanceConfig {
    auto_pipeline: pipeline,
    default_command_timeout_ms: 10_000,
    ..Default::default()
  };
  let client = RedisClient::new(config.clone(), Some(perf), Some(policy));
  let _client = client.clone();

  let _jh = client.connect();
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _: () = client.flushall(false).await.expect("Failed to flushall");
  func(_client, config.clone()).await.expect("Failed to run test");
  let _ = client.quit().await;
}

macro_rules! centralized_test_panic(
  ($module:tt, $name:tt) => {
    #[cfg(not(feature="sentinel-tests"))]
    mod $name {
      mod resp2 {
        #[tokio::test]
        #[should_panic]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, true, false).await;
        }

        #[tokio::test]
        #[should_panic]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, false, false).await;
        }
      }

      mod resp3 {
        #[tokio::test]
        #[should_panic]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, true, true).await;
        }

        #[tokio::test]
        #[should_panic]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, false, true).await;
        }
      }
    }

    #[cfg(feature="sentinel-tests")]
    mod $name {
      #[tokio::test]
      #[should_panic]
      async fn sentinel_pipelined() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_sentinel(crate::integration::$module::$name, true).await;
      }

      #[tokio::test]
      #[should_panic]
      async fn sentinel_no_pipeline() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_sentinel(crate::integration::$module::$name, false).await;
      }
    }
  }
);

macro_rules! cluster_test_panic(
  ($module:tt, $name:tt) => {
    #[cfg(not(feature="sentinel-tests"))]
    mod $name {
      mod resp2 {
        #[tokio::test]
        #[should_panic]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, true, false).await;
        }

        #[tokio::test]
        #[should_panic]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, false, false).await;
        }
      }

      mod resp3 {
        #[tokio::test]
        #[should_panic]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, true, true).await;
        }

        #[tokio::test]
        #[should_panic]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, false, true).await;
        }
      }
    }
  }
);

macro_rules! centralized_test(
  ($module:tt, $name:tt) => {
    #[cfg(not(feature="sentinel-tests"))]
    mod $name {
      mod resp2 {
        #[tokio::test]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, true, false).await;
        }

        #[tokio::test]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, false, false).await;
        }
      }

      mod resp3 {
        #[tokio::test]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, true, true).await;
        }

        #[tokio::test]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_centralized(crate::integration::$module::$name, false, true).await;
        }
      }
    }

    #[cfg(feature="sentinel-tests")]
    mod $name {
      #[tokio::test]
      async fn sentinel_pipelined() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_sentinel(crate::integration::$module::$name, true).await;
      }

      #[tokio::test]
      async fn sentinel_no_pipeline() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_sentinel(crate::integration::$module::$name, false).await;
      }
    }
  }
);

macro_rules! cluster_test(
  ($module:tt, $name:tt) => {
    #[cfg(not(feature="sentinel-tests"))]
    mod $name {
      mod resp2 {
        #[tokio::test]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, true, false).await;
        }

        #[tokio::test]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, false, false).await;
        }
      }

      mod resp3 {
        #[tokio::test]
        async fn pipelined() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, true, true).await;
        }

        #[tokio::test]
        async fn no_pipeline() {
          let _ = pretty_env_logger::try_init();
          crate::integration::utils::run_cluster(crate::integration::$module::$name, false, true).await;
        }
      }
    }
  }
);

macro_rules! return_err(
  ($($arg:tt)*) => { {
    return Err(fred::error::RedisError::new(
      fred::error::RedisErrorKind::Unknown, format!($($arg)*)
    ));
  } }
);

macro_rules! check_null(
  ($client:ident, $arg:expr) => { {
    let foo: RedisValue = $client.get($arg).await?;
    if !foo.is_null() {
      panic!("expected {} to be null", $arg);
    }
  } }
);
