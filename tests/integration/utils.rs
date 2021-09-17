use crate::chaos_monkey::set_test_kind;
use fred::client::RedisClient;
use fred::error::RedisError;
use fred::types::{ReconnectPolicy, RedisConfig, ServerConfig};
use std::env;
use std::future::Future;

#[cfg(feature = "chaos-monkey")]
const RECONNECT_DELAY: u32 = 500;
#[cfg(not(feature = "chaos-monkey"))]
const RECONNECT_DELAY: u32 = 1000;

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

pub async fn run_cluster<F, Fut>(func: F, pipeline: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  set_test_kind(true);

  let policy = ReconnectPolicy::new_constant(300, RECONNECT_DELAY);
  let config = RedisConfig {
    fail_fast: read_fail_fast_env(),
    server: ServerConfig::default_clustered(),
    pipeline,
    ..Default::default()
  };
  let client = RedisClient::new(config.clone());
  let _client = client.clone();

  let _jh = client.connect(Some(policy));
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _: () = client.flushall_cluster().await.expect("Failed to flushall");
  func(_client, config.clone()).await.expect("Failed to run test");
  let _ = client.quit().await;
}

pub async fn run_centralized<F, Fut>(func: F, pipeline: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  set_test_kind(false);

  let policy = ReconnectPolicy::new_constant(300, RECONNECT_DELAY);
  let config = RedisConfig {
    fail_fast: read_fail_fast_env(),
    server: ServerConfig::default_centralized(),
    pipeline,
    ..Default::default()
  };
  let client = RedisClient::new(config.clone());
  let _client = client.clone();

  let _jh = client.connect(Some(policy));
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _: () = client.flushall(false).await.expect("Failed to flushall");
  func(_client, config.clone()).await.expect("Failed to run test");
  let _ = client.quit().await;
}

macro_rules! centralized_test_panic(
  ($module:tt, $name:tt) => {
    mod $name {
      #[tokio::test]
      #[should_panic]
      async fn pipelined() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_centralized(crate::integration::$module::$name, true).await;
      }

      #[tokio::test]
      #[should_panic]
      async fn no_pipeline() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_centralized(crate::integration::$module::$name, false).await;
      }
    }
  }
);

macro_rules! cluster_test_panic(
  ($module:tt, $name:tt) => {
    mod $name {
      #[tokio::test]
      #[should_panic]
      async fn pipelined() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_cluster(crate::integration::$module::$name, true).await;
      }

      #[tokio::test]
      #[should_panic]
      async fn no_pipeline() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_cluster(crate::integration::$module::$name, false).await;
      }
    }
  }
);

macro_rules! centralized_test(
  ($module:tt, $name:tt) => {
    mod $name {
      #[tokio::test]
      async fn pipelined() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_centralized(crate::integration::$module::$name, true).await;
      }

      #[tokio::test]
      async fn no_pipeline() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_centralized(crate::integration::$module::$name, false).await;
      }
    }
  }
);

macro_rules! cluster_test(
  ($module:tt, $name:tt) => {
    mod $name {
      #[tokio::test]
      async fn pipelined() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_cluster(crate::integration::$module::$name, true).await;
      }

      #[tokio::test]
      async fn no_pipeline() {
        let _ = pretty_env_logger::try_init();
        crate::integration::utils::run_cluster(crate::integration::$module::$name, false).await;
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
