use fred::client::RedisClient;
use fred::error::RedisError;
use fred::types::{RedisConfig, ServerConfig};
use std::future::Future;

pub async fn run_cluster<F, Fut>(func: F, pipeline: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  let config = RedisConfig {
    server: ServerConfig::default_clustered(),
    pipeline,
    ..Default::default()
  };
  let client = RedisClient::new(config.clone());
  let _client = client.clone();

  let _jh = client.connect(None);
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _ = client.flushall_cluster().await;
  func(_client, config.clone()).await.expect("Failed to run test");
  let _ = client.quit().await;
}

pub async fn run_centralized<F, Fut>(func: F, pipeline: bool)
where
  F: Fn(RedisClient, RedisConfig) -> Fut,
  Fut: Future<Output = Result<(), RedisError>>,
{
  let config = RedisConfig {
    server: ServerConfig::default_centralized(),
    pipeline,
    ..Default::default()
  };
  let client = RedisClient::new(config.clone());
  let _client = client.clone();

  let _jh = client.connect(None);
  let _ = client.wait_for_connect().await.expect("Failed to connect client");

  let _ = client.flushall(false).await;
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
  ($client:ident, $arg:expr) => {
    if !$client.get($arg).await?.is_null() {
      panic!("expected {} to be null", $arg);
    }
  }
);
