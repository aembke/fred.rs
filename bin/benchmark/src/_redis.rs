use crate::{utils, Argv};
use bb8_redis::{
  bb8::{self, Pool, PooledConnection},
  redis::{cmd, AsyncCommands, ErrorKind as RedisErrorKind, RedisError},
  RedisConnectionManager,
  RedisMultiplexedConnectionManager,
};
use futures::TryStreamExt;
use indicatif::ProgressBar;
use opentelemetry::trace::FutureExt;
use std::{
  error::Error,
  sync::{atomic::AtomicUsize, Arc},
  time::{Duration, SystemTime},
};
use tokio::task::JoinHandle;

async fn incr_key(pool: &Pool<RedisMultiplexedConnectionManager>, key: &str) -> i64 {
  let mut conn = pool.get().await.map_err(utils::crash).unwrap();
  cmd("INCR")
    .arg(key)
    .query_async(&mut *conn)
    .await
    .map_err(utils::crash)
    .unwrap()
}

async fn del_key(pool: &Pool<RedisMultiplexedConnectionManager>, key: &str) -> i64 {
  let mut conn = pool.get().await.map_err(utils::crash).unwrap();
  cmd("DEL")
    .arg(key)
    .query_async(&mut *conn)
    .await
    .map_err(utils::crash)
    .unwrap()
}

fn spawn_client_task(
  bar: &Option<ProgressBar>,
  pool: &Pool<RedisMultiplexedConnectionManager>,
  counter: &Arc<AtomicUsize>,
  argv: &Arc<Argv>,
) -> JoinHandle<()> {
  let (bar, pool, counter, argv) = (bar.clone(), pool.clone(), counter.clone(), argv.clone());

  tokio::spawn(async move {
    let key = utils::random_string(15);
    let mut expected = 0;

    while utils::incr_atomic(&counter) < argv.count {
      expected += 1;
      let actual = incr_key(&pool, &key).await;

      #[cfg(feature = "assert-expected")]
      {
        if actual != expected {
          println!("Unexpected result: {} == {}", actual, expected);
          std::process::exit(1);
        }
      }

      if let Some(ref bar) = bar {
        bar.inc(1);
      }
    }
  })
}

// TODO support clustered deployments
async fn init(argv: &Arc<Argv>) -> Pool<RedisMultiplexedConnectionManager> {
  let (username, password) = utils::read_auth_env();
  let url = if let Some(password) = password {
    let username = username.map(|s| format!("{s}:")).unwrap_or("".into());
    format!("redis://{}{}@{}:{}", username, password, argv.host, argv.port)
  } else {
    format!("redis://{}:{}", argv.host, argv.port)
  };
  debug!("Redis conn: {}", url);

  let manager = RedisMultiplexedConnectionManager::new(url).expect("Failed to create redis connection manager");
  let pool = bb8::Pool::builder()
    .max_size(argv.pool as u32)
    .build(manager)
    .await
    .expect("Failed to create client pool");

  // try to warm up the pool first
  let mut warmup_ft = Vec::with_capacity(argv.pool);
  for _ in 0 .. argv.pool + 1 {
    warmup_ft.push(async { incr_key(&pool, "foo").await });
  }
  futures::future::join_all(warmup_ft).await;
  del_key(&pool, "foo").await;

  pool
}

pub async fn run(argv: Arc<Argv>, counter: Arc<AtomicUsize>, bar: Option<ProgressBar>) -> Duration {
  info!("Running with redis-rs");

  if argv.cluster || argv.replicas {
    panic!("Cluster or replica features are not supported yet with redis-rs benchmarks.");
  }
  let pool = init(&argv).await;
  let mut tasks = Vec::with_capacity(argv.tasks);

  info!("Starting commands...");
  let started = SystemTime::now();
  for _ in 0 .. argv.tasks {
    tasks.push(spawn_client_task(&bar, &pool, &counter, &argv));
  }
  futures::future::join_all(tasks).await;

  SystemTime::now()
    .duration_since(started)
    .expect("Failed to calculate duration")
}
