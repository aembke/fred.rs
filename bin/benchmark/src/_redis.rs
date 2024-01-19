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

// ### Background
//
// First, I'd recommend reading this: https://redis.io/docs/manual/pipelining. It's important to understand what RTT is,
// why pipelining minimizes its impact in general, and why it's often the only thing that really matters for the
// overall throughput of an IO-bound application with dependencies like Redis.
//
// These applications often share an important characteristic:
//
// End-user requests run concurrently, often in parallel in separate Tokio tasks, but need to share a small pool of
// Redis connections via some kind of dependency injection interface. These request tasks rarely have any kind of
// synchronization requirements (there's usually no reason one user's request should have to wait for another to
// finish), so ideally we could efficiently interleave their Redis commands on the wire in a way that can take
// advantage of this.
//
// For example,
//
// 1. Task A writes command 1 to server.
// 2. Task B writes command 2 to server.
// 3. Task A reads command response 1 from server.
// 4. Task B reads command response 2 from server.
//
// reduces the impact of RTT much more effectively than
//
// 1. Task A writes command 1 to server.
// 2. Task A reads command response 1 from server.
// 3. Task B writes command 2 to server.
// 4. Task B reads command response 2 from server.
//
// and the effect becomes even more pronounced as concurrency (the number of tasks) increases, at least until other
// bottlenecks kick in. You'll often see me describe this as "pipelining across tasks", whereas the `redis::Pipeline`
// and `fred::clients::Pipeline` interfaces control pipelining __within__ a task.
//
// This benchmarking tool is built specifically to represent this class of high concurrency use cases and to measure
// the impact of this particular pipelining optimization (`auto_pipeline: true` in `fred`), so it seemed interesting
// to adapt it to compare the two libraries. If this pipelining strategy is really that effective then we should see
// `fred` perform roughly the same as `redis-rs` when `auto_pipeline: false`, but it should outperform when
// `auto_pipeline: true`.
//
// If your use case is not structured this way or your stack does not use Tokio concurrency features then these
// results are likely less relevant.
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
