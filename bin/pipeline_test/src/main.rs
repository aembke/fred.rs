#[macro_use]
extern crate clap;
extern crate fred;
extern crate futures;
extern crate opentelemetry;
extern crate opentelemetry_jaeger;
extern crate tokio;
extern crate tracing;
extern crate tracing_opentelemetry;
extern crate tracing_subscriber;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use clap::{App, ArgMatches};
use fred::pool::StaticRedisPool;
use fred::prelude::*;
use opentelemetry::global;
use opentelemetry::sdk::trace::{self, IdGenerator, Sampler};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::thread::{self, JoinHandle as ThreadJoinHandle};
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

static DEFAULT_COMMAND_COUNT: usize = 10_000;
static DEFAULT_CONCURRENCY: usize = 10;
static DEFAULT_HOST: &'static str = "127.0.0.1";
static DEFAULT_PORT: u16 = 6379;
static TEST_KEY: &'static str = "foo";

mod utils;
use fred::globals;

#[derive(Debug)]
struct Argv {
  pub tracing: bool,
  pub count: usize,
  pub tasks: usize,
  pub host: String,
  pub port: u16,
  pub pipeline: bool,
  pub pool: usize,
}

fn parse_argv() -> Arc<Argv> {
  let yaml = load_yaml!("../cli.yml");
  let matches = App::from_yaml(yaml).get_matches();
  let tracing = matches.is_present("tracing");
  let count = matches
    .value_of("count")
    .map(|v| {
      v.parse::<usize>().unwrap_or_else(|_| {
        panic!("Invalid command count: {}.", v);
      })
    })
    .unwrap_or(DEFAULT_COMMAND_COUNT);
  let tasks = matches
    .value_of("concurrency")
    .map(|v| {
      v.parse::<usize>().unwrap_or_else(|_| {
        panic!("Invalid concurrency: {}.", v);
      })
    })
    .unwrap_or(DEFAULT_CONCURRENCY);
  let host = matches
    .value_of("host")
    .map(|v| v.to_owned())
    .unwrap_or("127.0.0.1".into());
  let port = matches
    .value_of("port")
    .map(|v| v.parse::<u16>().expect("Invalid port"))
    .unwrap_or(DEFAULT_PORT);
  let pool = matches
    .value_of("pool")
    .map(|v| v.parse::<usize>().expect("Invalid pool"))
    .unwrap_or(1);
  let pipeline = matches.subcommand_matches("pipeline").is_some();

  Arc::new(Argv {
    tracing,
    count,
    tasks,
    host,
    port,
    pipeline,
    pool,
  })
}

pub fn setup_tracing(enable: bool) -> ThreadJoinHandle<()> {
  thread::spawn(move || {
    let sampler = if enable {
      info!("Starting tracing...");
      Sampler::AlwaysOn
    } else {
      Sampler::AlwaysOff
    };

    let basic_sch = match Builder::new_current_thread().enable_all().build() {
      Ok(sch) => sch,
      Err(e) => panic!("Error initializing tracing tokio scheduler: {:?}", e),
    };
    let _ = basic_sch.block_on(async {
      global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
      let jaeger_install = opentelemetry_jaeger::new_pipeline()
        .with_service_name("pipeline-test")
        .with_collector_endpoint("http://localhost:14268/api/traces")
        .with_trace_config(
          trace::config()
            .with_sampler(sampler)
            .with_id_generator(IdGenerator::default())
            .with_max_attributes_per_span(32),
        )
        .install_batch(opentelemetry::runtime::Tokio);

      let tracer = match jaeger_install {
        Ok(t) => t,
        Err(e) => panic!("Fatal error initializing tracing: {:?}", e),
      };

      let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
      let subscriber = Registry::default().with(telemetry);
      let _tracing_guard = tracing::subscriber::set_global_default(subscriber);

      info!("Initialized opentelemetry-jaeger pipeline.");
      std::future::pending::<()>().await
    });

    warn!("Exiting jaeger tokio runtime thread.");
    ()
  })
}

fn spawn_client_task(
  client: &RedisClient,
  counter: &Arc<AtomicUsize>,
  argv: &Arc<Argv>,
) -> JoinHandle<Result<(), RedisError>> {
  let (client, counter, argv) = (client.clone(), counter.clone(), argv.clone());

  tokio::spawn(async move {
    while utils::incr_atomic(&counter) < argv.count {
      let _: () = client.incr(TEST_KEY).await?;
    }

    Ok(())
  })
}

fn main() {
  pretty_env_logger::init();
  globals::set_backpressure_count(1000000);
  let argv = parse_argv();
  info!("Running with configuration: {:?}", argv);

  let _ = setup_tracing(argv.tracing);
  let sch = Builder::new_multi_thread().enable_all().build().unwrap();

  let output = sch.block_on(async move {
    let counter = Arc::new(AtomicUsize::new(0));
    let config = RedisConfig {
      server: ServerConfig::new_centralized(&argv.host, argv.port),
      ..Default::default()
    };
    let pool = StaticRedisPool::new(config, argv.pool)?;

    info!("Connecting to {}:{}...", argv.host, argv.port);
    let _ = pool.connect(None);
    let _ = pool.wait_for_connect().await?;
    info!("Connected to {}:{}.", argv.host, argv.port);
    let _ = pool.del(TEST_KEY).await?;

    info!("Starting commands...");
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(argv.tasks);
    for _ in 0..argv.tasks {
      tasks.push(spawn_client_task(pool.next(), &counter, &argv));
    }

    for task in tasks.into_iter() {
      let _ = task.await?;
    }
    let duration = Instant::now().duration_since(started);
    let duration_sec = duration.as_secs() as f64 + (duration.subsec_millis() as f64 / 1000.0);
    println!(
      "Performed {} operations in: {:?}. Throughput: {} req/sec",
      argv.count,
      duration,
      (argv.count as f64 / duration_sec) as u32
    );

    let _ = pool.del(TEST_KEY).await?;
    global::shutdown_tracer_provider();

    Ok::<_, RedisError>(())
  });
  if let Err(e) = output {
    eprintln!("Script finished with error: {:?}", e);
  }
}
