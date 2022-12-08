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
use fred::{
  pool::RedisPool,
  prelude::*,
  types::{BackpressureConfig, BackpressurePolicy, PerformanceConfig},
};
use indicatif::ProgressBar;
use opentelemetry::{
  global,
  sdk::{
    export::trace::stdout,
    runtime::{Runtime, Tokio},
    trace::{self, RandomIdGenerator, Sampler, TraceRuntime},
  },
};
use opentelemetry_jaeger::JaegerTraceRuntime;
use rand::{self, distributions::Alphanumeric, Rng};
use std::{
  default::Default,
  sync::{atomic::AtomicUsize, Arc},
  thread::{self, JoinHandle as ThreadJoinHandle},
};
use tokio::{runtime::Builder, task::JoinHandle, time::Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

static DEFAULT_COMMAND_COUNT: usize = 10_000;
static DEFAULT_CONCURRENCY: usize = 10;
static DEFAULT_HOST: &'static str = "127.0.0.1";
static DEFAULT_PORT: u16 = 6379;

mod utils;

#[derive(Debug)]
struct Argv {
  pub cluster: bool,
  pub tracing: bool,
  pub count: usize,
  pub tasks: usize,
  pub host: String,
  pub port: u16,
  pub pipeline: bool,
  pub pool: usize,
  pub quiet: bool,
}

fn parse_argv() -> Arc<Argv> {
  let yaml = load_yaml!("../cli.yml");
  let matches = App::from_yaml(yaml).get_matches();
  let tracing = matches.is_present("tracing");
  let cluster = matches.is_present("cluster");
  let quiet = matches.is_present("quiet");

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
    cluster,
    quiet,
    tracing,
    count,
    tasks,
    host,
    port,
    pipeline,
    pool,
  })
}

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(len)
    .map(char::from)
    .collect()
}

#[cfg(all(
  not(feature = "partial-tracing"),
  not(feature = "stdout-tracing"),
  not(feature = "full-tracing")
))]
pub fn setup_tracing(enable: bool) {}

#[cfg(feature = "stdout-tracing")]
pub fn setup_tracing(enable: bool) {
  if enable {
    info!("Starting stdout tracing...");
    let tracer = stdout::new_pipeline().install_simple();
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global tracing subscriber");
  }
}

#[cfg(any(feature = "partial-tracing", feature = "full-tracing"))]
pub fn setup_tracing(enable: bool) {
  let sampler = if enable {
    info!("Starting tracing...");
    Sampler::AlwaysOn
  } else {
    Sampler::AlwaysOff
  };

  global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
  let jaeger_install = opentelemetry_jaeger::new_agent_pipeline()
    .with_service_name("pipeline-test")
    .with_trace_config(
      trace::config()
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_max_attributes_per_span(32),
    )
    .install_simple();

  let tracer = match jaeger_install {
    Ok(t) => t,
    Err(e) => panic!("Fatal error initializing tracing: {:?}", e),
  };

  let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
  let subscriber = Registry::default().with(telemetry);
  tracing::subscriber::set_global_default(subscriber).expect("Failed to set global tracing subscriber");

  info!("Initialized opentelemetry-jaeger pipeline.");
}

fn spawn_client_task(
  bar: &Option<ProgressBar>,
  client: &RedisClient,
  counter: &Arc<AtomicUsize>,
  argv: &Arc<Argv>,
) -> JoinHandle<Result<(), RedisError>> {
  let (bar, client, counter, argv) = (bar.clone(), client.clone(), counter.clone(), argv.clone());

  tokio::spawn(async move {
    let key = random_string(15);
    let mut expected = 0;

    while utils::incr_atomic(&counter) < argv.count {
      expected += 1;
      let actual: i64 = client.incr(&key).await?;
      if let Some(ref bar) = bar {
        bar.inc(1);
      }
      // assert_eq!(actual, expected);
    }

    Ok(())
  })
}

fn main() {
  pretty_env_logger::init();
  let argv = parse_argv();
  info!("Running with configuration: {:?}", argv);

  let sch = Builder::new_multi_thread().enable_all().build().unwrap();
  let output = sch.block_on(async move {
    setup_tracing(argv.tracing);

    let counter = Arc::new(AtomicUsize::new(0));
    let config = RedisConfig {
      server: if argv.cluster {
        ServerConfig::Clustered {
          hosts: vec![(argv.host.clone(), argv.port)],
        }
      } else {
        ServerConfig::new_centralized(&argv.host, argv.port)
      },
      #[cfg(any(feature = "stdout-tracing", feature = "partial-tracing", feature = "full-tracing"))]
      tracing: argv.tracing,
      ..Default::default()
    };
    let perf = PerformanceConfig {
      auto_pipeline: argv.pipeline,
      backpressure: BackpressureConfig {
        policy: BackpressurePolicy::Drain,
        max_in_flight_commands: 100_000_000,
        ..Default::default()
      },
      ..Default::default()
    };

    let pool = RedisPool::new(config, Some(perf), None, argv.pool)?;

    info!("Connecting to {}:{}...", argv.host, argv.port);
    let _ = pool.connect();
    let _ = pool.wait_for_connect().await?;
    info!("Connected to {}:{}.", argv.host, argv.port);
    let _ = pool.flushall_cluster().await?;

    info!("Starting commands...");
    let mut tasks = Vec::with_capacity(argv.tasks);
    let bar = if argv.quiet {
      None
    } else {
      Some(ProgressBar::new(argv.count as u64))
    };

    let started = Instant::now();
    for _ in 0..argv.tasks {
      tasks.push(spawn_client_task(&bar, pool.next(), &counter, &argv));
    }
    let _ = futures::future::try_join_all(tasks).await?;

    let duration = Instant::now().duration_since(started);
    let duration_sec = duration.as_secs() as f64 + (duration.subsec_millis() as f64 / 1000.0);
    if let Some(bar) = bar {
      bar.finish();
    }

    if argv.quiet {
      println!("{}", (argv.count as f64 / duration_sec) as u32);
    } else {
      println!(
        "Performed {} operations in: {:?}. Throughput: {} req/sec",
        argv.count,
        duration,
        (argv.count as f64 / duration_sec) as u32
      );
    }
    let _ = pool.flushall_cluster().await?;
    global::shutdown_tracer_provider();

    Ok::<_, RedisError>(())
  });
  if let Err(e) = output {
    eprintln!("Script finished with error: {:?}", e);
  }
}
