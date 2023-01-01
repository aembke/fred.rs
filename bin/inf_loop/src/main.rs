#[macro_use]
extern crate clap;
extern crate fred;
extern crate futures;
extern crate tokio;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use clap::App;
use fred::{pool::RedisPool, prelude::*, types::PerformanceConfig};
use std::{default::Default, time::Duration};
use tokio::time::sleep;

#[derive(Debug)]
struct Argv {
  pub cluster: bool,
  pub host:    String,
  pub port:    u16,
  pub pool:    usize,
  pub interval: u64,
  pub wait: u64,
  pub auth: String,
}

fn parse_argv() -> Argv {
  let yaml = load_yaml!("../cli.yml");
  let matches = App::from_yaml(yaml).get_matches();
  let cluster = matches.is_present("cluster");

  let host = matches
    .value_of("host")
    .map(|v| v.to_owned())
    .unwrap_or("127.0.0.1".into());
  let port = matches
    .value_of("port")
    .map(|v| v.parse::<u16>().expect("Invalid port"))
    .unwrap_or(6379);
  let pool = matches
    .value_of("pool")
    .map(|v| v.parse::<usize>().expect("Invalid pool"))
    .unwrap_or(1);
  let interval = matches
    .value_of("interval")
    .map(|v| v.parse::<u64>().expect("Invalid interval"))
    .unwrap_or(1000);
  let wait = matches
    .value_of("wait")
    .map(|v| v.parse::<u64>().expect("Invalid wait"))
    .unwrap_or(0);
  let auth = matches
    .value_of("auth")
    .map(|v| v.to_owned())
    .unwrap_or("".into());

  Argv {
    cluster,
    auth,
    host,
    port,
    pool,
    interval,
    wait
  }
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init_timed();
  let argv = parse_argv();
  info!("Running with configuration: {:?}", argv);

  let config = RedisConfig {
    server: if argv.cluster {
      ServerConfig::new_clustered(vec![(&argv.host, argv.port)])
    } else {
      ServerConfig::new_centralized(&argv.host, argv.port)
    },
    password: if argv.auth.is_empty() {
      None
    }else{
      Some(argv.auth.clone())
    },
    ..Default::default()
  };
  let perf = PerformanceConfig {
    auto_pipeline: false,
    network_timeout_ms: 5_000,
    default_command_timeout_ms: 10_000,
    ..Default::default()
  };
  let policy = ReconnectPolicy::new_linear(0, 5000, 100);
  let pool = RedisPool::new(config, Some(perf), Some(policy), argv.pool).expect("Failed to create pool.");

  info!("Connecting to {}:{}...", argv.host, argv.port);
  let _ = pool.connect();
  let _ = pool.wait_for_connect().await?;
  info!("Connected to {}:{}.", argv.host, argv.port);
  let _ = pool.flushall_cluster().await?;

  if argv.wait > 0 {
    info!("Waiting for {} ms", argv.wait);
    sleep(Duration::from_millis(argv.wait)).await;
  }

  loop {
    let _: i64 = pool.incr("foo").await.expect("Failed to INCR");
    sleep(Duration::from_millis(argv.interval)).await;
  }
}
