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

  Argv {
    cluster,
    host,
    port,
    pool,
  }
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();
  let argv = parse_argv();
  info!("Running with configuration: {:?}", argv);

  let config = RedisConfig {
    server: if argv.cluster {
      ServerConfig::Clustered {
        hosts: vec![(argv.host.clone(), argv.port)],
      }
    } else {
      ServerConfig::new_centralized(&argv.host, argv.port)
    },
    ..Default::default()
  };
  let perf = PerformanceConfig {
    auto_pipeline: false,
    ..Default::default()
  };
  let policy = ReconnectPolicy::new_linear(0, 5000, 500);
  let pool = RedisPool::new(config, Some(perf), Some(policy), argv.pool).expect("Failed to create pool.");

  info!("Connecting to {}:{}...", argv.host, argv.port);
  let _ = pool.connect();
  let _ = pool.wait_for_connect().await?;
  info!("Connected to {}:{}.", argv.host, argv.port);
  let _ = pool.flushall_cluster().await?;

  loop {
    let _: i64 = pool.incr("foo").await.expect("Failed to INCR");
    sleep(Duration::from_secs(1)).await;
  }
}
