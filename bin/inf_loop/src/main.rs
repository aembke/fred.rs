#[macro_use]
extern crate clap;
extern crate fred;
extern crate futures;
extern crate tokio;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use clap::App;
use fred::{
  bytes::Bytes,
  prelude::*,
  types::{ReplicaConfig, UnresponsiveConfig},
};
use rand::{self, distributions::Alphanumeric, Rng};
use std::{default::Default, time::Duration};
use tokio::time::sleep;

#[derive(Debug)]
struct Argv {
  pub cluster:  bool,
  pub replicas: bool,
  pub host:     String,
  pub port:     u16,
  pub pool:     usize,
  pub interval: u64,
  pub wait:     u64,
  pub auth:     String,
}

fn parse_argv() -> Argv {
  let yaml = load_yaml!("../cli.yml");
  let matches = App::from_yaml(yaml).get_matches();
  let cluster = matches.is_present("cluster");
  let replicas = matches.is_present("replicas");

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
  let auth = matches.value_of("auth").map(|v| v.to_owned()).unwrap_or("".into());

  Argv {
    cluster,
    auth,
    host,
    port,
    pool,
    interval,
    wait,
    replicas,
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
    } else {
      Some(argv.auth.clone())
    },
    ..Default::default()
  };
  let pool = Builder::from_config(config)
    .with_connection_config(|config| {
      config.max_command_attempts = 3;
      config.unresponsive = UnresponsiveConfig {
        interval:    Duration::from_secs(1),
        max_timeout: Some(Duration::from_secs(5)),
      };
      config.connection_timeout = Duration::from_secs(3);
      config.internal_command_timeout = Duration::from_secs(2);
      config.cluster_cache_update_delay = Duration::from_secs(20);
      if argv.replicas {
        config.replica = ReplicaConfig {
          lazy_connections: true,
          primary_fallback: true,
          connection_error_count: 1,
          ..Default::default()
        };
      }
    })
    .with_performance_config(|config| {
      config.auto_pipeline = true;
      config.default_command_timeout = Duration::from_secs(60 * 5);
    })
    .set_policy(ReconnectPolicy::new_linear(0, 5000, 100))
    .build_pool(argv.pool)
    .expect("Failed to create pool");

  info!("Connecting to {}:{}...", argv.host, argv.port);
  pool.init().await?;
  info!("Connected to {}:{}.", argv.host, argv.port);
  pool.flushall_cluster().await?;

  if argv.wait > 0 {
    info!("Waiting for {} ms", argv.wait);
    sleep(Duration::from_millis(argv.wait)).await;
  }

  tokio::spawn(async move {
    tokio::signal::ctrl_c().await;
    std::process::exit(0);
  });
  loop {
    if argv.replicas {
      let _: Option<Bytes> = pool.replicas().get("foo").await.expect("Failed to GET");
    } else {
      let _: i64 = pool.incr("foo").await.expect("Failed to INCR");
    }
    sleep(Duration::from_millis(argv.interval)).await;
  }
}
