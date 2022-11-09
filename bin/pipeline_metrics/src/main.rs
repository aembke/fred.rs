#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use clap::{load_yaml, App, ArgMatches};
use indicatif::ProgressBar;
use std::sync::Arc;
use subprocess::{Popen, PopenConfig};

struct Argv {
  pub count:             u64,
  pub pipeline:          bool,
  pub pool_range:        (u32, u32),
  pub pool_step:         u32,
  pub concurrency_range: (u32, u32),
  pub concurrency_step:  u32,
  pub host:              String,
  pub port:              u16,
  pub cluster:           bool,
}

fn parse_argv() -> Argv {
  let yaml = load_yaml!("../cli.yml");
  let matches = App::from_yaml(yaml).get_matches();
  let cluster = matches.is_present("cluster");

  let count = matches
    .value_of("count")
    .map(|v| {
      v.parse::<u64>().unwrap_or_else(|_| {
        panic!("Invalid command count: {}.", v);
      })
    })
    .expect("Invalid count");
  let concurrency_range = matches
    .value_of("concurrency")
    .map(|v| {
      let parts: Vec<&str> = v.split("-").collect();
      (
        parts[0].parse::<u32>().expect("Invalid concurrency range"),
        parts[1].parse::<u32>().expect("Invalid concurrency range"),
      )
    })
    .expect("Invalid concurrency");
  let concurrency_step = matches
    .value_of("concurrency-step")
    .map(|v| v.parse::<u32>().expect("Invalid concurrency."))
    .expect("Invalid concurrency step");
  let pool_range = matches
    .value_of("pool")
    .map(|v| {
      let parts: Vec<&str> = v.split("-").collect();
      (
        parts[0].parse::<u32>().expect("Invalid pool range"),
        parts[1].parse::<u32>().expect("Invalid pool range"),
      )
    })
    .expect("Invalid pool range");
  let pool_step = matches
    .value_of("pool-step")
    .map(|v| v.parse::<u32>().expect("Invalid pool."))
    .expect("Invalid pool range");
  let host = matches
    .value_of("host")
    .map(|v| v.to_owned())
    .unwrap_or("127.0.0.1".into());
  let port = matches
    .value_of("port")
    .map(|v| v.parse::<u16>().expect("Invalid port"))
    .unwrap_or(6379);
  let pipeline = matches.subcommand_matches("pipeline").is_some();

  Argv {
    pool_range,
    pool_step,
    concurrency_range,
    concurrency_step,
    cluster,
    count,
    host,
    port,
    pipeline,
  }
}

struct Metrics {
  pub concurrency: u32,
  pub pool:        u32,
  pub throughput:  f64,
}

fn run_command(argv: &Argv, bar: &ProgressBar, concurrency: u32, pool: u32) -> Metrics {
  let mut parts = vec![
    "cargo".into(),
    "run".into(),
    "--release".into(),
    "-p".into(),
    "../pipeline_test".into(),
    "--".into(),
    "-q".into(),
    "-h".into(),
    argv.host.clone(),
    "-p".into(),
    argv.port.to_string(),
    "-c".into(),
    argv.count.to_string(),
  ];
  if argv.cluster {
    parts.push("--cluster".into());
  }
  parts.extend(vec!["-C".into(), concurrency.to_string()]);
  parts.extend(vec!["-P".into(), pool.to_string()]);
  parts.push(if argv.pipeline {
    "pipeline".into()
  } else {
    "no-pipeline".into()
  });

  let mut process = Popen::create(&["cargo", "update"], PopenConfig::default()).expect("Failed to spawn process");
  let _ = process.wait().expect("Failed to wait on subprocess.");
  let (throughput, _) = process.communicate(None).expect("Failed to read process output");
  bar.inc(1);

  let throughput = throughput
    .expect("Missing output")
    .parse::<f64>()
    .expect("Failed to parse output");
  Metrics {
    concurrency,
    pool,
    throughput,
  }
}

fn print_output(data: Vec<Metrics>) {
  unimplemented!()
}

fn main() {
  pretty_env_logger::init();
  let argv = parse_argv();
  let concurrency_runs = (argv.concurrency_range.1 - argv.concurrency_range.0) / argv.concurrency_step;
  let pool_runs = (argv.pool_range.1 - argv.pool_range.0) / argv.pool_step;
  let bar = ProgressBar::new((concurrency_runs * pool_runs) as u64);

  let mut output = Vec::with_capacity((concurrency_runs * pool_runs) as usize);

  let mut pool = argv.pool_range.0;
  while pool <= argv.pool_range.1 {
    let mut concurrency = argv.concurrency_range.0;

    while concurrency <= argv.concurrency_range.1 {
      debug!("Running with concurrency: {}, pool: {}", concurrency, pool);

      run_command(&argv, &bar, concurrency, pool);
      concurrency += argv.concurrency_step;
    }

    pool += argv.pool_step;
  }

  print_output(output);
}
