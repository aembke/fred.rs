// check for REDISCLI_AUTH
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
use std::sync::Arc;

pub struct Argv {}

fn parse_argv() {
  let yaml = load_yaml!("../cli.yml");
  let matches = App::from_yaml(yaml).get_matches();
}

fn main() {
  parse_argv();
}
