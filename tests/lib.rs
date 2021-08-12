#[macro_use]
extern crate log;
extern crate pretty_env_logger;

// this is a poor way of dealing with global mutable state
#[test]
fn init_test_logger() {
  pretty_env_logger::init();
}

mod integration;
