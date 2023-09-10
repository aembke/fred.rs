#[macro_use]
pub mod utils;
pub mod docker;

mod acl;
mod client;
mod cluster;
mod geo;
mod hashes;
mod hyperloglog;
mod keys;
mod lists;
mod lua;
mod memory;
mod multi;
mod other;
mod pool;
mod pubsub;
mod scanning;
mod server;
mod sets;
mod slowlog;
mod sorted_sets;
mod streams;

#[cfg(feature = "client-tracking")]
mod tracking;

#[cfg(not(feature = "mocks"))]
pub mod centralized;
#[cfg(not(feature = "mocks"))]
pub mod clustered;

mod macro_tests {
  use fred::{b, s};

  #[test]
  fn should_use_static_str_macro() {
    let _s = s!("foo");
  }

  #[test]
  fn should_use_static_bytes_macro() {
    let _b = b!(b"foo");
  }
}

mod docker_tests {
  use super::*;

  #[tokio::test]
  async fn should_read_docker_state() {
    pretty_env_logger::try_init().unwrap();

    // FIXME need a portable way to expose the docker socket
    // let routing = docker::inspect_cluster(false).await.unwrap();
    // println!("routing {:?}", routing.slots());
    // panic!("meh");
  }
}
