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
#[cfg(feature = "transactions")]
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

#[cfg(feature = "redis-json")]
mod redis_json;

#[cfg(feature = "time-series")]
mod timeseries;

#[cfg(feature = "client-tracking")]
mod tracking;

#[cfg(not(feature = "mocks"))]
pub mod centralized;
#[cfg(not(feature = "mocks"))]
pub mod clustered;

mod macro_tests {
  use fred::{cmd, types::ClusterHash};
  use socket2::TcpKeepalive;

  #[test]
  fn should_use_cmd_macro() {
    let command = cmd!("GET");
    assert_eq!(command.cmd, "GET");
    assert_eq!(command.cluster_hash, ClusterHash::FirstKey);
    assert!(!command.blocking);
    let command = cmd!("GET", blocking: true);
    assert_eq!(command.cmd, "GET");
    assert_eq!(command.cluster_hash, ClusterHash::FirstKey);
    assert!(command.blocking);
    let command = cmd!("GET", hash: ClusterHash::FirstValue);
    assert_eq!(command.cmd, "GET");
    assert_eq!(command.cluster_hash, ClusterHash::FirstValue);
    assert!(!command.blocking);
    let command = cmd!("GET", hash: ClusterHash::FirstValue, blocking: true);
    assert_eq!(command.cmd, "GET");
    assert_eq!(command.cluster_hash, ClusterHash::FirstValue);
    assert!(command.blocking);
  }
}

mod docker_tests {
  use super::*;

  #[tokio::test]
  async fn should_read_docker_state() {
    // pretty_env_logger::try_init().unwrap();
    // FIXME need a portable way to expose the docker socket
    // let routing = docker::inspect_cluster(false).await.unwrap();
    // println!("routing {:?}", routing.slots());
    // panic!("meh");
  }
}
