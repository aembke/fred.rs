#[macro_use]
pub mod utils;

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

pub mod centralized;
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
