//! Fred
//! ====
//!
//! An async client library for [Redis](https://redis.io/) based on Tokio and Futures.
//!
//! ## Examples
//!
//! ```rust edition2018 no_run
//! use fred::prelude::*;
//! use std::future::Future;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), RedisError> {
//!   let config = RedisConfig::default();
//!   let policy = ReconnectPolicy::default();
//!   let client = RedisClient::new(config);
//!
//!   // connect to the server, returning a handle to a task that drives the connection
//!   let jh = client.connect(Some(policy));
//!   // wait for the client to connect
//!   let _ = client.wait_for_connect().await?;
//!   
//!   println!("Foo: {:?}", client.get("foo").await?);
//!   let _ = client.set("foo", "bar", None, None, false).await?;
//!   println!("Foo: {:?}", client.get("foo".to_owned()).await?);
//!
//!   let _ = client.quit().await?;
//!   // wait for the task driving the connection to finish
//!   let _ = jh.await;
//!   Ok(())
//! }
//! ```
//!
//! See the [github repository](https://github.com/aembke/fred.rs) for more examples.
//!
extern crate bytes;
extern crate float_cmp;
extern crate futures;
extern crate parking_lot;
extern crate rand;
extern crate redis_protocol;
extern crate tokio;
extern crate url;
#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

#[cfg(feature = "enable-tls")]
extern crate native_tls;
#[cfg(feature = "enable-tls")]
extern crate tokio_native_tls;

#[cfg(feature = "index-map")]
extern crate indexmap;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
extern crate tracing;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
extern crate tracing_futures;

#[macro_use]
mod macros;

mod commands;
mod inner;
mod multiplexer;
mod protocol;
mod trace;
mod utils;

/// The primary interface for communicating with the Redis server.
pub mod client;
/// Error structs returned by Redis commands.
pub mod error;
/// Utility functions for manipulating global values that can affect performance.
pub mod globals;
/// Metrics describing the latency and size of commands sent to the Redis server.
pub mod metrics;
/// Client pooling structs.
pub mod pool;
/// The structs and enums used by the Redis client.
pub mod types;

/// Convenience module to `use` a `RedisClient`, `RedisError`, and any argument types.
pub mod prelude {
  pub use crate::client::RedisClient;
  pub use crate::error::RedisError;
  pub use crate::types::*;
}
