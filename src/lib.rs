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
//!   let _ = client.flushall(false).await?;
//!
//!   // convert responses to many common Rust types
//!   let foo: Option<String> = client.get("foo").await?;
//!   assert_eq!(foo, None);
//!
//!   let _: () = client.set("foo", "bar", None, None, false).await?;
//!   // or use turbofish to declare types. the first type is always the response.
//!   println!("Foo: {:?}", client.get::<String, _>("foo".to_owned()).await?);
//!   // or use a lower level interface for responses to defer parsing, etc
//!   let foo: RedisValue = client.get("foo").await?;
//!   assert!(foo.is_string());
//!
//!   let _ = client.quit().await?;
//!   // and/or wait for the task driving the connection to finish
//!   let _ = jh.await;
//!   Ok(())
//! }
//! ```
//!
//! See the [github repository](https://github.com/aembke/fred.rs) for more examples.
//!
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;
#[cfg(feature = "index-map")]
extern crate indexmap;
#[cfg(feature = "enable-tls")]
extern crate native_tls;
#[cfg(feature = "enable-tls")]
extern crate tokio_native_tls;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
extern crate tracing;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
extern crate tracing_futures;

#[macro_use]
mod macros;

mod commands;
mod modules;
mod multiplexer;
mod protocol;
mod trace;
mod utils;

/// The primary interface for communicating with the Redis server.
pub mod client;
/// Error structs returned by Redis commands.
pub mod error;
/// An interface to run the `MONITOR` command.
#[cfg(feature = "monitor")]
pub mod monitor;

// TODO test cargo doc works
pub use crate::modules::{globals, pool, types};

/// Convenience module to `use` a `RedisClient`, `RedisError`, and any argument types.
pub mod prelude {
  pub use crate::client::RedisClient;
  pub use crate::error::RedisError;
  pub use crate::types::*;
}
