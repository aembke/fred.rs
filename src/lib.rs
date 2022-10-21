#![cfg_attr(docsrs, deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

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
pub extern crate bytes;
pub extern crate bytes_utils;
#[cfg(feature = "serde-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde-json")))]
pub extern crate serde_json;

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate log;
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

/// Redis client implementations.
pub mod clients;
/// Error structs returned by Redis commands.
pub mod error;
/// Traits that implement portions of the Redis interface.
pub mod interfaces;
/// An interface to run the `MONITOR` command.
#[cfg(feature = "monitor")]
#[cfg_attr(docsrs, doc(cfg(feature = "monitor")))]
pub mod monitor;
/// The structs and enums used by the Redis client.
pub mod types;
/// An interface for interacting directly with sentinel nodes.

/// Utility functions used by the client that may also be useful to callers.
pub mod util {
  pub use redis_protocol::redis_keyslot;

  pub use crate::{
    s,
    utils::{f64_to_redis_string, redis_string_to_f64, static_bytes, static_str},
  };

  /// Calculate the SHA1 hash output as a hex string. This is provided for clients that use the Lua interface to
  /// manage their own script caches.
  pub fn sha1_hash(input: &str) -> String {
    use sha1::Digest;

    let mut hasher = sha1::Sha1::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
  }
}

pub use crate::modules::{globals, pool};

/// Convenience module to import a `RedisClient`, all possible interfaces, error types, and common argument types or
/// return value types.
pub mod prelude {
  pub use crate::{
    clients::RedisClient,
    error::{RedisError, RedisErrorKind},
    interfaces::*,
    types::{
      Blocking, Expiration, FromRedis, ReconnectPolicy, RedisConfig, RedisValue, RedisValueKind,
      ServerConfig, SetOptions,
    },
  };
}
