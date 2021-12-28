use crate::error::RedisError;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

mod acl;
mod args;
mod client;
mod cluster;
mod config;
mod geo;
mod lists;
mod misc;
mod multiple;
mod scan;
mod sorted_sets;
mod streams;

pub use acl::*;
pub use args::*;
pub use client::*;
pub use cluster::*;
pub use config::*;
pub use geo::*;
pub use lists::*;
pub use misc::*;
pub use multiple::*;
pub use scan::*;
pub use sorted_sets::*;
pub use streams::*;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use crate::modules::metrics::Stats;

pub(crate) static QUEUED: &'static str = "QUEUED";
pub(crate) static NIL: &'static str = "nil";

pub use crate::modules::response::FromRedis;
pub use crate::protocol::types::ClusterKeyCache;
pub use redis_protocol::resp3::types::{Frame, RespVersion};

/// The ANY flag used on certain GEO commands.
pub type Any = bool;
/// The result from any of the `connect` functions showing the error that closed the connection, if any.
pub type ConnectHandle = JoinHandle<Result<(), RedisError>>;
/// A tuple of `(offset, count)` values for commands that allow paging through results.
pub type Limit = (i64, i64);
/// An argument type equivalent to "[LIMIT count]".
pub type LimitCount = Option<i64>;

/// A trait that can be used to override DNS resolution logic for a client.
///
/// Note: using this requires [async-trait](https://crates.io/crates/async-trait).
// TODO expose this to callers so they can do their own DNS resolution
#[async_trait]
pub(crate) trait Resolve: Send + Sync + 'static {
  /// Resolve a hostname.
  async fn resolve(&self, host: String, port: u16) -> Result<SocketAddr, RedisError>;
}
