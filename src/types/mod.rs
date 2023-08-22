use crate::error::RedisError;
pub use crate::modules::response::{FromRedis, FromRedisKey};
pub use arcstr::ArcStr;
pub use redis_protocol::resp3::types::{Frame, RespVersion};
use tokio::task::JoinHandle;

mod acl;
mod args;
mod builder;
mod client;
mod cluster;
mod config;
mod geo;
mod lists;
mod misc;
mod multiple;
mod scan;
mod scripts;
mod sorted_sets;
mod streams;

pub use acl::*;
pub use args::*;
pub use builder::*;
pub use client::*;
pub use cluster::*;
pub use config::*;
pub use geo::*;
pub use lists::*;
pub use misc::*;
pub use multiple::*;
pub use scan::*;
pub use scripts::*;
pub use semver::Version;
pub use sorted_sets::*;
pub use streams::*;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use crate::modules::metrics::Stats;

#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub use crate::protocol::types::Resolve;

pub(crate) static QUEUED: &'static str = "QUEUED";
pub(crate) static NIL: &'static str = "nil";

/// The ANY flag used on certain GEO commands.
pub type Any = bool;
/// The result from any of the `connect` functions showing the error that closed the connection, if any.
pub type ConnectHandle = JoinHandle<Result<(), RedisError>>;
/// A tuple of `(offset, count)` values for commands that allow paging through results.
pub type Limit = (i64, i64);
/// An argument type equivalent to "[LIMIT count]".
pub type LimitCount = Option<i64>;
