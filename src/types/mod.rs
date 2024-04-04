use crate::error::RedisError;
pub use crate::modules::response::{FromRedis, FromRedisKey};
pub use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, RespVersion};
use tokio::task::JoinHandle;

mod args;
mod builder;
#[cfg(feature = "i-client")]
mod client;
#[cfg(feature = "i-cluster")]
mod cluster;
mod config;
mod from_tuple;
#[cfg(feature = "i-geo")]
mod geo;
#[cfg(feature = "i-lists")]
mod lists;
mod misc;
mod multiple;
mod scan;
#[cfg(feature = "i-scripts")]
mod scripts;
#[cfg(feature = "i-sorted-sets")]
mod sorted_sets;
#[cfg(feature = "i-streams")]
mod streams;
#[cfg(feature = "i-time-series")]
mod timeseries;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use crate::modules::metrics::Stats;
pub use args::*;
pub use builder::*;
#[cfg(feature = "i-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-client")))]
pub use client::*;
#[cfg(feature = "i-cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-cluster")))]
pub use cluster::*;
pub use config::*;
#[cfg(feature = "i-geo")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-geo")))]
pub use geo::*;
#[cfg(feature = "i-lists")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-lists")))]
pub use lists::*;
pub use misc::*;
pub use multiple::*;
pub use scan::*;
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
pub use scripts::*;
pub use semver::Version;
#[cfg(feature = "i-sorted-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sorted-sets")))]
pub use sorted_sets::*;
#[cfg(feature = "i-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-streams")))]
pub use streams::*;
#[cfg(feature = "i-time-series")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-time-series")))]
pub use timeseries::*;

#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub use crate::protocol::types::Resolve;

pub(crate) static QUEUED: &str = "QUEUED";

/// The ANY flag used on certain GEO commands.
pub type Any = bool;
/// The result from any of the `connect` functions showing the error that closed the connection, if any.
pub type ConnectHandle = JoinHandle<Result<(), RedisError>>;
/// A tuple of `(offset, count)` values for commands that allow paging through results.
pub type Limit = (i64, i64);
/// An argument type equivalent to "[LIMIT count]".
pub type LimitCount = Option<i64>;
