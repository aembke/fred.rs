mod pubsub;
mod redis;
mod transaction;

pub use redis::RedisClient;
pub use transaction::TransactionClient;

#[cfg(feature = "sentinel-client")]
mod sentinel;
#[cfg(feature = "sentinel-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
pub use sentinel::{SentinelClient, SentinelConfig};
