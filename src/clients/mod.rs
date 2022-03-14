mod redis;
mod transaction;
pub use redis::RedisClient;
pub use transaction::TransactionClient;

#[cfg(feature = "sentinel-client")]
mod sentinel;
#[cfg(feature = "sentinel-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
pub use sentinel::{SentinelClient, SentinelConfig};

#[cfg(feature = "subscriber-client")]
mod pubsub;
#[cfg(feature = "subscriber-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "subscriber-client")))]
pub use pubsub::SubscriberClient;
