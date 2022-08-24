mod pipeline;
mod redis;
mod transaction;
pub use pipeline::Pipeline;
pub use redis::RedisClient;
pub use transaction::Transaction;

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

#[cfg(feature = "replicas")]
mod replica;
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
pub use replica::ReplicaClient;
