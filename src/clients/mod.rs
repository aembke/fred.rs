mod options;
mod pipeline;
mod pool;
mod redis;

pub use options::WithOptions;
pub use pipeline::Pipeline;
pub use pool::RedisPool;
pub use redis::RedisClient;

#[cfg(not(feature = "glommio"))]
pub use pool::ExclusivePool;

#[cfg(feature = "sentinel-client")]
mod sentinel;
#[cfg(feature = "sentinel-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
pub use sentinel::SentinelClient;

#[cfg(feature = "subscriber-client")]
mod pubsub;
#[cfg(feature = "subscriber-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "subscriber-client")))]
pub use pubsub::SubscriberClient;

#[cfg(feature = "replicas")]
mod replica;
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
pub use replica::Replicas;

#[cfg(feature = "transactions")]
mod transaction;
#[cfg(feature = "transactions")]
#[cfg_attr(docsrs, doc(cfg(feature = "transactions")))]
pub use transaction::Transaction;
