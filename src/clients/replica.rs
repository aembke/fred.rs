use crate::{
  clients::{Pipeline, RedisClient},
  error::RedisError,
  interfaces::{self, *},
  modules::inner::RedisClientInner,
  protocol::command::{RedisCommand, RouterCommand},
  types::Server,
};
use std::{collections::HashMap, fmt, fmt::Formatter, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// A struct for interacting with cluster replica nodes.
///
/// All commands sent via this interface will use a replica node, if possible. The underlying connections are shared
/// with the main client in order to maintain an up-to-date view of the system in the event that replicas change or
/// are promoted. The cached replica routing table will be updated on the client when following cluster redirections
/// or when any connection closes.
///
/// [Redis replication is asynchronous](https://redis.io/docs/management/replication/).
#[derive(Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
pub struct Replicas {
  inner: Arc<RedisClientInner>,
}

impl fmt::Debug for Replicas {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("Replicas").field("id", &self.inner.id).finish()
  }
}

#[doc(hidden)]
impl From<&Arc<RedisClientInner>> for Replicas {
  fn from(inner: &Arc<RedisClientInner>) -> Self {
    Replicas { inner: inner.clone() }
  }
}

impl ClientLike for Replicas {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }

  #[doc(hidden)]
  fn change_command(&self, command: &mut RedisCommand) {
    command.use_replica = true;
  }
}

#[cfg(feature = "i-redis-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redis-json")))]
impl RedisJsonInterface for Replicas {}
#[cfg(feature = "i-time-series")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-time-series")))]
impl TimeSeriesInterface for Replicas {}
#[cfg(feature = "i-cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-cluster")))]
impl ClusterInterface for Replicas {}
#[cfg(feature = "i-config")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-config")))]
impl ConfigInterface for Replicas {}
#[cfg(feature = "i-geo")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-geo")))]
impl GeoInterface for Replicas {}
#[cfg(feature = "i-hashes")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hashes")))]
impl HashesInterface for Replicas {}
#[cfg(feature = "i-hyperloglog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hyperloglog")))]
impl HyperloglogInterface for Replicas {}
#[cfg(feature = "i-keys")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-keys")))]
impl KeysInterface for Replicas {}
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
impl LuaInterface for Replicas {}
#[cfg(feature = "i-lists")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-lists")))]
impl ListInterface for Replicas {}
#[cfg(feature = "i-memory")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-memory")))]
impl MemoryInterface for Replicas {}
#[cfg(feature = "i-server")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-server")))]
impl ServerInterface for Replicas {}
#[cfg(feature = "i-slowlog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-slowlog")))]
impl SlowlogInterface for Replicas {}
#[cfg(feature = "i-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sets")))]
impl SetsInterface for Replicas {}
#[cfg(feature = "i-sorted-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sorted-sets")))]
impl SortedSetsInterface for Replicas {}
#[cfg(feature = "i-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-streams")))]
impl StreamsInterface for Replicas {}
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
impl FunctionInterface for Replicas {}

impl Replicas {
  /// Read a mapping of replica server IDs to primary server IDs.
  pub fn nodes(&self) -> HashMap<Server, Server> {
    self.inner.server_state.read().replicas.clone()
  }

  /// Send a series of commands in a [pipeline](https://redis.io/docs/manual/pipelining/).
  pub fn pipeline(&self) -> Pipeline<Replicas> {
    Pipeline::from(self.clone())
  }

  /// Read the underlying [RedisClient](crate::clients::RedisClient) that interacts with primary nodes.
  pub fn client(&self) -> RedisClient {
    RedisClient::from(&self.inner)
  }

  /// Sync the cached replica routing table with the server(s).
  ///
  /// If `reset: true` the client will forcefully disconnect from replicas even if the connections could otherwise be reused.
  pub async fn sync(&self, reset: bool) -> Result<(), RedisError> {
    let (tx, rx) = oneshot_channel();
    let cmd = RouterCommand::SyncReplicas { tx, reset };
    interfaces::send_to_router(&self.inner, cmd)?;
    rx.await?
  }
}
