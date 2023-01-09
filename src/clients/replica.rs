use crate::{
  clients::{Pipeline, RedisClient},
  error::RedisError,
  interfaces::{
    self,
    AuthInterface,
    ClientLike,
    FunctionInterface,
    GeoInterface,
    HashesInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    LuaInterface,
    MemoryInterface,
    MetricsInterface,
    ServerInterface,
    SetsInterface,
    SlowlogInterface,
    SortedSetsInterface,
    StreamsInterface,
  },
  modules::inner::RedisClientInner,
  protocol::command::{RedisCommand, RouterCommand},
  types::Server,
};
use arcstr::ArcStr;
use std::{collections::HashMap, fmt, fmt::Formatter, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// A struct for interacting with replica nodes.
///
/// All commands sent via this interface will use a replica node, if possible. The underlying connections are shared
/// with the main client in order to maintain an up-to-date view of the system in the event that replicas change or
/// are promoted. The cached replica routing table will be updated on the client when following cluster redirections
/// or when any connection closes.
///
/// Note: [Redis replication is asynchronous](https://redis.io/docs/management/replication/).
#[derive(Clone)]
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

impl GeoInterface for Replicas {}
impl HashesInterface for Replicas {}
impl HyperloglogInterface for Replicas {}
impl MetricsInterface for Replicas {}
impl KeysInterface for Replicas {}
impl LuaInterface for Replicas {}
impl FunctionInterface for Replicas {}
impl ListInterface for Replicas {}
impl MemoryInterface for Replicas {}
impl AuthInterface for Replicas {}
impl ServerInterface for Replicas {}
impl SlowlogInterface for Replicas {}
impl SetsInterface for Replicas {}
impl SortedSetsInterface for Replicas {}
impl StreamsInterface for Replicas {}

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
  /// This will also disconnect and reset any replica connections.
  pub async fn sync(&self) -> Result<(), RedisError> {
    let (tx, rx) = oneshot_channel();
    let cmd = RouterCommand::SyncReplicas { tx };
    let _ = interfaces::send_to_router(&self.inner, cmd)?;
    rx.await?
  }
}
