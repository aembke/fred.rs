use crate::{
  clients::{Pipeline, RedisClient},
  interfaces::{
    AclInterface,
    AuthInterface,
    ClientInterface,
    ClientLike,
    ClusterInterface,
    ConfigInterface,
    FunctionInterface,
    GeoInterface,
    HashesInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    LuaInterface,
    MemoryInterface,
    ServerInterface,
    SetsInterface,
    SlowlogInterface,
    SortedSetsInterface,
    StreamsInterface,
  },
  modules::inner::RedisClientInner,
  protocol::command::RedisCommand,
  types::Server,
};
use std::sync::Arc;

/// A struct for interacting with individual nodes in a cluster.
///
/// See [with_cluster_node](crate::clients::RedisClient::with_cluster_node) for more information.
#[derive(Clone)]
pub struct Node {
  inner:  Arc<RedisClientInner>,
  server: Server,
}

impl ClientLike for Node {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }

  #[doc(hidden)]
  fn change_command(&self, cmd: &mut RedisCommand) {
    cmd.cluster_node = Some(self.server.clone());
  }
}

impl Node {
  pub(crate) fn new(inner: &Arc<RedisClientInner>, server: Server) -> Node {
    Node {
      inner: inner.clone(),
      server,
    }
  }

  /// Read the server to which all commands will be sent.
  pub fn server(&self) -> &Server {
    &self.server
  }

  /// Create a client instance that can interact with all cluster nodes.
  pub fn client(&self) -> RedisClient {
    self.inner().into()
  }

  /// Send a series of commands in a pipeline to the cluster node.
  pub fn pipeline(&self) -> Pipeline<Node> {
    Pipeline::from(self.clone())
  }
}

impl AclInterface for Node {}
impl ClientInterface for Node {}
impl ClusterInterface for Node {}
impl ConfigInterface for Node {}
impl GeoInterface for Node {}
impl HashesInterface for Node {}
impl HyperloglogInterface for Node {}
impl KeysInterface for Node {}
impl LuaInterface for Node {}
impl ListInterface for Node {}
impl MemoryInterface for Node {}
impl AuthInterface for Node {}
impl ServerInterface for Node {}
impl SlowlogInterface for Node {}
impl SetsInterface for Node {}
impl SortedSetsInterface for Node {}
impl StreamsInterface for Node {}
impl FunctionInterface for Node {}
