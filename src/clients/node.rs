use crate::{
  clients::{Pipeline, RedisClient},
  commands,
  error::RedisError,
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
  types::{ScanResult, ScanType, Server},
};
use bytes_utils::Str;
use futures::Stream;
use std::sync::Arc;

use crate::interfaces::PubsubInterface;
#[cfg(feature = "client-tracking")]
use crate::{
  interfaces::RedisResult,
  types::{FromRedis, MultipleStrings, Toggle},
};

/// A struct for interacting with individual nodes in a cluster.
///
/// See [with_cluster_node](crate::clients::RedisClient::with_cluster_node) for more information.
///
/// ```
/// # use fred::prelude::*;
/// async fn example(client: &RedisClient) -> Result<(), RedisError> {
///   // discover servers via the `RedisConfig` or active connections
///   let connections = client.active_connections().await?;
///
///   // ping each node in the cluster individually
///   for server in connections.into_iter() {
///     let _: () = client.with_cluster_node(server).ping().await?;
///   }
///
///   // or use the cached cluster routing table to discover servers
///   let servers = client
///     .cached_cluster_state()
///     .expect("Failed to read cached cluster state")
///     .unique_primary_nodes();
///   for server in servers {
///     // verify the server address with `CLIENT INFO`
///     let server_addr = client
///       .with_cluster_node(&server)
///       .client_info::<String>()
///       .await?
///       .split(" ")
///       .find_map(|s| {
///         let parts: Vec<&str> = s.split("=").collect();
///         if parts[0] == "laddr" {
///           Some(parts[1].to_owned())
///         } else {
///           None
///         }
///       })
///       .expect("Failed to read or parse client info.");
///
///     assert_eq!(server_addr, server.to_string());
///   }
///
///   Ok(())
/// }
/// ```
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

  /// Incrementally iterate over a set of keys matching the `pattern` argument, returning `count` results per page, if
  /// specified.
  ///
  /// The scan operation can be canceled by dropping the returned stream.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<ScanResult, RedisError>>
  where
    P: Into<Str>,
  {
    commands::scan::scan(&self.inner, pattern.into(), count, r#type, Some(self.server.clone()))
  }
}

impl AclInterface for Node {}
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
impl PubsubInterface for Node {}

// remove the restriction on clustered deployments with the basic `CLIENT TRACKING` commands here
#[async_trait]
impl ClientInterface for Node {
  /// This command enables the tracking feature of the Redis server that is used for server assisted client side
  /// caching.
  ///
  /// <https://redis.io/commands/client-tracking/>
  #[cfg(feature = "client-tracking")]
  #[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
  async fn client_tracking<R, T, P>(
    &self,
    toggle: T,
    redirect: Option<i64>,
    prefixes: P,
    bcast: bool,
    optin: bool,
    optout: bool,
    noloop: bool,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    T: TryInto<Toggle> + Send,
    T::Error: Into<RedisError> + Send,
    P: Into<MultipleStrings> + Send,
  {
    try_into!(toggle);
    into!(prefixes);
    commands::tracking::client_tracking(self, toggle, redirect, prefixes, bcast, optin, optout, noloop)
      .await?
      .convert()
  }

  /// This command controls the tracking of the keys in the next command executed by the connection, when tracking is
  /// enabled in OPTIN or OPTOUT mode.
  ///
  /// <https://redis.io/commands/client-caching/>
  ///
  /// Note: **This function requires a centralized server**. See
  /// [crate::interfaces::TrackingInterface::caching] for a version that works with all server deployment
  /// modes.
  #[cfg(feature = "client-tracking")]
  #[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
  async fn client_caching<R>(&self, enabled: bool) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::tracking::client_caching(self, enabled).await?.convert()
  }
}
