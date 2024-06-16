use crate::{
  clients::RedisClient,
  error::{RedisError, RedisErrorKind},
  interfaces::*,
  modules::inner::RedisClientInner,
  types::{ConnectHandle, ConnectionConfig, PerformanceConfig, ReconnectPolicy, RedisConfig, Server},
  utils,
};
use futures::future::{join_all, try_join_all};
use std::{
  fmt,
  sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
  },
  time::Duration,
};
use tokio::time::interval as tokio_interval;

#[cfg(feature = "replicas")]
use crate::clients::Replicas;
#[cfg(feature = "dns")]
use crate::protocol::types::Resolve;

struct RedisPoolInner {
  clients:          Vec<RedisClient>,
  counter:          AtomicUsize,
  prefer_connected: AtomicBool,
}

/// A cheaply cloneable round-robin client pool.
///
/// ### Restrictions
///
/// The following interfaces are not implemented on `RedisPool`:
/// * [MetricsInterface](crate::interfaces::MetricsInterface)
/// * [PubsubInterface](crate::interfaces::PubsubInterface)
/// * [EventInterface](crate::interfaces::EventInterface)
/// * [ClientInterface](crate::interfaces::ClientInterface)
/// * [AuthInterface](crate::interfaces::AuthInterface)
///
/// In some cases, such as [publish](crate::interfaces::PubsubInterface::publish), callers can work around this by
/// adding a call to [next](Self::next), but in other scenarios this may not work. As a general rule, any commands
/// that change or depend on local connection state will not be implemented directly on `RedisPool`. Callers can use
/// [clients](Self::clients), [next](Self::next), or [last](Self::last) to operate on individual clients if needed.
#[derive(Clone)]
pub struct RedisPool {
  inner: Arc<RedisPoolInner>,
}

impl fmt::Debug for RedisPool {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    f.debug_struct("RedisPool")
      .field("size", &self.inner.clients.len())
      .field(
        "prefer_connected",
        &utils::read_bool_atomic(&self.inner.prefer_connected),
      )
      .finish()
  }
}

impl RedisPool {
  /// Create a new pool from an existing set of clients.
  pub fn from_clients(clients: Vec<RedisClient>) -> Result<Self, RedisError> {
    if clients.is_empty() {
      Err(RedisError::new(RedisErrorKind::Config, "Pool cannot be empty."))
    } else {
      Ok(RedisPool {
        inner: Arc::new(RedisPoolInner {
          clients,
          counter: AtomicUsize::new(0),
          prefer_connected: AtomicBool::new(true),
        }),
      })
    }
  }

  /// Create a new pool without connecting to the server.
  ///
  /// See the [builder](crate::types::Builder) interface for more information.
  pub fn new(
    config: RedisConfig,
    perf: Option<PerformanceConfig>,
    connection: Option<ConnectionConfig>,
    policy: Option<ReconnectPolicy>,
    size: usize,
  ) -> Result<Self, RedisError> {
    if size == 0 {
      Err(RedisError::new(RedisErrorKind::Config, "Pool cannot be empty."))
    } else {
      let mut clients = Vec::with_capacity(size);
      for _ in 0 .. size {
        clients.push(RedisClient::new(
          config.clone(),
          perf.clone(),
          connection.clone(),
          policy.clone(),
        ));
      }

      Ok(RedisPool {
        inner: Arc::new(RedisPoolInner {
          clients,
          counter: AtomicUsize::new(0),
          prefer_connected: AtomicBool::new(true),
        }),
      })
    }
  }

  /// Set whether the client will use [next_connected](Self::next_connected) or [next](Self::next) when routing
  /// commands among the pooled clients.
  pub fn prefer_connected(&self, val: bool) -> bool {
    utils::set_bool_atomic(&self.inner.prefer_connected, val)
  }

  /// Read the individual clients in the pool.
  pub fn clients(&self) -> &[RedisClient] {
    &self.inner.clients
  }

  /// Connect each client to the server, returning the task driving each connection.
  ///
  /// Use the base [connect](Self::connect) function to return one handle that drives all connections via [join](https://docs.rs/futures/latest/futures/macro.join.html).
  pub fn connect_pool(&self) -> Vec<ConnectHandle> {
    self.inner.clients.iter().map(|c| c.connect()).collect()
  }

  /// Read the size of the pool.
  pub fn size(&self) -> usize {
    self.inner.clients.len()
  }

  /// Read the next connected client that should run the next command.
  pub fn next_connected(&self) -> &RedisClient {
    let mut idx = utils::incr_atomic(&self.inner.counter) % self.inner.clients.len();

    for _ in 0 .. self.inner.clients.len() {
      let client = &self.inner.clients[idx];
      if client.is_connected() {
        return client;
      }
      idx = (idx + 1) % self.inner.clients.len();
    }

    &self.inner.clients[idx]
  }

  /// Read the client that should run the next command.
  pub fn next(&self) -> &RedisClient {
    &self.inner.clients[utils::incr_atomic(&self.inner.counter) % self.inner.clients.len()]
  }

  /// Read the client that ran the last command.
  pub fn last(&self) -> &RedisClient {
    &self.inner.clients[utils::read_atomic(&self.inner.counter) % self.inner.clients.len()]
  }

  /// Create a client that interacts with the replica nodes associated with the [next](Self::next) client.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn replicas(&self) -> Replicas {
    Replicas::from(self.inner())
  }
}

impl ClientLike for RedisPool {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    if utils::read_bool_atomic(&self.inner.prefer_connected) {
      &self.next_connected().inner
    } else {
      &self.next().inner
    }
  }

  /// Update the internal [PerformanceConfig](crate::types::PerformanceConfig) on each client in place with new
  /// values.
  fn update_perf_config(&self, config: PerformanceConfig) {
    for client in self.inner.clients.iter() {
      client.update_perf_config(config.clone());
    }
  }

  /// Read the set of active connections across all clients in the pool.
  async fn active_connections(&self) -> Result<Vec<Server>, RedisError> {
    let all_connections = try_join_all(self.inner.clients.iter().map(|c| c.active_connections())).await?;
    let total_size = if all_connections.is_empty() {
      return Ok(Vec::new());
    } else {
      all_connections.len() * all_connections[0].len()
    };
    let mut out = Vec::with_capacity(total_size);

    for connections in all_connections.into_iter() {
      out.extend(connections);
    }
    Ok(out)
  }

  /// Override the DNS resolution logic for all clients in the pool.
  #[cfg(feature = "dns")]
  #[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
  #[allow(refining_impl_trait)]
  async fn set_resolver(&self, resolver: Arc<dyn Resolve>) {
    for client in self.inner.clients.iter() {
      client.set_resolver(resolver.clone()).await;
    }
  }

  /// Connect each client to the server.
  ///
  /// This function returns a `JoinHandle` to a task that drives **all** connections via [join](https://docs.rs/futures/latest/futures/macro.join.html).
  ///
  /// See [connect_pool](crate::clients::RedisPool::connect_pool) for a variation of this function that separates the
  /// connection tasks.
  ///
  /// See [init](Self::init) for an alternative shorthand.
  fn connect(&self) -> ConnectHandle {
    let clients = self.inner.clients.clone();
    tokio::spawn(async move {
      let tasks: Vec<_> = clients.iter().map(|c| c.connect()).collect();
      for result in join_all(tasks).await.into_iter() {
        result??;
      }

      Ok(())
    })
  }

  /// Force a reconnection to the server(s) for each client.
  ///
  /// When running against a cluster this function will also refresh the cached cluster routing table.
  async fn force_reconnection(&self) -> RedisResult<()> {
    try_join_all(self.inner.clients.iter().map(|c| c.force_reconnection())).await?;

    Ok(())
  }

  /// Wait for all the clients to connect to the server.
  async fn wait_for_connect(&self) -> RedisResult<()> {
    try_join_all(self.inner.clients.iter().map(|c| c.wait_for_connect())).await?;

    Ok(())
  }

  /// Initialize a new routing and connection task for each client and wait for them to connect successfully.
  ///
  /// The returned [ConnectHandle](crate::types::ConnectHandle) refers to the task that drives the routing and
  /// connection layer for each client via [join](https://docs.rs/futures/latest/futures/macro.join.html). It will not finish until the max reconnection count is reached.
  ///
  /// Callers can also use [connect](Self::connect) and [wait_for_connect](Self::wait_for_connect) separately if
  /// needed.
  ///
  /// ```rust
  /// use fred::prelude::*;
  ///
  /// #[tokio::main]
  /// async fn main() -> Result<(), RedisError> {
  ///   let pool = Builder::default_centralized().build_pool(5)?;
  ///   let connection_task = pool.init().await?;
  ///
  ///   // ...
  ///
  ///   pool.quit().await?;
  ///   connection_task.await?
  /// }
  /// ```
  async fn init(&self) -> RedisResult<ConnectHandle> {
    let rxs: Vec<_> = self.inner.clients.iter().map(|c| c.wait_for_connect()).collect();

    let connect_task = self.connect();
    let init_err = futures::future::join_all(rxs).await.into_iter().find_map(|r| r.err());

    if let Some(err) = init_err {
      for client in self.inner.clients.iter() {
        utils::reset_router_task(client.inner());
      }

      Err(err)
    } else {
      Ok(connect_task)
    }
  }

  /// Close the connection to the Redis server for each client. The returned future resolves when the command has been
  /// written to the socket, not when the connection has been fully closed. Some time after this future resolves the
  /// future returned by [connect](Self::connect) will resolve which indicates that the connection has been fully
  /// closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams on all clients in the
  /// pool.
  async fn quit(&self) -> RedisResult<()> {
    join_all(self.inner.clients.iter().map(|c| c.quit())).await;

    Ok(())
  }
}

impl HeartbeatInterface for RedisPool {
  async fn enable_heartbeat(&self, interval: Duration, break_on_error: bool) -> RedisResult<()> {
    let mut interval = tokio_interval(interval);

    loop {
      interval.tick().await;

      if let Err(error) = try_join_all(self.inner.clients.iter().map(|c| c.ping::<()>())).await {
        if break_on_error {
          return Err(error);
        }
      }
    }
  }
}

#[cfg(feature = "i-acl")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-acl")))]
impl AclInterface for RedisPool {}
#[cfg(feature = "i-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-client")))]
impl ClientInterface for RedisPool {}
#[cfg(feature = "i-cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-cluster")))]
impl ClusterInterface for RedisPool {}
#[cfg(feature = "i-config")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-config")))]
impl ConfigInterface for RedisPool {}
#[cfg(feature = "i-geo")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-geo")))]
impl GeoInterface for RedisPool {}
#[cfg(feature = "i-hashes")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hashes")))]
impl HashesInterface for RedisPool {}
#[cfg(feature = "i-hyperloglog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hyperloglog")))]
impl HyperloglogInterface for RedisPool {}
#[cfg(feature = "transactions")]
#[cfg_attr(docsrs, doc(cfg(feature = "transactions")))]
impl TransactionInterface for RedisPool {}
#[cfg(feature = "i-keys")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-keys")))]
impl KeysInterface for RedisPool {}
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
impl LuaInterface for RedisPool {}
#[cfg(feature = "i-lists")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-lists")))]
impl ListInterface for RedisPool {}
#[cfg(feature = "i-memory")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-memory")))]
impl MemoryInterface for RedisPool {}
#[cfg(feature = "i-server")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-server")))]
impl ServerInterface for RedisPool {}
#[cfg(feature = "i-slowlog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-slowlog")))]
impl SlowlogInterface for RedisPool {}
#[cfg(feature = "i-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sets")))]
impl SetsInterface for RedisPool {}
#[cfg(feature = "i-sorted-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sorted-sets")))]
impl SortedSetsInterface for RedisPool {}
#[cfg(feature = "i-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-streams")))]
impl StreamsInterface for RedisPool {}
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
impl FunctionInterface for RedisPool {}
#[cfg(feature = "i-redis-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redis-json")))]
impl RedisJsonInterface for RedisPool {}
#[cfg(feature = "i-time-series")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-time-series")))]
impl TimeSeriesInterface for RedisPool {}
#[cfg(feature = "i-redisearch")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redisearch")))]
impl RediSearchInterface for RedisPool {}
