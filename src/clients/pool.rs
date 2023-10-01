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
  sync::{atomic::AtomicUsize, Arc},
  time::Duration,
};
use tokio::time::interval as tokio_interval;

#[cfg(feature = "dns")]
use crate::protocol::types::Resolve;

#[cfg(feature = "replicas")]
use crate::clients::Replicas;

/// A cheaply cloneable round-robin client pool.
///
/// ### Restrictions
///
/// The following interfaces are not implemented on `RedisPool`:
/// * [MetricsInterface](crate::interfaces::MetricsInterface)
/// * [PubsubInterface](crate::interfaces::PubsubInterface)
/// * [TransactionInterface](crate::interfaces::TransactionInterface)
/// * [EventInterface](crate::interfaces::EventInterface)
///
/// In some cases, such as [publish](crate::interfaces::PubsubInterface::publish), callers can work around this by
/// adding a call to [next](Self::next), but in other scenarios this may not work. As a general rule, any commands
/// that change or depend on local connection state will not be implemented directly on `RedisPool`. Callers can use
/// [clients](Self::clients), [next](Self::next), or [last](Self::last) to operate on individual clients if needed.
#[derive(Clone)]
pub struct RedisPool {
  clients: Arc<Vec<RedisClient>>,
  counter: Arc<AtomicUsize>,
}

impl fmt::Debug for RedisPool {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    f.debug_struct("RedisPool").field("size", &self.clients.len()).finish()
  }
}

impl RedisPool {
  /// Create a new pool from an existing set of clients.
  pub fn from_clients(clients: Vec<RedisClient>) -> Result<Self, RedisError> {
    if clients.is_empty() {
      Err(RedisError::new(RedisErrorKind::Config, "Pool cannot be empty."))
    } else {
      Ok(RedisPool {
        clients: Arc::new(clients),
        counter: Arc::new(AtomicUsize::new(0)),
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
        clients: Arc::new(clients),
        counter: Arc::new(AtomicUsize::new(0)),
      })
    }
  }

  /// Read the individual clients in the pool.
  pub fn clients(&self) -> &[RedisClient] {
    &self.clients
  }

  /// Connect each client to the server, returning the task driving each connection.
  ///
  /// Use the base [connect](Self::connect) function to return one handle that drives all connections via [join](https://docs.rs/futures/latest/futures/macro.join.html).
  pub fn connect_pool(&self) -> Vec<ConnectHandle> {
    self.clients.iter().map(|c| c.connect()).collect()
  }

  /// Read the size of the pool.
  pub fn size(&self) -> usize {
    self.clients.len()
  }

  /// Read the client that should run the next command.
  #[cfg(feature = "pool-prefer-active")]
  pub fn next(&self) -> &RedisClient {
    let mut idx = utils::incr_atomic(&self.counter) % self.clients.len();

    for _ in 0 .. self.clients.len() {
      let client = &self.clients[idx];
      if client.is_connected() {
        return client;
      }
      idx = (idx + 1) % self.clients.len();
    }

    &self.clients[idx]
  }

  /// Read the client that should run the next command.
  #[cfg(not(feature = "pool-prefer-active"))]
  pub fn next(&self) -> &RedisClient {
    &self.clients[utils::incr_atomic(&self.counter) % self.clients.len()]
  }

  /// Read the client that ran the last command.
  pub fn last(&self) -> &RedisClient {
    &self.clients[utils::read_atomic(&self.counter) % self.clients.len()]
  }

  /// Create a client that interacts with the replica nodes associated with the [next](Self::next) client.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn replicas(&self) -> Replicas {
    Replicas::from(self.inner())
  }
}

#[async_trait]
impl ClientLike for RedisPool {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.next().inner
  }

  /// Update the internal [PerformanceConfig](crate::types::PerformanceConfig) on each client in place with new
  /// values.
  fn update_perf_config(&self, config: PerformanceConfig) {
    for client in self.clients.iter() {
      client.update_perf_config(config.clone());
    }
  }

  /// Read the set of active connections across all clients in the pool.
  async fn active_connections(&self) -> Result<Vec<Server>, RedisError> {
    let all_connections = try_join_all(self.clients.iter().map(|c| c.active_connections())).await?;
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
  async fn set_resolver(&self, resolver: Arc<dyn Resolve>) {
    for client in self.clients.iter() {
      client.set_resolver(resolver.clone()).await;
    }
  }

  /// Connect each client to the Redis server.
  ///
  /// This function returns a `JoinHandle` to a task that drives **all** connections via [join](https://docs.rs/futures/latest/futures/macro.join.html).
  ///
  /// See [connect_pool](crate::clients::RedisPool::connect_pool) for a variation of this function that separates the
  /// connection tasks.
  fn connect(&self) -> ConnectHandle {
    let clients = self.clients.clone();
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
    let _ = try_join_all(self.clients.iter().map(|c| c.force_reconnection())).await?;

    Ok(())
  }

  /// Wait for all the clients to connect to the server.
  async fn wait_for_connect(&self) -> RedisResult<()> {
    let _ = try_join_all(self.clients.iter().map(|c| c.wait_for_connect())).await?;

    Ok(())
  }

  /// Close the connection to the Redis server for each client. The returned future resolves when the command has been
  /// written to the socket, not when the connection has been fully closed. Some time after this future resolves the
  /// future returned by [connect](Self::connect) will resolve which indicates that the connection has been fully
  /// closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams on all clients in the
  /// pool.
  async fn quit(&self) -> RedisResult<()> {
    let _ = join_all(self.clients.iter().map(|c| c.quit())).await;

    Ok(())
  }
}

#[async_trait]
impl HeartbeatInterface for RedisPool {
  async fn enable_heartbeat(&self, interval: Duration, break_on_error: bool) -> RedisResult<()> {
    let mut interval = tokio_interval(interval);

    loop {
      interval.tick().await;

      if let Err(error) = try_join_all(self.clients.iter().map(|c| c.ping::<()>())).await {
        if break_on_error {
          return Err(error);
        }
      }
    }
  }
}

impl AclInterface for RedisPool {}
impl ClientInterface for RedisPool {}
impl ClusterInterface for RedisPool {}
impl ConfigInterface for RedisPool {}
impl GeoInterface for RedisPool {}
impl HashesInterface for RedisPool {}
impl HyperloglogInterface for RedisPool {}
impl KeysInterface for RedisPool {}
impl LuaInterface for RedisPool {}
impl ListInterface for RedisPool {}
impl MemoryInterface for RedisPool {}
impl AuthInterface for RedisPool {}
impl ServerInterface for RedisPool {}
impl SlowlogInterface for RedisPool {}
impl SetsInterface for RedisPool {}
impl SortedSetsInterface for RedisPool {}
impl StreamsInterface for RedisPool {}
impl FunctionInterface for RedisPool {}
#[cfg(feature = "redis-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-json")))]
impl RedisJsonInterface for RedisPool {}
