use crate::{
  clients::RedisClient,
  error::{RedisError, RedisErrorKind},
  interfaces::ClientLike,
  types::{ConnectHandle, PerformanceConfig, ReconnectPolicy, RedisConfig},
  utils,
};
use futures::future::{join_all, try_join_all};
use std::{
  fmt,
  ops::Deref,
  sync::{atomic::AtomicUsize, Arc},
};

/// The inner state used by a `RedisPool`.
#[derive(Clone)]
pub(crate) struct RedisPoolInner {
  clients: Vec<RedisClient>,
  last:    Arc<AtomicUsize>,
}

/// A struct to pool multiple Redis clients together into one interface that will round-robin requests among clients,
/// preferring clients with an active connection if specified.
#[derive(Clone)]
pub struct RedisPool {
  inner: Arc<RedisPoolInner>,
}

impl fmt::Display for RedisPool {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Redis Pool]")
  }
}

impl Deref for RedisPool {
  type Target = RedisClient;

  fn deref(&self) -> &Self::Target {
    self.next()
  }
}

impl<'a> From<&'a RedisPool> for &'a RedisClient {
  fn from(p: &'a RedisPool) -> &'a RedisClient {
    p.next()
  }
}

impl<'a> From<&'a RedisPool> for RedisClient {
  fn from(p: &'a RedisPool) -> RedisClient {
    p.next().clone()
  }
}

impl RedisPool {
  /// Create a new pool without connecting to the server.
  // TODO rename the perf config struct and argument names
  pub fn new(config: RedisConfig, perf: PerformanceConfig, size: usize) -> Result<Self, RedisError> {
    if size > 0 {
      let mut clients = Vec::with_capacity(size);
      for _ in 0 .. size {
        clients.push(RedisClient::new(config.clone()));
      }
      let last = Arc::new(AtomicUsize::new(0));

      Ok(RedisPool {
        inner: Arc::new(RedisPoolInner { clients, last }),
      })
    } else {
      Err(RedisError::new(RedisErrorKind::Config, "Pool cannot be empty."))
    }
  }

  /// Read the individual clients in the pool.
  pub fn clients(&self) -> &[RedisClient] {
    &self.inner.clients
  }

  /// Connect each client to the server, returning the task driving each connection.
  ///
  /// The caller is responsible for calling `wait_for_connect` or any `on_*` functions on each client.
  pub fn connect(&self) -> Vec<ConnectHandle> {
    self.inner.clients.iter().map(|c| c.connect()).collect()
  }

  /// Wait for all the clients to connect to the server.
  pub async fn wait_for_connect(&self) -> Result<(), RedisError> {
    let futures = self.inner.clients.iter().map(|c| c.wait_for_connect());
    let _ = try_join_all(futures).await?;

    Ok(())
  }

  /// Read the size of the pool.
  pub fn size(&self) -> usize {
    self.inner.clients.len()
  }

  /// Read the client that should run the next command.
  #[cfg(feature = "pool-prefer-active")]
  pub fn next(&self) -> &RedisClient {
    let mut idx = utils::incr_atomic(&self.inner.last) % self.inner.clients.len();

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
  #[cfg(not(feature = "pool-prefer-active"))]
  pub fn next(&self) -> &RedisClient {
    &self.inner.clients[utils::incr_atomic(&self.inner.last) % self.inner.clients.len()]
  }

  /// Read the client that ran the last command.
  pub fn last(&self) -> &RedisClient {
    &self.inner.clients[utils::read_atomic(&self.inner.last) % self.inner.clients.len()]
  }

  /// Call `QUIT` on each client in the pool.
  pub async fn quit_pool(&self) {
    let futures = self.inner.clients.iter().map(|c| c.quit());
    let _ = join_all(futures).await;
  }
}
