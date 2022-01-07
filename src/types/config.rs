use crate::types::RespVersion;
use crate::utils;
use std::cmp;

#[cfg(feature = "enable-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-tls")))]
pub use crate::protocol::tls::TlsConfig;

/// The default amount of jitter when waiting to reconnect.
pub const DEFAULT_JITTER_MS: u32 = 100;

/// The type of reconnection policy to use. This will apply to every connection used by the client.
///
/// Use a `max_attempts` value of `0` to retry forever.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconnectPolicy {
  /// Wait a constant amount of time between reconnect attempts, in ms.
  Constant {
    attempts: u32,
    max_attempts: u32,
    delay: u32,
    jitter: u32,
  },
  /// Backoff reconnection attempts linearly, adding `delay` each time.
  Linear {
    attempts: u32,
    max_attempts: u32,
    max_delay: u32,
    delay: u32,
    jitter: u32,
  },
  /// Backoff reconnection attempts exponentially, multiplying the last delay by `mult` each time.
  Exponential {
    attempts: u32,
    max_attempts: u32,
    min_delay: u32,
    max_delay: u32,
    mult: u32,
    jitter: u32,
  },
}

impl Default for ReconnectPolicy {
  fn default() -> Self {
    ReconnectPolicy::Constant {
      attempts: 0,
      max_attempts: 0,
      delay: 1000,
      jitter: DEFAULT_JITTER_MS,
    }
  }
}

impl ReconnectPolicy {
  /// Create a new reconnect policy with a constant backoff.
  pub fn new_constant(max_attempts: u32, delay: u32) -> ReconnectPolicy {
    ReconnectPolicy::Constant {
      max_attempts,
      delay,
      attempts: 0,
      jitter: DEFAULT_JITTER_MS,
    }
  }

  /// Create a new reconnect policy with a linear backoff.
  pub fn new_linear(max_attempts: u32, max_delay: u32, delay: u32) -> ReconnectPolicy {
    ReconnectPolicy::Linear {
      max_attempts,
      max_delay,
      delay,
      attempts: 0,
      jitter: DEFAULT_JITTER_MS,
    }
  }

  /// Create a new reconnect policy with an exponential backoff.
  pub fn new_exponential(max_attempts: u32, min_delay: u32, max_delay: u32, mult: u32) -> ReconnectPolicy {
    ReconnectPolicy::Exponential {
      max_delay,
      max_attempts,
      min_delay,
      mult,
      attempts: 0,
      jitter: DEFAULT_JITTER_MS,
    }
  }

  /// Reset the number of reconnection attempts. It's unlikely users will need to call this.
  pub fn reset_attempts(&mut self) {
    match *self {
      ReconnectPolicy::Constant { ref mut attempts, .. } => {
        *attempts = 0;
      }
      ReconnectPolicy::Linear { ref mut attempts, .. } => {
        *attempts = 0;
      }
      ReconnectPolicy::Exponential { ref mut attempts, .. } => {
        *attempts = 0;
      }
    }
  }

  /// Read the number of reconnection attempts.
  pub fn attempts(&self) -> u32 {
    match *self {
      ReconnectPolicy::Constant { ref attempts, .. } => *attempts,
      ReconnectPolicy::Linear { ref attempts, .. } => *attempts,
      ReconnectPolicy::Exponential { ref attempts, .. } => *attempts,
    }
  }

  /// Calculate the next delay, incrementing `attempts` in the process.
  pub fn next_delay(&mut self) -> Option<u64> {
    match *self {
      ReconnectPolicy::Constant {
        ref mut attempts,
        delay,
        max_attempts,
        jitter,
      } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None,
        };

        Some(utils::add_jitter(delay as u64, jitter))
      }
      ReconnectPolicy::Linear {
        ref mut attempts,
        max_delay,
        max_attempts,
        delay,
        jitter,
      } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None,
        };
        let delay = (delay as u64).saturating_mul(*attempts as u64);

        Some(cmp::min(max_delay as u64, utils::add_jitter(delay, jitter)))
      }
      ReconnectPolicy::Exponential {
        ref mut attempts,
        min_delay,
        max_delay,
        max_attempts,
        mult,
        jitter,
      } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None,
        };
        let delay = (mult as u64).pow(*attempts - 1).saturating_mul(min_delay as u64);

        Some(cmp::min(max_delay as u64, utils::add_jitter(delay, jitter)))
      }
    }
  }
}

/// Describes how the client should respond when a command is sent while the client is in a blocked state from a blocking command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Blocking {
  /// Wait to send the command until the blocked command finishes. (Default)
  Block,
  /// Return an error to the caller.
  Error,
  /// Interrupt the blocked command by automatically sending `CLIENT UNBLOCK` for the blocked connection.
  Interrupt,
}

impl Default for Blocking {
  fn default() -> Self {
    Blocking::Block
  }
}

/// Configuration options for backpressure features in the client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpressureConfig {
  /// Whether or not to disable the automatic backpressure features when pipelining is enabled.
  ///
  /// If `true` then `RedisErrorKind::Backpressure` errors may be surfaced to callers.
  ///
  /// Default: `false`
  pub disable_auto_backpressure: bool,
  /// Disable the backpressure scaling logic used to calculate the `sleep` duration when throttling commands.
  ///
  /// If `true` then the client will always wait a constant amount of time defined by `min_sleep_duration_ms` when throttling commands.
  ///
  /// Default: `false`
  pub disable_backpressure_scaling: bool,
  /// The minimum amount of time to wait when applying backpressure to a command.
  ///
  /// If `0` then no backpressure will be applied, but backpressure errors will not be surfaced to callers unless `disable_auto_backpressure` is `true`.
  ///
  /// Default: 100 ms
  pub min_sleep_duration_ms: u64,
  /// The maximum number of in-flight commands (per connection) before backpressure will be applied.
  ///
  /// Default: 5000
  pub max_in_flight_commands: u64,
}

impl Default for BackpressureConfig {
  fn default() -> Self {
    BackpressureConfig {
      disable_auto_backpressure: false,
      disable_backpressure_scaling: false,
      min_sleep_duration_ms: 100,
      max_in_flight_commands: 5000,
    }
  }
}

/// Configuration options that can affect the performance of the client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PerformanceConfig {
  /// Whether or not the client should automatically pipeline commands when possible.
  ///
  /// Default: `true`
  pub pipeline: bool,
  /// The maximum number of times the client will attempt to send a command.
  ///
  /// This value be incremented on a command whenever the connection closes while the command is in-flight.
  ///
  /// Default: `3`
  pub max_command_attempts: u32,
  /// Configuration options for backpressure features in the client.
  pub backpressure: BackpressureConfig,
  /// An optional timeout (in milliseconds) to apply to all commands.
  ///
  /// If `0` this will disable any timeout being applied to commands.
  ///
  /// Default: `0`
  pub default_command_timeout_ms: u64,
  /// The maximum number of frames that will be passed to a socket before flushing the socket.
  ///
  /// Note: in some circumstances the client with always flush the socket (`QUIT`, `EXEC`, etc).
  ///
  /// Default: 1000
  pub max_feed_count: u64,
  /// The amount of time, in milliseconds, to wait after a `MOVED` or `ASK` error is received before the client will update the cached cluster state and try again.
  ///
  /// If `0` the client will follow `MOVED` or `ASK` redirects as quickly as possible. However, this can result in some unnecessary state synchronization commands when large values are being moved between nodes.
  ///
  /// Default: 50 ms
  pub cluster_cache_update_delay_ms: u64,
}

impl Default for PerformanceConfig {
  fn default() -> Self {
    PerformanceConfig {
      pipeline: true,
      backpressure: BackpressureConfig::default(),
      max_command_attempts: 3,
      default_command_timeout_ms: 0,
      max_feed_count: 1000,
      cluster_cache_update_delay_ms: 50,
    }
  }
}

/// Configuration options for a `RedisClient`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisConfig {
  /// Whether or not the client should return an error if it cannot connect to the server the first time when being initialized.
  /// If `false` the client will run the reconnect logic if it cannot connect to the server the first time, but if `true` the client
  /// will return initial connection errors to the caller immediately.
  ///
  /// Normally the reconnection logic only applies to connections that close unexpectedly, but this flag can apply the same logic to
  /// the first connection as it is being created.
  ///
  /// Note: Callers should use caution setting this to `false` since it can make debugging configuration issues more difficult.
  ///
  /// Default: `true`
  pub fail_fast: bool,
  /// The default behavior of the client when a command is sent while the connection is blocked on a blocking command.
  ///
  /// Default: `Blocking::Block`
  pub blocking: Blocking,
  /// An optional ACL username for the client to use when authenticating. If ACL rules are not configured this should be `None`.
  ///
  /// Default: `None`
  pub username: Option<String>,
  /// An optional password for the client to use when authenticating.
  ///
  /// Default: `None`
  pub password: Option<String>,
  /// Connection configuration for the server(s).
  ///
  /// Default: `Centralized(localhost, 6379)`
  pub server: ServerConfig,
  /// The protocol version to use when communicating with the server(s).
  ///
  /// If RESP3 is specified the client will automatically use `HELLO` when authenticating. **This requires Redis >=6.0.0.** If the `HELLO`
  /// command fails this will prevent the client from connecting. Callers should set this to RESP2 and use `HELLO` manually to fall back
  /// to RESP2 if needed.
  ///
  /// Default: `RESP2`
  pub version: RespVersion,
  /// Configuration options that can affect the performance of the client.
  pub performance: PerformanceConfig,
  /// An optional database number that the client will automatically `SELECT` after connecting or reconnecting.
  ///
  /// It is recommended that callers use this field instead of putting a `select()` call inside the `on_reconnect` block, if possible. Commands that were in-flight when the connection closed will retry before anything inside the `on_reconnect` block.
  ///
  /// Default: `None`
  pub database: Option<u8>,
  /// TLS configuration fields. If `None` the connection will not use TLS.
  ///
  /// Default: `None`
  #[cfg(feature = "enable-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-tls")))]
  pub tls: Option<TlsConfig>,
  /// Whether or not to enable tracing for this client.
  ///
  /// Default: `false`
  #[cfg(feature = "partial-tracing")]
  #[cfg_attr(docsrs, doc(cfg(feature = "partial-tracing")))]
  pub tracing: bool,
}

impl Default for RedisConfig {
  fn default() -> Self {
    RedisConfig {
      fail_fast: true,
      blocking: Blocking::default(),
      username: None,
      password: None,
      server: ServerConfig::default(),
      version: RespVersion::RESP2,
      performance: PerformanceConfig::default(),
      database: None,
      #[cfg(feature = "enable-tls")]
      #[cfg_attr(docsrs, doc(cfg(feature = "enable-tls")))]
      tls: None,
      #[cfg(feature = "partial-tracing")]
      #[cfg_attr(docsrs, doc(cfg(feature = "partial-tracing")))]
      tracing: false,
    }
  }
}

impl RedisConfig {
  /// Whether or not the client uses TLS.
  #[cfg(feature = "enable-tls")]
  pub fn uses_tls(&self) -> bool {
    self.tls.is_some()
  }

  /// Whether or not the client uses TLS.
  #[cfg(not(feature = "enable-tls"))]
  pub fn uses_tls(&self) -> bool {
    false
  }
}

/// Connection configuration for the Redis server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerConfig {
  Centralized {
    /// The hostname or IP address of the Redis server.
    host: String,
    /// The port on which the Redis server is listening.
    port: u16,
  },
  Clustered {
    /// An array of `(host, port)` tuples for nodes in the cluster. Only one node in the cluster needs to be provided here,
    /// the rest will be discovered via the `CLUSTER NODES` command.
    hosts: Vec<(String, u16)>,
  },
  Sentinel {
    /// An array of `(host, port)` tuples for each known sentinel instance.
    hosts: Vec<(String, u16)>,
    /// The service name for primary/main instances.
    service_name: String,

    /// An optional ACL username for the client to use when authenticating.
    #[cfg(feature = "sentinel-auth")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-auth")))]
    username: Option<String>,
    /// An optional password for the client to use when authenticating.
    #[cfg(feature = "sentinel-auth")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-auth")))]
    password: Option<String>,
  },
}

impl Default for ServerConfig {
  fn default() -> Self {
    ServerConfig::default_centralized()
  }
}

impl ServerConfig {
  /// Create a new centralized config with the provided host and port.
  pub fn new_centralized<S>(host: S, port: u16) -> ServerConfig
  where
    S: Into<String>,
  {
    ServerConfig::Centralized {
      host: host.into(),
      port,
    }
  }

  /// Create a new clustered config with the provided set of hosts and ports.
  ///
  /// Only one valid host in the cluster needs to be provided here. The client will use `CLUSTER NODES` to discover the other nodes.
  pub fn new_clustered<S>(mut hosts: Vec<(S, u16)>) -> ServerConfig
  where
    S: Into<String>,
  {
    ServerConfig::Clustered {
      hosts: hosts.drain(..).map(|(s, p)| (s.into(), p)).collect(),
    }
  }

  /// Create a new sentinel config with the provided set of hosts and the name of the service.
  ///
  /// This library will connect using the details from the [Redis documentation](https://redis.io/topics/sentinel-clients).
  pub fn new_sentinel<H, N>(mut hosts: Vec<(H, u16)>, service_name: N) -> ServerConfig
  where
    H: Into<String>,
    N: Into<String>,
  {
    ServerConfig::Sentinel {
      hosts: hosts.drain(..).map(|(h, p)| (h.into(), p)).collect(),
      service_name: service_name.into(),
      #[cfg(feature = "sentinel-auth")]
      username: None,
      #[cfg(feature = "sentinel-auth")]
      password: None,
    }
  }

  /// Create a centralized config with default settings for a local deployment.
  pub fn default_centralized() -> ServerConfig {
    ServerConfig::Centralized {
      host: "127.0.0.1".to_owned(),
      port: 6379,
    }
  }

  /// Create a clustered config with the same defaults as specified in the `create-cluster` script provided by Redis.
  pub fn default_clustered() -> ServerConfig {
    ServerConfig::Clustered {
      hosts: vec![
        ("127.0.0.1".to_owned(), 30001),
        ("127.0.0.1".to_owned(), 30002),
        ("127.0.0.1".to_owned(), 30003),
      ],
    }
  }

  /// Check if the config is for a clustered Redis deployment.
  pub fn is_clustered(&self) -> bool {
    match self {
      ServerConfig::Clustered { .. } => true,
      _ => false,
    }
  }

  /// Check if the config is for a sentinel deployment.
  pub fn is_sentinel(&self) -> bool {
    match self {
      ServerConfig::Sentinel { .. } => true,
      _ => false,
    }
  }

  /// Read the server hosts or sentinel hosts if using the sentinel interface.
  pub fn hosts(&self) -> Vec<(&str, u16)> {
    match *self {
      ServerConfig::Centralized { ref host, port } => vec![(host.as_str(), port)],
      ServerConfig::Clustered { ref hosts } => hosts.iter().map(|(h, p)| (h.as_str(), *p)).collect(),
      ServerConfig::Sentinel { ref hosts, .. } => hosts.iter().map(|(h, p)| (h.as_str(), *p)).collect(),
    }
  }
}
