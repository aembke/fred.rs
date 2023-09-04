use crate::{
  clients::{RedisClient, RedisPool},
  error::{RedisError, RedisErrorKind},
  prelude::ReconnectPolicy,
  types::{ConnectionConfig, PerformanceConfig, RedisConfig, ServerConfig},
};

#[cfg(feature = "subscriber-client")]
use crate::clients::SubscriberClient;
#[cfg(feature = "sentinel-client")]
use crate::{clients::SentinelClient, types::SentinelConfig};

/// A client and pool builder interface.
///
///
/// ```rust
/// use fred::prelude::*;
///
/// fn example() -> Result<(), RedisError> {
///   // use default values
///   let client = Builder::default_centralized().build()?;
///   #[cfg(feature = "subscriber-client")]
///   let subscriber = Builder::default_centralized().build_subscriber_client()?;
///   #[cfg(feature = "sentinel-client")]
///   let sentinel = Builder::default_centralized().build_sentinel_client()?;
///
///   // or customize the config structs
///   let config = RedisConfig::from_url("redis://localhost:6379/1")?;
///   let perf = PerformanceConfig::default();
///   let mut builder = Builder::from_config(config);
///   // set config structs directly
///   builder.set_performance_config(perf)
///     // or modify them in place (creating defaults if needed)
///     .with_performance_config(|config| {
///       config.auto_pipeline = true;
///     })
///     .with_connection_config(|config| {
///       config.tcp = TcpConfig {
///         nodelay: true,
///         ..Default::default()
///       };
///     });
///
///   let client = builder.build()?;
///   let pool = builder.build_pool(3)?;
///
///   // ...
///
///   Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct Builder {
  config:      Option<RedisConfig>,
  performance: PerformanceConfig,
  connection:  ConnectionConfig,
  policy:      Option<ReconnectPolicy>,
  #[cfg(feature = "sentinel-client")]
  sentinel:    Option<SentinelConfig>,
}

impl Default for Builder {
  fn default() -> Self {
    Builder {
      config:                                       None,
      performance:                                  PerformanceConfig::default(),
      connection:                                   ConnectionConfig::default(),
      policy:                                       None,
      #[cfg(feature = "sentinel-client")]
      sentinel:                                     None,
    }
  }
}

impl Builder {
  /// Create a new builder instance with default config values for a centralized deployment.
  pub fn default_centralized() -> Self {
    Builder {
      config: Some(RedisConfig {
        server: ServerConfig::default_centralized(),
        ..Default::default()
      }),
      ..Default::default()
    }
  }

  /// Create a new builder instance with default config values for a clustered deployment.
  pub fn default_clustered() -> Self {
    Builder {
      config: Some(RedisConfig {
        server: ServerConfig::default_clustered(),
        ..Default::default()
      }),
      ..Default::default()
    }
  }

  /// Create a new builder instance from the provided client config.
  pub fn from_config(config: RedisConfig) -> Self {
    Builder {
      config: Some(config),
      ..Default::default()
    }
  }

  /// Read the client config.
  pub fn get_config(&self) -> Option<&RedisConfig> {
    self.config.as_ref()
  }

  /// Read the reconnection policy.
  pub fn get_policy(&self) -> Option<&ReconnectPolicy> {
    self.policy.as_ref()
  }

  /// Read the performance config.
  pub fn get_performance_config(&self) -> &PerformanceConfig {
    &self.performance
  }

  /// Read the connection config.
  pub fn get_connection_config(&self) -> &ConnectionConfig {
    &self.connection
  }

  /// Read the sentinel client config.
  #[cfg(feature = "sentinel-client")]
  #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
  pub fn get_sentinel_config(&self) -> Option<&RedisConfig> {
    self.config.as_ref()
  }

  /// Overwrite the client config on the builder.
  pub fn set_config(&mut self, config: RedisConfig) -> &mut Self {
    self.config = Some(config);
    self
  }

  /// Overwrite the reconnection policy on the builder.
  pub fn set_policy(&mut self, policy: ReconnectPolicy) -> &mut Self {
    self.policy = Some(policy);
    self
  }

  /// Overwrite the performance config on the builder.
  pub fn set_performance_config(&mut self, config: PerformanceConfig) -> &mut Self {
    self.performance = config;
    self
  }

  /// Overwrite the connection config on the builder.
  pub fn set_connection_config(&mut self, config: ConnectionConfig) -> &mut Self {
    self.connection = config;
    self
  }

  /// Overwrite the sentinel config on the builder.
  #[cfg(feature = "sentinel-client")]
  #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
  pub fn set_sentinel_config(&mut self, config: SentinelConfig) -> &mut Self {
    self.sentinel = Some(config);
    self
  }

  /// Modify the client config in place, creating a new one with default centralized values first if needed.
  pub fn with_config<F>(&mut self, func: F) -> &mut Self
  where
    F: FnOnce(&mut RedisConfig),
  {
    if let Some(config) = self.config.as_mut() {
      func(config);
    } else {
      let mut config = RedisConfig::default();
      func(&mut config);
      self.config = Some(config);
    }

    self
  }

  /// Modify the performance config in place, creating a new one with default values first if needed.
  pub fn with_performance_config<F>(&mut self, func: F) -> &mut Self
  where
    F: FnOnce(&mut PerformanceConfig),
  {
    func(&mut self.performance);
    self
  }

  /// Modify the connection config in place, creating a new one with default values first if needed.
  pub fn with_connection_config<F>(&mut self, func: F) -> &mut Self
  where
    F: FnOnce(&mut ConnectionConfig),
  {
    func(&mut self.connection);
    self
  }

  /// Modify the sentinel config in place, creating a new one with default values first if needed.
  #[cfg(feature = "sentinel-client")]
  #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
  pub fn with_sentinel_config<F>(&mut self, func: F) -> &mut Self
  where
    F: FnOnce(&mut SentinelConfig),
  {
    if let Some(config) = self.sentinel.as_mut() {
      func(config);
    } else {
      let mut config = SentinelConfig::default();
      func(&mut config);
      self.sentinel = Some(config);
    }

    self
  }

  /// Create a new client.
  pub fn build(&self) -> Result<RedisClient, RedisError> {
    if let Some(config) = self.config.as_ref() {
      Ok(RedisClient::new(
        config.clone(),
        Some(self.performance.clone()),
        Some(self.connection.clone()),
        self.policy.clone(),
      ))
    } else {
      Err(RedisError::new(RedisErrorKind::Config, "Missing client configuration."))
    }
  }

  /// Create a new client pool.
  pub fn build_pool(&self, size: usize) -> Result<RedisPool, RedisError> {
    if let Some(config) = self.config.as_ref() {
      RedisPool::new(
        config.clone(),
        Some(self.performance.clone()),
        Some(self.connection.clone()),
        self.policy.clone(),
        size,
      )
    } else {
      Err(RedisError::new(RedisErrorKind::Config, "Missing client configuration."))
    }
  }

  /// Create a new subscriber client.
  #[cfg(feature = "subscriber-client")]
  #[cfg_attr(docsrs, doc(cfg(feature = "subscriber-client")))]
  pub fn build_subscriber_client(&self) -> Result<SubscriberClient, RedisError> {
    if let Some(config) = self.config.as_ref() {
      Ok(SubscriberClient::new(
        config.clone(),
        Some(self.performance.clone()),
        Some(self.connection.clone()),
        self.policy.clone(),
      ))
    } else {
      Err(RedisError::new(RedisErrorKind::Config, "Missing client configuration."))
    }
  }

  /// Create a new sentinel client.
  ///
  /// This is only necessary if callers need to communicate directly with sentinel nodes. Use a
  /// `ServerConfig::Sentinel` to interact with Redis servers behind a sentinel layer.
  #[cfg(feature = "sentinel-client")]
  #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
  pub fn build_sentinel_client(&self) -> Result<SentinelClient, RedisError> {
    if let Some(config) = self.sentinel.as_ref() {
      Ok(SentinelClient::new(
        config.clone(),
        Some(self.performance.clone()),
        Some(self.connection.clone()),
        self.policy.clone(),
      ))
    } else {
      Err(RedisError::new(
        RedisErrorKind::Config,
        "Missing sentinel client configuration.",
      ))
    }
  }
}
