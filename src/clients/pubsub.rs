use crate::{
  commands,
  error::RedisError,
  interfaces::*,
  modules::inner::RedisClientInner,
  prelude::RedisClient,
  types::{ConnectionConfig, MultipleStrings, PerformanceConfig, ReconnectPolicy, RedisConfig, RedisKey},
  util::group_by_hash_slot,
};
use bytes_utils::Str;
use parking_lot::RwLock;
use std::{collections::BTreeSet, fmt, fmt::Formatter, mem, sync::Arc};
use tokio::task::JoinHandle;

#[cfg(feature = "client-tracking")]
use crate::interfaces::TrackingInterface;

type ChannelSet = Arc<RwLock<BTreeSet<Str>>>;

/// A subscriber client that will manage subscription state to any [pubsub](https://redis.io/docs/manual/pubsub/) channels or patterns for the caller.
///
/// If the connection to the server closes for any reason this struct can automatically re-subscribe to channels,
/// patterns, and sharded channels.
///
/// **Note: most non-pubsub commands are only supported when using RESP3.**
///
/// ```rust no_run
/// use fred::clients::SubscriberClient;
/// use fred::prelude::*;
///
/// async fn example() -> Result<(), RedisError> {
///   let subscriber = Builder::default_centralized().build_subscriber_client()?;
///   subscriber.init().await?;
///
///   // spawn a task that will re-subscribe to channels and patterns after reconnecting
///   let _ = subscriber.manage_subscriptions();
///
///   let mut message_rx = subscriber.message_rx();
///   let jh = tokio::spawn(async move {
///     while let Ok(message) = message_rx.recv().await {
///       println!("Recv message {:?} on channel {}", message.value, message.channel);
///     }
///   });
///
///   let _ = subscriber.subscribe("foo").await?;
///   let _ = subscriber.psubscribe("bar*").await?;
///   println!("Tracking channels: {:?}", subscriber.tracked_channels()); // foo
///   println!("Tracking patterns: {:?}", subscriber.tracked_patterns()); // bar*
///
///   // force a re-subscription
///   subscriber.resubscribe_all().await?;
///   // clear all the local state and unsubscribe
///   subscriber.unsubscribe_all().await?;
///   subscriber.quit().await?;
///   Ok(())
/// }
/// ```
#[derive(Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "subscriber-client")))]
pub struct SubscriberClient {
  channels:       ChannelSet,
  patterns:       ChannelSet,
  shard_channels: ChannelSet,
  inner:          Arc<RedisClientInner>,
}

impl fmt::Debug for SubscriberClient {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("SubscriberClient")
      .field("id", &self.inner.id)
      .field("channels", &self.tracked_channels())
      .field("patterns", &self.tracked_patterns())
      .field("shard_channels", &self.tracked_shard_channels())
      .finish()
  }
}

impl ClientLike for SubscriberClient {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}

impl EventInterface for SubscriberClient {}
impl AclInterface for SubscriberClient {}
impl ClientInterface for SubscriberClient {}
impl ClusterInterface for SubscriberClient {}
impl ConfigInterface for SubscriberClient {}
impl GeoInterface for SubscriberClient {}
impl HashesInterface for SubscriberClient {}
impl HyperloglogInterface for SubscriberClient {}
impl MetricsInterface for SubscriberClient {}
impl TransactionInterface for SubscriberClient {}
impl KeysInterface for SubscriberClient {}
impl LuaInterface for SubscriberClient {}
impl ListInterface for SubscriberClient {}
impl MemoryInterface for SubscriberClient {}
impl AuthInterface for SubscriberClient {}
impl ServerInterface for SubscriberClient {}
impl SlowlogInterface for SubscriberClient {}
impl SetsInterface for SubscriberClient {}
impl SortedSetsInterface for SubscriberClient {}
impl HeartbeatInterface for SubscriberClient {}
impl StreamsInterface for SubscriberClient {}
impl FunctionInterface for SubscriberClient {}
#[cfg(feature = "redis-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-json")))]
impl RedisJsonInterface for SubscriberClient {}
#[cfg(feature = "time-series")]
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
impl TimeSeriesInterface for SubscriberClient {}

#[cfg(feature = "client-tracking")]
#[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
impl TrackingInterface for SubscriberClient {}

#[async_trait]
impl PubsubInterface for SubscriberClient {
  async fn subscribe<S>(&self, channels: S) -> RedisResult<()>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(channels);

    let result = commands::pubsub::subscribe(self, channels.clone()).await;
    if result.is_ok() {
      let mut guard = self.channels.write();

      for channel in channels.inner().into_iter() {
        if let Some(channel) = channel.as_bytes_str() {
          guard.insert(channel);
        }
      }
    }

    result
  }

  async fn psubscribe<S>(&self, patterns: S) -> RedisResult<()>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(patterns);

    let result = commands::pubsub::psubscribe(self, patterns.clone()).await;
    if result.is_ok() {
      let mut guard = self.patterns.write();

      for pattern in patterns.inner().into_iter() {
        if let Some(pattern) = pattern.as_bytes_str() {
          guard.insert(pattern);
        }
      }
    }
    result
  }

  async fn unsubscribe<S>(&self, channels: S) -> RedisResult<()>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(channels);

    let result = commands::pubsub::unsubscribe(self, channels.clone()).await;
    if result.is_ok() {
      let mut guard = self.channels.write();

      if channels.len() == 0 {
        guard.clear();
      } else {
        for channel in channels.inner().into_iter() {
          if let Some(channel) = channel.as_bytes_str() {
            let _ = guard.remove(&channel);
          }
        }
      }
    }
    result
  }

  async fn punsubscribe<S>(&self, patterns: S) -> RedisResult<()>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(patterns);

    let result = commands::pubsub::punsubscribe(self, patterns.clone()).await;
    if result.is_ok() {
      let mut guard = self.patterns.write();

      if patterns.len() == 0 {
        guard.clear();
      } else {
        for pattern in patterns.inner().into_iter() {
          if let Some(pattern) = pattern.as_bytes_str() {
            let _ = guard.remove(&pattern);
          }
        }
      }
    }
    result
  }

  async fn ssubscribe<C>(&self, channels: C) -> RedisResult<()>
  where
    C: Into<MultipleStrings> + Send,
  {
    into!(channels);

    let result = commands::pubsub::ssubscribe(self, channels.clone()).await;
    if result.is_ok() {
      let mut guard = self.shard_channels.write();

      for channel in channels.inner().into_iter() {
        if let Some(channel) = channel.as_bytes_str() {
          guard.insert(channel);
        }
      }
    }
    result
  }

  async fn sunsubscribe<C>(&self, channels: C) -> RedisResult<()>
  where
    C: Into<MultipleStrings> + Send,
  {
    into!(channels);

    let result = commands::pubsub::sunsubscribe(self, channels.clone()).await;
    if result.is_ok() {
      let mut guard = self.shard_channels.write();

      if channels.len() == 0 {
        guard.clear();
      } else {
        for channel in channels.inner().into_iter() {
          if let Some(channel) = channel.as_bytes_str() {
            let _ = guard.remove(&channel);
          }
        }
      }
    }
    result
  }
}

impl SubscriberClient {
  /// Create a new client instance without connecting to the server.
  ///
  /// See the [builder](crate::types::Builder) interface for more information.
  pub fn new(
    config: RedisConfig,
    perf: Option<PerformanceConfig>,
    connection: Option<ConnectionConfig>,
    policy: Option<ReconnectPolicy>,
  ) -> SubscriberClient {
    SubscriberClient {
      channels:       Arc::new(RwLock::new(BTreeSet::new())),
      patterns:       Arc::new(RwLock::new(BTreeSet::new())),
      shard_channels: Arc::new(RwLock::new(BTreeSet::new())),
      inner:          RedisClientInner::new(config, perf.unwrap_or_default(), connection.unwrap_or_default(), policy),
    }
  }

  /// Create a new `SubscriberClient` from the config provided to this client.
  ///
  /// The returned client will not be connected to the server, and it will use new connections after connecting.
  /// However, it will manage the same channel subscriptions as the original client.
  pub fn clone_new(&self) -> Self {
    let inner = RedisClientInner::new(
      self.inner.config.as_ref().clone(),
      self.inner.performance_config(),
      self.inner.connection.as_ref().clone(),
      self.inner.reconnect_policy(),
    );

    SubscriberClient {
      inner,
      channels: Arc::new(RwLock::new(self.channels.read().clone())),
      patterns: Arc::new(RwLock::new(self.patterns.read().clone())),
      shard_channels: Arc::new(RwLock::new(self.shard_channels.read().clone())),
    }
  }

  /// Spawn a task that will automatically re-subscribe to any channels or channel patterns used by the client.
  pub fn manage_subscriptions(&self) -> JoinHandle<()> {
    let _self = self.clone();
    tokio::spawn(async move {
      let mut stream = _self.reconnect_rx();

      while let Ok(_) = stream.recv().await {
        if let Err(error) = _self.resubscribe_all().await {
          error!(
            "{}: Failed to resubscribe to channels or patterns: {:?}",
            _self.id(),
            error
          );
        }
      }
    })
  }

  /// Read the set of channels that this client will manage.
  pub fn tracked_channels(&self) -> BTreeSet<Str> {
    self.channels.read().clone()
  }

  /// Read the set of channel patterns that this client will manage.
  pub fn tracked_patterns(&self) -> BTreeSet<Str> {
    self.patterns.read().clone()
  }

  /// Read the set of shard channels that this client will manage.
  pub fn tracked_shard_channels(&self) -> BTreeSet<Str> {
    self.shard_channels.read().clone()
  }

  /// Re-subscribe to any tracked channels and patterns.
  ///
  /// This can be used to sync the client's subscriptions with the server after calling `QUIT`, then `connect`, etc.
  pub async fn resubscribe_all(&self) -> Result<(), RedisError> {
    let channels: Vec<RedisKey> = self.tracked_channels().into_iter().map(|s| s.into()).collect();
    let patterns: Vec<RedisKey> = self.tracked_patterns().into_iter().map(|s| s.into()).collect();
    let shard_channels: Vec<RedisKey> = self.tracked_shard_channels().into_iter().map(|s| s.into()).collect();

    self.subscribe(channels).await?;
    self.psubscribe(patterns).await?;

    let shard_channel_groups = group_by_hash_slot(shard_channels)?;
    for (_, keys) in shard_channel_groups.into_iter() {
      // the client never pipelines this so no point in using join! or a pipeline here
      self.ssubscribe(keys).await?;
    }

    Ok(())
  }

  /// Unsubscribe from all tracked channels and patterns, and remove them from the client cache.
  pub async fn unsubscribe_all(&self) -> Result<(), RedisError> {
    let channels: Vec<RedisKey> = mem::take(&mut *self.channels.write())
      .into_iter()
      .map(|s| s.into())
      .collect();
    let patterns: Vec<RedisKey> = mem::take(&mut *self.patterns.write())
      .into_iter()
      .map(|s| s.into())
      .collect();
    let shard_channels: Vec<RedisKey> = mem::take(&mut *self.shard_channels.write())
      .into_iter()
      .map(|s| s.into())
      .collect();

    self.unsubscribe(channels).await?;
    self.punsubscribe(patterns).await?;

    let shard_channel_groups = group_by_hash_slot(shard_channels)?;
    let shard_subscriptions: Vec<_> = shard_channel_groups
      .into_iter()
      .map(|(_, keys)| async { self.sunsubscribe(keys).await })
      .collect();

    futures::future::try_join_all(shard_subscriptions).await?;
    Ok(())
  }

  /// Create a new `RedisClient`, reusing the existing connection(s).
  ///
  /// Note: most non-pubsub commands are only supported when using RESP3.
  pub fn to_client(&self) -> RedisClient {
    RedisClient::from(&self.inner)
  }
}
