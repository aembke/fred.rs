use crate::{
  clients::RedisClient,
  commands,
  error::RedisError,
  interfaces::{AuthInterface, ClientLike, MetricsInterface, PipelineInterface, PubsubInterface, RedisResult},
  modules::inner::RedisClientInner,
  prelude::FromRedis,
  types::{MultipleStrings, PerformanceConfig, ReconnectPolicy, RedisConfig},
  utils,
};
use bytes_utils::Str;
use futures::{
  future::{join_all, try_join_all, FutureExt},
  Stream,
};
use parking_lot::RwLock;
use std::{collections::BTreeSet, fmt, fmt::Formatter, mem, sync::Arc};
use tokio::{sync::mpsc::unbounded_channel, task::JoinHandle};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};

type ChannelSet = Arc<RwLock<BTreeSet<Str>>>;

/// A subscriber client that will manage subscription state to any pubsub channels or patterns for the caller.
///
/// If the connection to the server closes for any reason this struct can automatically re-subscribe, etc.
///
/// ```rust no_run
/// use fred::clients::SubscriberClient;
/// use fred::prelude::*;
/// use futures::stream::StreamExt;
///
/// let subscriber = SubscriberClient::new(RedisConfig::default());
/// let _ = subscriber.connect(Some(ReconnectPolicy::default()));
/// let _ = subscriber.wait_for_connect().await?;
/// // spawn a task that will automatically re-subscribe to channels and patterns as needed
/// let _ = subscriber.manage_subscriptions();
///
/// // do pubsub things
/// let jh = tokio::spawn(subscriber.on_message().for_each_concurrent(10, |(channel, message)| {
///   println!("Recv message {:?} on channel {}", message, channel);
///   Ok(())
/// }));
///
/// let _ = subscriber.subscribe("foo").await?;
/// let _ = subscriber.psubscribe("bar*").await?;
/// // if the subscriber connection closes now for any reason the client will automatically re-subscribe to "foo" and "bar*"
///
/// // some convenience functions exist as well
/// println!("Tracking channels: {:?}", subscriber.tracked_channels());
/// println!("Tracking patterns: {:?}", subscriber.tracked_patterns());
///
/// // or force a re-subscription at any time
/// let _ = subscriber.resubscribe_all().await?;
/// // or clear all the local state and unsubscribe
/// let _ = subscriber.unsubscribe_all().await?;
///
/// // basic commands (AUTH, QUIT, INFO, PING, etc) work the same as the `RedisClient`
/// // additionally, tracing and metrics are supported in the same way as the `RedisClient`
/// let _ = subscriber.quit().await?;
/// let _ = jh.await;
/// ```
#[derive(Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "subscriber-client")))]
pub struct SubscriberClient {
  channels: ChannelSet,
  patterns: ChannelSet,
  inner:    Arc<RedisClientInner>,
}

impl fmt::Debug for SubscriberClient {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("SubscriberClient")
      .field("id", &self.inner.id)
      .field("channels", &self.tracked_channels())
      .field("patterns", &self.tracked_patterns())
      .finish()
  }
}

impl ClientLike for SubscriberClient {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}

impl AuthInterface for SubscriberClient {}
impl MetricsInterface for SubscriberClient {}

#[async_trait]
impl PubsubInterface for SubscriberClient {
  async fn subscribe<R, S>(&self, channel: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    into!(channel);

    let result = commands::pubsub::subscribe(self, channel.clone()).await;
    if result.is_ok() {
      self.channels.write().insert(channel);
    }

    result.and_then(|r| r.convert())
  }

  async fn psubscribe<R, S>(&self, patterns: S) -> RedisResult<R>
  where
    R: FromRedis,
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
    result.and_then(|r| r.convert())
  }

  async fn unsubscribe<R, S>(&self, channel: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    into!(channel);

    let result = commands::pubsub::unsubscribe(self, channel.clone()).await;
    if result.is_ok() {
      let _ = self.channels.write().remove(&channel);
    }
    result.and_then(|r| r.convert())
  }

  async fn punsubscribe<R, S>(&self, patterns: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<MultipleStrings> + Send,
  {
    into!(patterns);

    let result = commands::pubsub::punsubscribe(self, patterns.clone()).await;
    if result.is_ok() {
      let mut guard = self.patterns.write();

      for pattern in patterns.inner().into_iter() {
        if let Some(pattern) = pattern.as_bytes_str() {
          let _ = guard.remove(&pattern);
        }
      }
    }
    result.and_then(|r| r.convert())
  }
}

impl SubscriberClient {
  /// Create a new client instance without connecting to the server.
  pub fn new(
    config: RedisConfig,
    perf: Option<PerformanceConfig>,
    policy: Option<ReconnectPolicy>,
  ) -> SubscriberClient {
    SubscriberClient {
      channels: Arc::new(RwLock::new(BTreeSet::new())),
      patterns: Arc::new(RwLock::new(BTreeSet::new())),
      inner:    RedisClientInner::new(config, perf.unwrap_or_default(), policy),
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
      self.inner.reconnect_policy(),
    );

    SubscriberClient {
      inner,
      channels: Arc::new(RwLock::new(self.channels.read().clone())),
      patterns: Arc::new(RwLock::new(self.patterns.read().clone())),
    }
  }

  /// Spawn a task that will automatically re-subscribe to any channels or channel patterns used by the client.
  pub fn manage_subscriptions(&self) -> JoinHandle<()> {
    let _self = self.clone();
    tokio::spawn(async move {
      let mut stream = _self.on_reconnect();

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

  /// Re-subscribe to any tracked channels and patterns.
  ///
  /// This can be used to sync the client's subscriptions with the server after calling `QUIT`, then `connect`, etc.
  pub async fn resubscribe_all(&self) -> Result<(), RedisError> {
    let channels = self.tracked_channels();
    let patterns = self.tracked_patterns();

    for channel in channels.into_iter() {
      let _ = self.subscribe(channel).await?;
    }
    for pattern in patterns.into_iter() {
      let _ = self.psubscribe(pattern).await?;
    }

    Ok(())
  }

  /// Unsubscribe from all tracked channels and patterns, and remove them from the client cache.
  pub async fn unsubscribe_all(&self) -> Result<(), RedisError> {
    let channels = mem::replace(&mut *self.channels.write(), BTreeSet::new());
    let patterns = mem::replace(&mut *self.patterns.write(), BTreeSet::new());

    for channel in channels.into_iter() {
      let _ = self.unsubscribe(channel).await?;
    }
    for pattern in patterns.into_iter() {
      let _ = self.punsubscribe(pattern).await?;
    }

    Ok(())
  }
}
