use crate::clients::RedisClient;
use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{
  async_spawn, wrap_async, AsyncResult, AuthInterface, ClientLike, MetricsInterface, PubsubInterface,
};
use crate::modules::inner::RedisClientInner;
use crate::types::{MultipleStrings, RedisConfig};
use crate::utils;
use bytes_utils::Str;
use futures::future::join_all;
use futures::Stream;
use parking_lot::RwLock;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Formatter;
use std::mem;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;

type ChannelSet = Arc<RwLock<BTreeSet<Str>>>;

fn from_redis_client(client: RedisClient, channels: &ChannelSet, patterns: &ChannelSet) -> SubscriberClient {
  SubscriberClient {
    inner: client.inner,
    patterns: patterns.clone(),
    channels: channels.clone(),
  }
}

fn result_of_vec(vec: Vec<Result<(), RedisError>>) -> Result<(), RedisError> {
  vec.into_iter().collect()
}

fn add_to_channels(channels: &ChannelSet, channel: Str) {
  channels.write().insert(channel);
}

fn remove_from_channels(channels: &ChannelSet, channel: &str) {
  channels.write().remove(channel);
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReconnectOperation {
  Subscribe,
  PSubscribe,
  Unsubscribe,
  PUnsubscribe,
}

fn concurrent_op(
  client: &SubscriberClient,
  channels: BTreeSet<Str>,
  operation: ReconnectOperation,
) -> Vec<AsyncResult<()>> {
  let client = client.clone();

  channels
    .into_iter()
    .map(move |val| {
      let (operation, client) = (operation.clone(), client.clone());
      wrap_async(|| async move {
        match operation {
          ReconnectOperation::Subscribe => client.subscribe(val).await.map(|_| ()),
          ReconnectOperation::PSubscribe => client.psubscribe(val).await.map(|_| ()),
          ReconnectOperation::Unsubscribe => client.unsubscribe(val).await.map(|_| ()),
          ReconnectOperation::PUnsubscribe => client.punsubscribe(val).await.map(|_| ()),
        }
      })
    })
    .collect()
}

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
  inner: Arc<RedisClientInner>,
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

impl PubsubInterface for SubscriberClient {
  fn subscribe<S>(&self, channel: S) -> AsyncResult<usize>
  where
    S: Into<Str>,
  {
    into!(channel);
    let cached_channels = self.channels.clone();
    async_spawn(self, |inner| async move {
      let result = commands::pubsub::subscribe(&inner, channel.clone()).await;
      if result.is_ok() {
        add_to_channels(&cached_channels, channel);
      }
      result
    })
  }

  fn psubscribe<S>(&self, patterns: S) -> AsyncResult<Vec<usize>>
  where
    S: Into<MultipleStrings>,
  {
    into!(patterns);
    let cached_patterns = self.patterns.clone();
    async_spawn(self, |inner| async move {
      let result = commands::pubsub::psubscribe(&inner, patterns.clone()).await;
      if result.is_ok() {
        for pattern in patterns.inner().into_iter() {
          if let Some(pattern) = pattern.as_bytes_str() {
            add_to_channels(&cached_patterns, pattern)
          }
        }
      }
      result
    })
  }

  fn unsubscribe<S>(&self, channel: S) -> AsyncResult<usize>
  where
    S: Into<Str>,
  {
    into!(channel);
    let cached_channels = self.channels.clone();
    async_spawn(self, |inner| async move {
      let result = commands::pubsub::unsubscribe(&inner, channel.clone()).await;
      if result.is_ok() {
        remove_from_channels(&cached_channels, &channel);
      }
      result
    })
  }

  fn punsubscribe<S>(&self, patterns: S) -> AsyncResult<Vec<usize>>
  where
    S: Into<MultipleStrings>,
  {
    into!(patterns);
    let cached_patterns = self.patterns.clone();
    async_spawn(self, |inner| async move {
      let result = commands::pubsub::punsubscribe(&inner, patterns.clone()).await;
      if result.is_ok() {
        for pattern in patterns.inner().into_iter() {
          if let Some(pattern) = pattern.as_bytes_str() {
            remove_from_channels(&cached_patterns, &pattern)
          }
        }
      }
      result
    })
  }
}

impl SubscriberClient {
  /// Create a new client instance without connecting to the server.
  pub fn new(config: RedisConfig) -> SubscriberClient {
    SubscriberClient {
      channels: Arc::new(RwLock::new(BTreeSet::new())),
      patterns: Arc::new(RwLock::new(BTreeSet::new())),
      inner: RedisClientInner::new(config),
    }
  }

  /// Create a new `SubscriberClient` from the config provided to this client.
  ///
  /// The returned client will not be connected to the server, and it will use new connections after connecting. However, it will manage the same channel subscriptions as the original client.
  pub fn clone_new(&self) -> Self {
    let inner = RedisClientInner::new(utils::read_locked(&self.inner.config));

    SubscriberClient {
      inner,
      channels: Arc::new(RwLock::new(self.channels.read().clone())),
      patterns: Arc::new(RwLock::new(self.patterns.read().clone())),
    }
  }

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client successfully reconnects in order to select the right database again, re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  pub fn on_reconnect(&self) -> impl Stream<Item = Self> {
    let (tx, rx) = unbounded_channel();
    self.inner().reconnect_tx.write().push_back(tx);

    let channels = self.channels.clone();
    let patterns = self.patterns.clone();
    UnboundedReceiverStream::new(rx).map(move |client| from_redis_client(client, &channels, &patterns))
  }

  /// Spawn a task that will automatically re-subscribe to any channels or channel patterns used by the client.
  pub fn manage_subscriptions(&self) -> JoinHandle<()> {
    let _self = self.clone();
    tokio::spawn(async move {
      let mut stream = _self.on_reconnect();

      while let Some(client) = stream.next().await {
        if let Err(error) = client.resubscribe_all().await {
          error!(
            "{}: Failed to resubscribe to channels or patterns: {:?}",
            client.id(),
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

  /// Re-subscribe to any tracked channels and patterns concurrently.
  ///
  /// This can be used to sync the client's subscriptions with the server after calling `QUIT`, then `connect`, etc.
  pub async fn resubscribe_all(&self) -> Result<(), RedisError> {
    let channels = self.tracked_channels();
    let patterns = self.tracked_patterns();

    let mut channel_tasks = concurrent_op(self, channels, ReconnectOperation::Subscribe);
    let pattern_tasks = concurrent_op(self, patterns, ReconnectOperation::PSubscribe);
    channel_tasks.extend(pattern_tasks);

    result_of_vec(join_all(channel_tasks).await)?;
    Ok(())
  }

  /// Unsubscribe from all tracked channels and patterns, and remove them from the client cache.
  pub async fn unsubscribe_all(&self) -> Result<(), RedisError> {
    let channels = mem::replace(&mut *self.channels.write(), BTreeSet::new());
    let patterns = mem::replace(&mut *self.patterns.write(), BTreeSet::new());

    let mut channel_tasks = concurrent_op(self, channels, ReconnectOperation::Unsubscribe);
    let pattern_tasks = concurrent_op(self, patterns, ReconnectOperation::PUnsubscribe);
    channel_tasks.extend(pattern_tasks);

    result_of_vec(join_all(channel_tasks).await)?;
    Ok(())
  }
}
