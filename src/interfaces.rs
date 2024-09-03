use crate::{
  commands,
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::command::{RedisCommand, RouterCommand},
  runtime::{sleep, spawn, BroadcastReceiver, JoinHandle, RefCount},
  types::{ClientState, ClusterStateChange, KeyspaceEvent, Message, RespVersion, Server},
  utils,
};
use bytes_utils::Str;
use futures::Future;
pub use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use rm_send_macros::rm_send_if;
use std::time::Duration;

/// Type alias for `Result<T, RedisError>`.
pub type RedisResult<T> = Result<T, RedisError>;

/// Send a single `RedisCommand` to the router.
pub(crate) fn default_send_command<C>(inner: &RefCount<RedisClientInner>, command: C) -> Result<(), RedisError>
where
  C: Into<RedisCommand>,
{
  let mut command: RedisCommand = command.into();
  _trace!(
    inner,
    "Sending command {} ({}) to router.",
    command.kind.to_str_debug(),
    command.debug_id()
  );
  command.inherit_options(inner);

  send_to_router(inner, command.into())
}

/// Send a `RouterCommand` to the router.
pub(crate) fn send_to_router(inner: &RefCount<RedisClientInner>, command: RouterCommand) -> Result<(), RedisError> {
  #[allow(clippy::collapsible_if)]
  if command.should_check_fail_fast() {
    if utils::read_locked(&inner.state) != ClientState::Connected {
      _debug!(inner, "Responding early after fail fast check.");
      command.finish_with_error(RedisError::new(
        RedisErrorKind::Canceled,
        "Connection closed unexpectedly.",
      ));
      return Ok(());
    }
  }

  let new_len = inner.counters.incr_cmd_buffer_len();
  let should_apply_backpressure = inner.connection.max_command_buffer_len > 0
    && new_len > inner.connection.max_command_buffer_len
    && !command.should_skip_backpressure();

  if should_apply_backpressure {
    inner.counters.decr_cmd_buffer_len();
    command.finish_with_error(RedisError::new(
      RedisErrorKind::Backpressure,
      "Max command queue length exceeded.",
    ));
    return Ok(());
  }

  if let Err(e) = inner.send_command(command) {
    // usually happens if the caller tries to send a command before calling `connect` or after calling `quit`
    inner.counters.decr_cmd_buffer_len();

    if let RouterCommand::Command(mut command) = e {
      _warn!(
        inner,
        "Fatal error sending {} command to router. Client may be stopped or not yet initialized.",
        command.kind.to_str_debug()
      );

      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Client is not initialized.",
      )));
    } else {
      _warn!(
        inner,
        "Fatal error sending command to router. Client may be stopped or not yet initialized."
      );
    }

    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Failed to send command to router.",
    ))
  } else {
    Ok(())
  }
}

#[cfg(not(feature = "glommio"))]
pub use crate::_tokio::ClientLike;
#[cfg(feature = "glommio")]
pub use crate::glommio::interfaces::ClientLike;

#[cfg(not(feature = "glommio"))]
pub use crate::_tokio::spawn_event_listener;
#[cfg(feature = "glommio")]
pub use crate::glommio::interfaces::spawn_event_listener;

/// Functions that provide a connection heartbeat interface.
#[rm_send_if(feature = "glommio")]
pub trait HeartbeatInterface: ClientLike {
  /// Return a future that will ping the server on an interval.
  #[allow(unreachable_code)]
  fn enable_heartbeat(
    &self,
    interval: Duration,
    break_on_error: bool,
  ) -> impl Future<Output = RedisResult<()>> + Send {
    async move {
      let _self = self.clone();

      loop {
        sleep(interval).await;

        if break_on_error {
          let _: () = _self.ping().await?;
        } else if let Err(e) = _self.ping::<()>().await {
          warn!("{}: Heartbeat ping failed with error: {:?}", _self.inner().id, e);
        }
      }

      Ok(())
    }
  }
}

/// Functions for authenticating clients.
#[rm_send_if(feature = "glommio")]
pub trait AuthInterface: ClientLike {
  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated
  /// `RedisConfig` when calling [connect](crate::interfaces::ClientLike::connect).
  ///
  /// If running against clustered servers this function will authenticate all connections.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<S>(&self, username: Option<String>, password: S) -> impl Future<Output = RedisResult<()>> + Send
  where
    S: Into<Str> + Send,
  {
    async move {
      into!(password);
      commands::server::auth(self, username, password).await
    }
  }

  /// Switch to a different protocol, optionally authenticating in the process.
  ///
  /// If running against clustered servers this function will issue the HELLO command to each server concurrently.
  ///
  /// <https://redis.io/commands/hello>
  fn hello(
    &self,
    version: RespVersion,
    auth: Option<(Str, Str)>,
    setname: Option<Str>,
  ) -> impl Future<Output = RedisResult<()>> + Send {
    async move { commands::server::hello(self, version, auth, setname).await }
  }
}

/// An interface that exposes various client and connection events.
///
/// Calling [quit](crate::interfaces::ClientLike::quit) will close all event streams.
#[rm_send_if(feature = "glommio")]
pub trait EventInterface: ClientLike {
  /// Spawn a task that runs the provided function on each publish-subscribe message.
  ///
  /// See [message_rx](Self::message_rx) for more information.
  fn on_message<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Message) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.message_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each keyspace event.
  ///
  /// <https://redis.io/topics/notifications>
  fn on_keyspace_event<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(KeyspaceEvent) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.keyspace_event_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each reconnection event.
  ///
  /// Errors returned by `func` will exit the task.
  fn on_reconnect<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Server) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.reconnect_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each cluster change event.
  ///
  /// Errors returned by `func` will exit the task.
  fn on_cluster_change<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Vec<ClusterStateChange>) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.cluster_change_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each connection error event.
  ///
  /// Errors returned by `func` will exit the task.
  fn on_error<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(RedisError) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.error_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function whenever the client detects an unresponsive connection.
  fn on_unresponsive<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Server) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.unresponsive_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn one task that listens for all connection management event types.
  ///
  /// Errors in any of the provided functions will exit the task.
  fn on_any<Fe, Fr, Fc>(&self, error_fn: Fe, reconnect_fn: Fr, cluster_change_fn: Fc) -> JoinHandle<RedisResult<()>>
  where
    Fe: Fn(RedisError) -> RedisResult<()> + Send + 'static,
    Fr: Fn(Server) -> RedisResult<()> + Send + 'static,
    Fc: Fn(Vec<ClusterStateChange>) -> RedisResult<()> + Send + 'static,
  {
    let mut error_rx = self.error_rx();
    let mut reconnect_rx = self.reconnect_rx();
    let mut cluster_rx = self.cluster_change_rx();

    spawn(async move {
      #[allow(unused_assignments)]
      let mut result = Ok(());

      loop {
        tokio::select! {
          Ok(error) = error_rx.recv() => {
            if let Err(err) = error_fn(error) {
              result = Err(err);
              break;
            }
          }
          Ok(server) = reconnect_rx.recv() => {
            if let Err(err) = reconnect_fn(server) {
              result = Err(err);
              break;
            }
          }
          Ok(changes) = cluster_rx.recv() => {
            if let Err(err) = cluster_change_fn(changes) {
              result = Err(err);
              break;
            }
          }
        }
      }

      result
    })
  }

  /// Listen for messages on the publish-subscribe interface.
  ///
  /// **Keyspace events are not sent on this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  /// Messages will start appearing on the original stream after
  /// [subscribe](crate::interfaces::PubsubInterface::subscribe) is called again.
  fn message_rx(&self) -> BroadcastReceiver<Message> {
    self.inner().notifications.pubsub.load().subscribe()
  }

  /// Listen for keyspace and keyevent notifications on the publish-subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will
  /// parse and format the messages automatically.
  ///
  /// <https://redis.io/topics/notifications>
  fn keyspace_event_rx(&self) -> BroadcastReceiver<KeyspaceEvent> {
    self.inner().notifications.keyspace.load().subscribe()
  }

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client reconnects in order to
  /// re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  fn reconnect_rx(&self) -> BroadcastReceiver<Server> {
    self.inner().notifications.reconnect.load().subscribe()
  }

  /// Listen for notifications whenever the cluster state changes.
  ///
  /// This is usually triggered in response to a `MOVED` error, but can also happen when connections close
  /// unexpectedly.
  fn cluster_change_rx(&self) -> BroadcastReceiver<Vec<ClusterStateChange>> {
    self.inner().notifications.cluster_change.load().subscribe()
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  fn error_rx(&self) -> BroadcastReceiver<RedisError> {
    self.inner().notifications.errors.load().subscribe()
  }

  /// Receive a message when the client initiates a reconnection after detecting an unresponsive connection.
  fn unresponsive_rx(&self) -> BroadcastReceiver<Server> {
    self.inner().notifications.unresponsive.load().subscribe()
  }
}

#[cfg(feature = "i-acl")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-acl")))]
pub use crate::commands::interfaces::acl::*;
#[cfg(feature = "i-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-client")))]
pub use crate::commands::interfaces::client::*;
#[cfg(feature = "i-cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-cluster")))]
pub use crate::commands::interfaces::cluster::*;
#[cfg(feature = "i-config")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-config")))]
pub use crate::commands::interfaces::config::*;
#[cfg(feature = "i-geo")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-geo")))]
pub use crate::commands::interfaces::geo::*;
#[cfg(feature = "i-hashes")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hashes")))]
pub use crate::commands::interfaces::hashes::*;
#[cfg(feature = "i-hyperloglog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hyperloglog")))]
pub use crate::commands::interfaces::hyperloglog::*;
#[cfg(feature = "i-keys")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-keys")))]
pub use crate::commands::interfaces::keys::*;
#[cfg(feature = "i-lists")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-lists")))]
pub use crate::commands::interfaces::lists::*;
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
pub use crate::commands::interfaces::lua::*;
#[cfg(feature = "i-memory")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-memory")))]
pub use crate::commands::interfaces::memory::*;
#[cfg(feature = "i-pubsub")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-pubsub")))]
pub use crate::commands::interfaces::pubsub::*;
#[cfg(feature = "i-redis-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redis-json")))]
pub use crate::commands::interfaces::redis_json::RedisJsonInterface;
#[cfg(feature = "i-redisearch")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redisearch")))]
pub use crate::commands::interfaces::redisearch::*;
#[cfg(feature = "sentinel-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
pub use crate::commands::interfaces::sentinel::SentinelInterface;
#[cfg(feature = "i-server")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-server")))]
pub use crate::commands::interfaces::server::*;
#[cfg(feature = "i-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sets")))]
pub use crate::commands::interfaces::sets::*;
#[cfg(feature = "i-slowlog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-slowlog")))]
pub use crate::commands::interfaces::slowlog::*;
#[cfg(feature = "i-sorted-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sorted-sets")))]
pub use crate::commands::interfaces::sorted_sets::*;
#[cfg(feature = "i-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-streams")))]
pub use crate::commands::interfaces::streams::*;
#[cfg(feature = "i-time-series")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-time-series")))]
pub use crate::commands::interfaces::timeseries::*;
#[cfg(feature = "i-tracking")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-tracking")))]
pub use crate::commands::interfaces::tracking::*;
#[cfg(feature = "transactions")]
#[cfg_attr(docsrs, doc(cfg(feature = "transactions")))]
pub use crate::commands::interfaces::transactions::*;

pub use crate::commands::interfaces::metrics::MetricsInterface;
