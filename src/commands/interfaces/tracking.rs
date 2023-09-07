use crate::{
  clients::Caching,
  commands,
  interfaces::ClientLike,
  prelude::RedisResult,
  types::{Invalidation, MultipleStrings},
};
use tokio::{sync::broadcast::Receiver as BroadcastReceiver, task::JoinHandle};

/// A high level interface that supports [client side caching](https://redis.io/docs/manual/client-side-caching/) via the [client tracking](https://redis.io/commands/client-tracking/) interface.
#[async_trait]
#[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
pub trait TrackingInterface: ClientLike + Sized {
  /// Send the [CLIENT TRACKING](https://redis.io/commands/client-tracking/) command to all connected servers, subscribing to [invalidation messages](Self::on_invalidation) on the same connection.
  ///
  /// This interface requires the RESP3 protocol mode and supports all server deployment types (centralized,
  /// clustered, and sentinel).
  ///
  /// See the basic [client tracking](crate::interfaces::ClientInterface::client_tracking) function for more
  /// information on the underlying commands.
  async fn start_tracking<P>(
    &self,
    prefixes: P,
    bcast: bool,
    optin: bool,
    optout: bool,
    noloop: bool,
  ) -> RedisResult<()>
  where
    P: Into<MultipleStrings> + Send,
  {
    into!(prefixes);
    commands::tracking::start_tracking(self, prefixes, bcast, optin, optout, noloop).await
  }

  /// Disable client tracking on all connections.
  async fn stop_tracking(&self) -> RedisResult<()> {
    commands::tracking::stop_tracking(self).await
  }

  /// Spawn a task that processes invalidation messages from the server.
  ///
  /// See [invalidation_rx](Self::invalidation_rx) for a more flexible variation of this function.
  fn on_invalidation<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Invalidation) -> RedisResult<()> + Send + 'static,
  {
    let mut invalidation_rx = self.invalidation_rx();

    tokio::spawn(async move {
      let mut result = Ok(());

      while let Ok(invalidation) = invalidation_rx.recv().await {
        if let Err(err) = func(invalidation) {
          result = Err(err);
          break;
        }
      }
      result
    })
  }

  /// Subscribe to invalidation messages from the server(s).
  fn invalidation_rx(&self) -> BroadcastReceiver<Invalidation> {
    self.inner().notifications.invalidations.load().subscribe()
  }

  /// Send a `CLIENT CACHING yes|no` command before each subsequent command.
  fn caching(&self, enabled: bool) -> Caching {
    Caching::new(self.inner(), enabled)
  }
}
