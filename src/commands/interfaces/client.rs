use crate::{
  commands,
  interfaces::{ClientLike, RedisResult},
  types::{
    ClientKillFilter,
    ClientKillType,
    ClientPauseKind,
    ClientReplyFlag,
    ClientUnblockFlag,
    FromRedis,
    RedisValue,
    Server,
  },
};
use bytes_utils::Str;
use std::collections::HashMap;

#[cfg(feature = "client-tracking")]
use crate::{
  error::{RedisError, RedisErrorKind},
  types::{MultipleStrings, Toggle},
};

/// Functions that implement the [client](https://redis.io/commands#connection) interface.
#[async_trait]
pub trait ClientInterface: ClientLike + Sized {
  /// Return the ID of the current connection.
  ///
  /// Note: Against a clustered deployment this will return the ID of a random connection. See
  /// [connection_ids](Self::connection_ids) for  more information.
  ///
  /// <https://redis.io/commands/client-id>
  async fn client_id<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::client::client_id(self).await?.convert()
  }

  /// Read the connection IDs for the active connections to each server.
  ///
  /// The returned map contains each server's `host:port` and the result of calling `CLIENT ID` on the connection.
  ///
  /// Note: despite being async this function will return cached information from the client if possible.
  async fn connection_ids(&self) -> HashMap<Server, i64> {
    self.inner().backchannel.read().await.connection_ids.clone()
  }

  /// The command returns information and statistics about the current client connection in a mostly human readable
  /// format.
  ///
  /// <https://redis.io/commands/client-info>
  async fn client_info<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::client::client_info(self).await?.convert()
  }

  /// Close a given connection or set of connections.
  ///
  /// <https://redis.io/commands/client-kill>
  async fn client_kill<R>(&self, filters: Vec<ClientKillFilter>) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::client::client_kill(self, filters).await?.convert()
  }

  /// The CLIENT LIST command returns information and statistics about the client connections server in a mostly human
  /// readable format.
  ///
  /// <https://redis.io/commands/client-list>
  async fn client_list<R, I>(&self, r#type: Option<ClientKillType>, ids: Option<Vec<String>>) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::client::client_list(self, r#type, ids).await?.convert()
  }

  /// The CLIENT GETNAME returns the name of the current connection as set by CLIENT SETNAME.
  ///
  /// <https://redis.io/commands/client-getname>
  async fn client_getname<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::client::client_getname(self).await?.convert()
  }

  /// Assign a name to the current connection.
  ///
  /// **Note: The client automatically generates a unique name for each client that is shared by all underlying
  /// connections. Use `self.id() to read the automatically generated name.**
  ///
  /// <https://redis.io/commands/client-setname>
  async fn client_setname<S>(&self, name: S) -> RedisResult<()>
  where
    S: Into<Str> + Send,
  {
    into!(name);
    commands::client::client_setname(self, name).await
  }

  /// CLIENT PAUSE is a connections control command able to suspend all the Redis clients for the specified amount of
  /// time (in milliseconds).
  ///
  /// <https://redis.io/commands/client-pause>
  async fn client_pause(&self, timeout: i64, mode: Option<ClientPauseKind>) -> RedisResult<()> {
    commands::client::client_pause(self, timeout, mode).await
  }

  /// CLIENT UNPAUSE is used to resume command processing for all clients that were paused by CLIENT PAUSE.
  ///
  /// <https://redis.io/commands/client-unpause>
  async fn client_unpause(&self) -> RedisResult<()> {
    commands::client::client_unpause(self).await
  }

  /// The CLIENT REPLY command controls whether the server will reply the client's commands. The following modes are
  /// available:
  ///
  /// <https://redis.io/commands/client-reply>
  async fn client_reply(&self, flag: ClientReplyFlag) -> RedisResult<()> {
    commands::client::client_reply(self, flag).await
  }

  /// This command can unblock, from a different connection, a client blocked in a blocking operation, such as for
  /// instance BRPOP or XREAD or WAIT.
  ///
  /// Note: this command is sent on a backchannel connection and will work even when the main connection is blocked.
  ///
  /// <https://redis.io/commands/client-unblock>
  async fn client_unblock<R, S>(&self, id: S, flag: Option<ClientUnblockFlag>) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<RedisValue> + Send,
  {
    into!(id);
    commands::client::client_unblock(self, id, flag).await?.convert()
  }

  /// A convenience function to unblock any blocked connection on this client.
  async fn unblock_self(&self, flag: Option<ClientUnblockFlag>) -> RedisResult<()> {
    commands::client::unblock_self(self, flag).await
  }

  /// This command enables the tracking feature of the Redis server that is used for server assisted client side
  /// caching.
  ///
  /// <https://redis.io/commands/client-tracking/>
  ///
  /// Note: **This function requires a centralized server**. See
  /// [crate::interfaces::TrackingInterface::start_tracking] for a version that works with all server deployment
  /// modes.
  #[cfg(feature = "client-tracking")]
  #[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
  async fn client_tracking<R, T, P>(
    &self,
    toggle: T,
    redirect: Option<i64>,
    prefixes: P,
    bcast: bool,
    optin: bool,
    optout: bool,
    noloop: bool,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    T: TryInto<Toggle> + Send,
    T::Error: Into<RedisError> + Send,
    P: Into<MultipleStrings> + Send,
  {
    if self.inner().config.server.is_clustered() {
      return Err(RedisError::new(
        RedisErrorKind::Config,
        "Invalid server type. Expected centralized server.",
      ));
    }

    try_into!(toggle);
    into!(prefixes);
    commands::tracking::client_tracking(self, toggle, redirect, prefixes, bcast, optin, optout, noloop)
      .await?
      .convert()
  }

  /// The command returns information about the current client connection's use of the server assisted client side
  /// caching feature.
  ///
  /// <https://redis.io/commands/client-trackinginfo/>
  #[cfg(feature = "client-tracking")]
  #[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
  async fn client_trackinginfo<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::tracking::client_trackinginfo(self).await?.convert()
  }

  /// This command returns the client ID we are redirecting our tracking notifications to.
  ///
  /// <https://redis.io/commands/client-getredir/>
  #[cfg(feature = "client-tracking")]
  #[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
  async fn client_getredir<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::tracking::client_getredir(self).await?.convert()
  }

  /// This command controls the tracking of the keys in the next command executed by the connection, when tracking is
  /// enabled in OPTIN or OPTOUT mode.
  ///
  /// <https://redis.io/commands/client-caching/>
  ///
  /// Note: **This function requires a centralized server**. See
  /// [TrackingInterface::caching](crate::interfaces::TrackingInterface::caching) for a version that works with all
  /// server deployment modes.
  #[cfg(feature = "client-tracking")]
  #[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
  async fn client_caching<R>(&self, enabled: bool) -> RedisResult<R>
  where
    R: FromRedis,
  {
    if self.inner().config.server.is_clustered() {
      return Err(RedisError::new(
        RedisErrorKind::Config,
        "Invalid server type. Expected centralized server.",
      ));
    }

    commands::tracking::client_caching(self, enabled).await?.convert()
  }
}
