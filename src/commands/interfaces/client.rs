use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{
  ClientKillFilter, ClientKillType, ClientPauseKind, ClientReplyFlag, ClientUnblockFlag, FromRedis, RedisValue,
};
use crate::utils;
use bytes_utils::Str;
use std::collections::HashMap;
use std::sync::Arc;

/// Functions that implement the [CLIENT](https://redis.io/commands#connection) interface.
pub trait ClientInterface: ClientLike + Sized {
  /// Return the ID of the current connection.
  ///
  /// Note: Against a clustered deployment this will return the ID of a random connection. See [connection_ids](Self::connection_ids) for  more information.
  ///
  /// <https://redis.io/commands/client-id>
  fn client_id<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::client::client_id(&inner).await?.convert()
    })
  }

  /// Read the connection IDs for the active connections to each server.
  ///
  /// The returned map contains each server's `host:port` and the result of calling `CLIENT ID` on the connection.
  ///
  /// Note: despite being async this function will usually return cached information from the client if possible.
  fn connection_ids(&self) -> AsyncResult<HashMap<Arc<String>, i64>> {
    async_spawn(self, |inner| async move {
      utils::read_connection_ids(&inner).await.ok_or(RedisError::new(
        RedisErrorKind::Unknown,
        "Failed to read connection IDs",
      ))
    })
  }

  /// Force update the client's sentinel nodes list if using the sentinel interface.
  ///
  /// The client will automatically update this when connections to the primary server close.
  fn update_sentinel_nodes(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move { utils::update_sentinel_nodes(&inner).await })
  }

  /// The command returns information and statistics about the current client connection in a mostly human readable format.
  ///
  /// <https://redis.io/commands/client-info>
  fn client_info<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::client::client_info(&inner).await?.convert()
    })
  }

  /// Close a given connection or set of connections.
  ///
  /// <https://redis.io/commands/client-kill>
  fn client_kill<R>(&self, filters: Vec<ClientKillFilter>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::client::client_kill(&inner, filters).await?.convert()
    })
  }

  /// The CLIENT LIST command returns information and statistics about the client connections server in a mostly human readable format.
  ///
  /// <https://redis.io/commands/client-list>
  fn client_list<R, I>(&self, r#type: Option<ClientKillType>, ids: Option<Vec<String>>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::client::client_list(&inner, r#type, ids).await?.convert()
    })
  }

  /// The CLIENT GETNAME returns the name of the current connection as set by CLIENT SETNAME.
  ///
  /// <https://redis.io/commands/client-getname>
  fn client_getname<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::client::client_getname(&inner).await?.convert()
    })
  }

  /// Assign a name to the current connection.
  ///
  /// **Note: The client automatically generates a unique name for each client that is shared by all underlying connections.
  /// Use `self.id() to read the automatically generated name.**
  ///
  /// <https://redis.io/commands/client-setname>
  fn client_setname<S>(&self, name: S) -> AsyncResult<()>
  where
    S: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::client::client_setname(&inner, name).await
    })
  }

  /// CLIENT PAUSE is a connections control command able to suspend all the Redis clients for the specified amount of time (in milliseconds).
  ///
  /// <https://redis.io/commands/client-pause>
  fn client_pause(&self, timeout: i64, mode: Option<ClientPauseKind>) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      commands::client::client_pause(&inner, timeout, mode).await
    })
  }

  /// CLIENT UNPAUSE is used to resume command processing for all clients that were paused by CLIENT PAUSE.
  ///
  /// <https://redis.io/commands/client-unpause>
  fn client_unpause(&self) -> AsyncResult<()> {
    async_spawn(
      self,
      |inner| async move { commands::client::client_unpause(&inner).await },
    )
  }

  /// The CLIENT REPLY command controls whether the server will reply the client's commands. The following modes are available:
  ///
  /// <https://redis.io/commands/client-reply>
  fn client_reply(&self, flag: ClientReplyFlag) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      commands::client::client_reply(&inner, flag).await
    })
  }

  /// This command can unblock, from a different connection, a client blocked in a blocking operation, such as for instance BRPOP or XREAD or WAIT.
  ///
  /// Note: this command is sent on a backchannel connection and will work even when the main connection is blocked.
  ///
  /// <https://redis.io/commands/client-unblock>
  fn client_unblock<R, S>(&self, id: S, flag: Option<ClientUnblockFlag>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisValue>,
  {
    into!(id);
    async_spawn(self, |inner| async move {
      commands::client::client_unblock(&inner, id, flag).await?.convert()
    })
  }

  /// A convenience function to unblock any blocked connection on this client.
  fn unblock_self(&self, flag: Option<ClientUnblockFlag>) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::client::unblock_self(&inner, flag).await
    })
  }
}
