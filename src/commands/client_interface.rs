use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{
  ClientKillFilter, ClientKillType, ClientPauseKind, ClientReplyFlag, ClientUnblockFlag, RedisKey, RedisResponse,
  RedisValue,
};
use crate::utils;
use std::collections::HashMap;
use std::sync::Arc;

/// Functions that implement the [CLIENT](https://redis.io/commands#connection) interface.
pub trait ClientInterface: ClientLike + Sized {
  /// Return the ID of the current connection.
  ///
  /// Note: Against a clustered deployment this will return the ID of a random connection. See [connection_ids](Self::connection_ids) for  more information.
  ///
  /// <https://redis.io/commands/client-id>
  async fn client_id<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_id(&self.inner).await?.convert()
  }

  /// Read the connection IDs for the active connections to each server.
  ///
  /// The returned map contains each server's `host:port` and the result of calling `CLIENT ID` on the connection.
  ///
  /// Note: despite being async this function will usually return cached information from the client if possible.
  async fn connection_ids(&self) -> Result<HashMap<Arc<String>, i64>, RedisError> {
    utils::read_connection_ids(&self.inner).await.ok_or(RedisError::new(
      RedisErrorKind::Unknown,
      "Failed to read connection IDs",
    ))
  }

  /// Update the client's sentinel nodes list if using the sentinel interface.
  ///
  /// The client will automatically update this when connections to the primary server close.
  async fn update_sentinel_nodes(&self) -> Result<(), RedisError> {
    utils::update_sentinel_nodes(&self.inner).await
  }

  /// The command returns information and statistics about the current client connection in a mostly human readable format.
  ///
  /// <https://redis.io/commands/client-info>
  async fn client_info<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_info(&self.inner).await?.convert()
  }

  /// Close a given connection or set of connections.
  ///
  /// <https://redis.io/commands/client-kill>
  async fn client_kill<R>(&self, filters: Vec<ClientKillFilter>) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_kill(&self.inner, filters).await?.convert()
  }

  /// The CLIENT LIST command returns information and statistics about the client connections server in a mostly human readable format.
  ///
  /// <https://redis.io/commands/client-list>
  async fn client_list<R, I>(&self, r#type: Option<ClientKillType>, ids: Option<Vec<I>>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    I: Into<RedisKey>,
  {
    commands::client::client_list(&self.inner, r#type, ids).await?.convert()
  }

  /// The CLIENT GETNAME returns the name of the current connection as set by CLIENT SETNAME.
  ///
  /// <https://redis.io/commands/client-getname>
  async fn client_getname<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_getname(&self.inner).await?.convert()
  }

  /// Assign a name to the current connection.
  ///
  /// **Note: The client automatically generates a unique name for each client that is shared by all underlying connections.
  /// Use [Self::id] to read the automatically generated name.**
  ///
  /// <https://redis.io/commands/client-setname>
  async fn client_setname<S>(&self, name: S) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_setname(&self.inner, name).await
  }

  /// CLIENT PAUSE is a connections control command able to suspend all the Redis clients for the specified amount of time (in milliseconds).
  ///
  /// <https://redis.io/commands/client-pause>
  async fn client_pause(&self, timeout: i64, mode: Option<ClientPauseKind>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_pause(&self.inner, timeout, mode).await
  }

  /// CLIENT UNPAUSE is used to resume command processing for all clients that were paused by CLIENT PAUSE.
  ///
  /// <https://redis.io/commands/client-unpause>
  async fn client_unpause(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_unpause(&self.inner).await
  }

  /// The CLIENT REPLY command controls whether the server will reply the client's commands. The following modes are available:
  ///
  /// <https://redis.io/commands/client-reply>
  async fn client_reply(&self, flag: ClientReplyFlag) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_reply(&self.inner, flag).await
  }

  /// This command can unblock, from a different connection, a client blocked in a blocking operation, such as for instance BRPOP or XREAD or WAIT.
  ///
  /// Note: this command is sent on a backchannel connection and will work even when the main connection is blocked.
  ///
  /// <https://redis.io/commands/client-unblock>
  async fn client_unblock<R, S>(&self, id: S, flag: Option<ClientUnblockFlag>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisValue>,
  {
    commands::client::client_unblock(&self.inner, id, flag).await?.convert()
  }

  /// A convenience function to unblock any blocked connection on this client.
  async fn unblock_self(&self, flag: Option<ClientUnblockFlag>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::unblock_self(&self.inner, flag).await
  }
}
