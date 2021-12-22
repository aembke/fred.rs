use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{MultipleKeys, MultipleStrings, MultipleValues, RedisResponse, ScriptDebugFlag};
use crate::utils;
use std::convert::TryInto;

/// Functions that implement the [lua](https://redis.io/commands#lua) interface.
pub trait LuaInterface: ClientLike + Sized {
  /// Load a script into the scripts cache, without executing it. After the specified command is loaded into the script cache it will be callable using EVALSHA with the correct SHA1 digest of the script.
  ///
  /// <https://redis.io/commands/script-load>
  fn script_load<S>(&self, script: S) -> AsyncResult<String>
  where
    S: Into<String>,
  {
    into!(script);
    async_spawn(self, |inner| async move {
      commands::lua::script_load(&inner, script).await?.convert()
    })
  }

  /// A clustered variant of [script_load](Self::script_load) that loads the script on all primary nodes in a cluster.
  fn script_load_cluster<S>(&self, script: S) -> AsyncResult<String>
  where
    S: Into<String>,
  {
    into!(script);
    async_spawn(self, |inner| async move {
      commands::lua::script_load_cluster(&inner, script).await?.convert()
    })
  }

  /// Kills the currently executing Lua script, assuming no write operation was yet performed by the script.
  ///
  /// <https://redis.io/commands/script-kill>
  fn script_kill(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::script_kill(&inner).await
    })
  }

  /// A clustered variant of the [script_kill](Self::script_kill) command that issues the command to all primary nodes in the cluster.
  fn script_kill_cluster(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::script_kill_cluster(&inner).await
    })
  }

  /// Flush the Lua scripts cache.
  ///
  /// <https://redis.io/commands/script-flush>
  fn script_flush(&self, r#async: bool) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::script_flush(&inner, r#async).await
    })
  }

  /// A clustered variant of [script_flush](Self::script_flush) that flushes the script cache on all primary nodes in the cluster.
  fn script_flush_cluster(&self, r#async: bool) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::script_flush_cluster(&inner, r#async).await
    })
  }

  /// Returns information about the existence of the scripts in the script cache.
  ///
  /// <https://redis.io/commands/script-exists>
  fn script_exists<H>(&self, hashes: H) -> AsyncResult<Vec<bool>>
  where
    H: Into<MultipleStrings>,
  {
    into!(hashes);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::script_exists(&inner, hashes).await
    })
  }

  /// Set the debug mode for subsequent scripts executed with EVAL.
  ///
  /// <https://redis.io/commands/script-debug>
  fn script_debug(&self, flag: ScriptDebugFlag) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::script_debug(&inner, flag).await
    })
  }

  /// Evaluates a script cached on the server side by its SHA1 digest.
  ///
  /// <https://redis.io/commands/evalsha>
  ///
  /// **Note: Use `None` to represent an empty set of keys or args instead of `()`.**
  fn evalsha<R, S, K, V>(&self, hash: S, keys: K, args: V) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
    S: Into<String>,
    K: Into<MultipleKeys>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(hash, keys);
    try_into!(args);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::evalsha(&inner, hash, keys, args).await?.convert()
    })
  }

  /// Evaluate a Lua script on the server.
  ///
  /// <https://redis.io/commands/eval>
  ///
  /// **Note: Use `None` to represent an empty set of keys or args instead of `()`.**
  fn eval<R, S, K, V>(&self, script: S, keys: K, args: V) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
    S: Into<String>,
    K: Into<MultipleKeys>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(script, keys);
    try_into!(args);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::lua::eval(&inner, script, keys, args).await?.convert()
    })
  }
}
