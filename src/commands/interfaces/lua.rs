use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, MultipleKeys, MultipleStrings, MultipleValues, ScriptDebugFlag},
};
use bytes_utils::Str;
use std::convert::TryInto;

/// Functions that implement the [lua](https://redis.io/commands#lua) interface.
#[async_trait]
pub trait LuaInterface: ClientLike + Sized {
  /// Load a script into the scripts cache, without executing it. After the specified command is loaded into the
  /// script cache it will be callable using EVALSHA with the correct SHA1 digest of the script.
  ///
  /// <https://redis.io/commands/script-load>
  async fn script_load<S>(&self, script: S) -> RedisResult<String>
  where
    S: Into<Str> + Send,
  {
    into!(script);
    commands::lua::script_load(self, script).await?.convert()
  }

  /// A clustered variant of [script_load](Self::script_load) that loads the script on all primary nodes in a cluster.
  async fn script_load_cluster<S>(&self, script: S) -> RedisResult<String>
  where
    S: Into<Str> + Send,
  {
    into!(script);
    commands::lua::script_load_cluster(self, script).await?.convert()
  }

  /// Kills the currently executing Lua script, assuming no write operation was yet performed by the script.
  ///
  /// <https://redis.io/commands/script-kill>
  async fn script_kill(&self) -> RedisResult<()> {
    commands::lua::script_kill(self).await
  }

  /// A clustered variant of the [script_kill](Self::script_kill) command that issues the command to all primary nodes
  /// in the cluster.
  async fn script_kill_cluster(&self) -> RedisResult<()> {
    commands::lua::script_kill_cluster(self).await
  }

  /// Flush the Lua scripts cache.
  ///
  /// <https://redis.io/commands/script-flush>
  async fn script_flush(&self, r#async: bool) -> RedisResult<()> {
    commands::lua::script_flush(self, r#async).await
  }

  /// A clustered variant of [script_flush](Self::script_flush) that flushes the script cache on all primary nodes in
  /// the cluster.
  async fn script_flush_cluster(&self, r#async: bool) -> RedisResult<()> {
    commands::lua::script_flush_cluster(self, r#async).await
  }

  /// Returns information about the existence of the scripts in the script cache.
  ///
  /// <https://redis.io/commands/script-exists>
  async fn script_exists<R, H>(&self, hashes: H) -> RedisResult<R>
  where
    R: FromRedis,
    H: Into<MultipleStrings> + Send,
  {
    into!(hashes);
    commands::lua::script_exists(self, hashes).await?.convert()
  }

  /// Set the debug mode for subsequent scripts executed with EVAL.
  ///
  /// <https://redis.io/commands/script-debug>
  async fn script_debug(&self, flag: ScriptDebugFlag) -> RedisResult<()> {
    commands::lua::script_debug(self, flag).await
  }

  /// Evaluates a script cached on the server side by its SHA1 digest.
  ///
  /// <https://redis.io/commands/evalsha>
  ///
  /// **Note: Use `None` to represent an empty set of keys or args.**
  async fn evalsha<R, S, K, V>(&self, hash: S, keys: K, args: V) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
    K: Into<MultipleKeys> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(hash, keys);
    try_into!(args);
    commands::lua::evalsha(self, hash, keys, args).await?.convert()
  }

  /// Evaluate a Lua script on the server.
  ///
  /// <https://redis.io/commands/eval>
  ///
  /// **Note: Use `None` to represent an empty set of keys or args.**
  async fn eval<R, S, K, V>(&self, script: S, keys: K, args: V) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
    K: Into<MultipleKeys> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(script, keys);
    try_into!(args);
    commands::lua::eval(self, script, keys, args).await?.convert()
  }
}
