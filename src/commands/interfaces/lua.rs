use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FnPolicy, FromRedis, MultipleKeys, MultipleStrings, MultipleValues, ScriptDebugFlag},
};
use bytes::Bytes;
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

/// Functions implementing the [function interface](https://redis.io/docs/manual/programmability/functions-intro/).
#[async_trait]
pub trait FunctionInterface: ClientLike + Sized {
  /// Invoke a function.
  ///
  /// <https://redis.io/commands/fcall/>
  async fn fcall<R, F, K, V>(&self, func: F, keys: K, args: V) -> RedisResult<R>
  where
    R: FromRedis,
    F: Into<Str> + Send,
    K: Into<MultipleKeys> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(func);
    try_into!(keys, args);
    commands::lua::fcall(self, func, keys, args).await?.convert()
  }

  /// This is a read-only variant of the FCALL command that cannot execute commands that modify data.
  ///
  /// <https://redis.io/commands/fcall_ro/>
  async fn fcall_ro<R, F, K, V>(&self, func: F, keys: K, args: V) -> RedisResult<R>
  where
    R: FromRedis,
    F: Into<Str> + Send,
    K: Into<MultipleKeys> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(func);
    try_into!(keys, args);
    commands::lua::fcall_ro(self, func, keys, args).await?.convert()
  }

  /// Delete a library and all its functions.
  ///
  /// <https://redis.io/commands/function-delete/>
  async fn function_delete<R, S>(&self, library_name: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    into!(library_name);
    commands::lua::function_delete(self, library_name).await?.convert()
  }

  /// Delete a library and all its functions from each cluster node concurrently.
  ///
  /// <https://redis.io/commands/function-delete/>
  async fn function_delete_cluster<S>(&self, library_name: S) -> RedisResult<()>
  where
    S: Into<Str> + Send,
  {
    into!(library_name);
    commands::lua::function_delete_cluster(self, library_name).await
  }

  /// Return the serialized payload of loaded libraries.
  ///
  /// <https://redis.io/commands/function-dump/>
  async fn function_dump<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::lua::function_dump(self).await?.convert()
  }

  /// Deletes all the libraries.
  ///
  /// <https://redis.io/commands/function-flush/>
  async fn function_flush<R>(&self, r#async: bool) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::lua::function_flush(self, r#async).await?.convert()
  }

  /// Deletes all the libraries on all cluster nodes concurrently.
  ///
  /// <https://redis.io/commands/function-flush/>
  async fn function_flush_cluster(&self, r#async: bool) -> RedisResult<()> {
    commands::lua::function_flush_cluster(self, r#async).await
  }

  /// Kill a function that is currently executing.
  ///
  /// Note: This command runs on a backchannel connection to the server in order to take effect as quickly as
  /// possible.
  ///
  /// <https://redis.io/commands/function-kill/>
  async fn function_kill<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::lua::function_kill(self).await?.convert()
  }

  /// Return information about the functions and libraries.
  ///
  /// <https://redis.io/commands/function-list/>
  async fn function_list<R, S>(&self, library_name: Option<S>, withcode: bool) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    let library_name = library_name.map(|l| l.into());
    commands::lua::function_list(self, library_name, withcode)
      .await?
      .convert()
  }

  /// Load a library to Redis.
  ///
  /// <https://redis.io/commands/function-load/>
  async fn function_load<R, S>(&self, replace: bool, code: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    into!(code);
    commands::lua::function_load(self, replace, code).await?.convert()
  }

  /// Load a library to Redis on all cluster nodes concurrently.
  ///
  /// <https://redis.io/commands/function-load/>
  async fn function_load_cluster<S>(&self, replace: bool, code: S) -> RedisResult<()>
  where
    S: Into<Str> + Send,
  {
    into!(code);
    commands::lua::function_load_cluster(self, replace, code).await
  }

  /// Restore libraries from the serialized payload.
  ///
  /// <https://redis.io/commands/function-restore/>
  ///
  /// Note: Use `FnPolicy::default()` to use the default function restore policy (`"APPEND"`).
  async fn function_restore<R, B, P>(&self, serialized: B, policy: P) -> RedisResult<R>
  where
    R: FromRedis,
    B: Into<Bytes> + Send,
    P: TryInto<FnPolicy> + Send,
    P::Error: Into<RedisError> + Send,
  {
    into!(serialized);
    try_into!(policy);
    commands::lua::function_restore(self, serialized, policy)
      .await?
      .convert()
  }

  /// Restore libraries from the serialized payload on all cluster nodes concurrently.
  ///
  /// <https://redis.io/commands/function-restore/>
  ///
  /// Note: Use `FnPolicy::default()` to use the default function restore policy (`"APPEND"`).
  async fn function_restore_cluster<B, P>(&self, serialized: B, policy: P) -> RedisResult<()>
  where
    B: Into<Bytes> + Send,
    P: TryInto<FnPolicy> + Send,
    P::Error: Into<RedisError> + Send,
  {
    into!(serialized);
    try_into!(policy);
    commands::lua::function_restore_cluster(self, serialized, policy).await
  }

  /// Return information about the function that's currently running and information about the available execution
  /// engines.
  ///
  /// Note: This command runs on a backchannel connection to the server.
  ///
  /// <https://redis.io/commands/function-stats/>
  async fn function_stats<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::lua::function_stats(self).await?.convert()
  }
}
