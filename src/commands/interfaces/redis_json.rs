use crate::{
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, MultipleKeys, MultipleStrings, MultipleValues, RedisKey, RedisValue, SetOptions},
};
use bytes_utils::Str;
use serde_json::Value;

/// The client commands in the [RedisJSON](https://redis.io/docs/data-types/json/) interface.
#[async_trait]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-json")))]
pub trait RedisJsonInterface: ClientLike + Sized {
  /// Append the json values into the array at path after the last element in it.
  ///
  /// <https://redis.io/commands/json.arrappend>
  async fn json_arrappend<R, K, P, V>(&self, key: K, path: Option<P>, values: Vec<V>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Search for the first occurrence of a JSON value in an array.
  ///
  /// <https://redis.io/commands/json.arrindex/>
  async fn json_arrindex<R, K, P, V>(
    &self,
    key: K,
    path: P,
    value: V,
    start: Option<i64>,
    stop: Option<i64>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Insert the json values into the array at path before the index (shifts to the right).
  ///
  /// <https://redis.io/commands/json.arrinsert/>
  async fn json_arrinsert<R, K, P, V>(&self, key: K, path: P, index: i64, values: Vec<V>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Report the length of the JSON array at path in key.
  ///
  /// <https://redis.io/commands/json.arrlen/>
  async fn json_arrlen<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Remove and return an element from the index in the array
  ///
  /// <https://redis.io/commands/json.arrpop/>
  async fn json_arrpop<R, K, P>(&self, key: K, path: Option<P>, index: Option<i64>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Trim an array so that it contains only the specified inclusive range of elements
  ///
  /// <https://redis.io/commands/json.arrtrim/>
  async fn json_arrtrim<R, K, P>(&self, key: K, path: P, start: i64, stop: i64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Clear container values (arrays/objects) and set numeric values to 0
  ///
  /// <https://redis.io/commands/json.clear/>
  async fn json_clear<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Report a value's memory usage in bytes
  ///
  /// <https://redis.io/commands/json.debug-memory/>
  async fn json_debug_memory<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Delete a value.
  ///
  /// <https://redis.io/commands/json.del/>
  async fn json_del<R, K, P>(&self, key: K, path: P) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Return the value at path in JSON serialized form.
  ///
  /// <https://redis.io/commands/json.get/>
  async fn json_get<R, K, I, N, S, P>(
    &self,
    key: K,
    indent: Option<I>,
    newline: Option<N>,
    space: Option<S>,
    paths: P,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    I: Into<Str> + Send,
    N: Into<Str> + Send,
    S: Into<Str> + Send,
    P: Into<MultipleStrings> + Send,
  {
    unimplemented!()
  }

  /// Merge a given JSON value into matching paths.
  ///
  /// <https://redis.io/commands/json.merge/>
  async fn json_merge<R, K, P, V>(&self, key: K, path: P, value: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Return the values at path from multiple key arguments.
  ///
  /// <https://redis.io/commands/json.mget/>
  async fn json_mget<R, K, P>(&self, keys: K, path: P) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Set or update one or more JSON values according to the specified key-path-value triplets.
  ///
  /// <https://redis.io/commands/json.mset/>
  async fn json_mset<R, K, P, V>(&self, values: Vec<(K, P, V)>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Increment the number value stored at path by number
  ///
  /// <https://redis.io/commands/json.numincrby/>
  async fn json_numincrby<R, K, P, V>(&self, key: K, path: P, value: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Return the keys in the object that's referenced by path.
  ///
  /// <https://redis.io/commands/json.objkeys/>
  async fn json_objkeys<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Report the number of keys in the JSON object at path in key.
  ///
  /// <https://redis.io/commands/json.objlen/>
  async fn json_objlen<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Return the JSON in key in Redis serialization protocol specification form.
  ///
  /// <https://redis.io/commands/json.resp/>
  async fn json_resp<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Set the JSON value at path in key.
  ///
  /// <https://redis.io/commands/json.set/>
  async fn json_set<R, K, P, V>(&self, key: K, path: P, value: V, options: Option<SetOptions>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Append the json-string values to the string at path.
  ///
  /// <https://redis.io/commands/json.strappend/>
  async fn json_strappend<R, K, P, V>(&self, key: K, path: Option<P>, value: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
    V: Into<Value> + Send,
  {
    unimplemented!()
  }

  /// Report the length of the JSON String at path in key.
  ///
  /// <https://redis.io/commands/json.strlen/>
  async fn json_strlen<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Toggle a Boolean value stored at path.
  ///
  /// <https://redis.io/commands/json.toggle/>
  async fn json_toggle<R, K, P>(&self, key: K, path: P) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Report the type of JSON value at path.
  ///
  /// <https://redis.io/commands/json.type/>
  async fn json_type<R, K, P>(&self, key: K, path: Option<P>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    P: Into<Str> + Send,
  {
    unimplemented!()
  }
}
