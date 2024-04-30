use crate::{
  interfaces::{ClientLike, RedisResult},
  prelude::RedisError,
  types::{
    Apply,
    FromRedis,
    Limit,
    Load,
    MultipleStrings,
    Parameter,
    RedisValue,
    Reducer,
    SortByProperty,
    WithCursor,
  },
};
use bytes_utils::Str;
use std::future::Future;

#[cfg_attr(docsrs, doc(cfg(feature = "i-redisearch")))]
pub trait RediSearchInterface: ClientLike + Sized {
  /// Returns a list of all existing indexes.
  ///
  /// <https://redis.io/docs/latest/commands/ft._list/>
  fn ft_list<R>(&self) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
  {
    unimplemented!()
  }

  /// Run a search query on an index, and perform aggregate transformations on the results.
  ///
  /// <https://redis.io/docs/latest/commands/ft.aggregate/>
  fn ft_aggregate<R, I, Q, L, G, F>(
    &self,
    index: I,
    query: Q,
    verbatim: bool,
    load: Option<Load>,
    timeout: Option<i64>,
    group_by: G,
    reduce: Vec<Reducer>,
    sort_by: Option<SortByProperty>,
    apply: Vec<Apply>,
    limit: Option<Limit>,
    filter: Option<F>,
    cursor: Option<WithCursor>,
    params: Vec<Parameter>,
    dialect: Option<i64>,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    I: Into<Str> + Send,
    Q: Into<Str> + Send,
    L: Into<MultipleStrings> + Send,
    G: Into<MultipleStrings> + Send,
    F: Into<Str> + Send,
  {
    unimplemented!()
  }

  // TODO alter, create,

  /// Add an alias to an index.
  ///
  /// <https://redis.io/docs/latest/commands/ft.aliasadd/>
  fn ft_aliasadd<R, A, I>(&self, alias: A, index: I) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    A: Into<Str> + Send,
    I: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Remove an alias from an index.
  ///
  /// <https://redis.io/docs/latest/commands/ft.aliasdel/>
  fn ft_aliasdel<R, A>(&self, alias: A) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    A: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Add an alias to an index. If the alias is already associated with another index, FT.ALIASUPDATE removes the
  /// alias association with the previous index.
  ///
  /// <https://redis.io/docs/latest/commands/ft.aliasupdate/>
  fn ft_aliasupdate<R, A, I>(&self, alias: A, index: I) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    A: Into<Str> + Send,
    I: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Retrieve configuration options.
  ///
  /// <https://redis.io/docs/latest/commands/ft.config-get/>
  fn ft_config_get<R, S>(&self, option: S) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Set the value of a RediSearch configuration parameter.
  ///
  /// <https://redis.io/docs/latest/commands/ft.config-set/>
  fn ft_config_set<R, S, V>(&self, option: S, value: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    S: Into<Str> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    unimplemented!()
  }

  /// Delete a cursor.
  ///
  /// <https://redis.io/docs/latest/commands/ft.cursor-del/>
  fn ft_cursor_del<R, I, C>(&self, index: I, cursor: C) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    I: Into<Str> + Send,
    C: TryInto<RedisValue> + Send,
    C::Error: Into<RedisError> + Send,
  {
    unimplemented!()
  }

  /// Read next results from an existing cursor.
  ///
  /// <https://redis.io/docs/latest/commands/ft.cursor-read/>
  fn ft_cursor_read<R, I, C>(
    &self,
    index: I,
    cursor: C,
    count: Option<u64>,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    I: Into<Str> + Send,
    C: TryInto<RedisValue> + Send,
    C::Error: Into<RedisError> + Send,
  {
    unimplemented!()
  }

  // dictadd, ...
}
