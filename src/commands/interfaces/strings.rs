use crate::{
  interfaces::{ClientLike, RedisResult},
  prelude::RedisError,
  types::{BitcountRange, FromRedis, RedisKey},
};
use std::convert::TryInto;

/// Functions implementing the [bitmap](https://redis.io/commands/?group=bitmap) interface.
#[async_trait]
pub trait BitmapInterface: ClientLike + Sized {
  /// Sets or clears the bit at offset in the string value stored at key.
  ///
  /// <https://redis.io/commands/setbit/>
  async fn setbit<R, K>(&self, key: K, offset: usize, value: u8) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    unimplemented!()
  }

  /// Count the number of set bits (population counting) in a string.
  ///
  /// <https://redis.io/commands/bitcount/>
  async fn bitcount<R, K, V>(&self, key: K, range: Option<V>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<BitcountRange> + Send,
    V::Error: Into<RedisError> + Send,
  {
    unimplemented!()
  }
}
