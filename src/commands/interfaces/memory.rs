use crate::{
  commands,
  interfaces::{ClientLike, RedisResult},
  prelude::FromRedis,
  types::RedisKey,
};

/// Functions that implement the [memory](https://redis.io/commands#server) interface.
#[async_trait]
pub trait MemoryInterface: ClientLike + Sized {
  /// The MEMORY DOCTOR command reports about different memory-related issues that the Redis server experiences, and
  /// advises about possible remedies.
  ///
  /// <https://redis.io/commands/memory-doctor>
  async fn memory_doctor<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::memory::memory_doctor(self).await?.convert()
  }

  /// The MEMORY MALLOC-STATS command provides an internal statistics report from the memory allocator.
  ///
  /// <https://redis.io/commands/memory-malloc-stats>
  async fn memory_malloc_stats<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::memory::memory_malloc_stats(self).await?.convert()
  }

  /// The MEMORY PURGE command attempts to purge dirty pages so these can be reclaimed by the allocator.
  ///
  /// <https://redis.io/commands/memory-purge>
  async fn memory_purge(&self) -> RedisResult<()> {
    commands::memory::memory_purge(self).await
  }

  /// The MEMORY STATS command returns an Array reply about the memory usage of the server.
  ///
  /// <https://redis.io/commands/memory-stats>
  async fn memory_stats<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::memory::memory_stats(self).await?.convert()
  }

  /// The MEMORY USAGE command reports the number of bytes that a key and its value require to be stored in RAM.
  ///
  /// <https://redis.io/commands/memory-usage>
  async fn memory_usage<R, K>(&self, key: K, samples: Option<u32>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::memory::memory_usage(self, key, samples).await?.convert()
  }
}
