use crate::commands;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{MemoryStats, RedisKey};
use crate::utils;

/// Functions that implement the [Memory](https://redis.io/commands#server) interface.
pub trait MemoryInterface: ClientLike + Sized {
  /// The MEMORY DOCTOR command reports about different memory-related issues that the Redis server experiences, and advises about possible remedies.
  ///
  /// <https://redis.io/commands/memory-doctor>
  fn memory_doctor(&self) -> AsyncResult<String> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::memory::memory_doctor(&inner).await
    })
  }

  /// The MEMORY MALLOC-STATS command provides an internal statistics report from the memory allocator.
  ///
  /// <https://redis.io/commands/memory-malloc-stats>
  fn memory_malloc_stats(&self) -> AsyncResult<String> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::memory::memory_malloc_stats(&inner).await
    })
  }

  /// The MEMORY PURGE command attempts to purge dirty pages so these can be reclaimed by the allocator.
  ///
  /// <https://redis.io/commands/memory-purge>
  fn memory_purge(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::memory::memory_purge(&inner).await
    })
  }

  /// The MEMORY STATS command returns an Array reply about the memory usage of the server.
  ///
  /// <https://redis.io/commands/memory-stats>
  fn memory_stats(&self) -> AsyncResult<MemoryStats> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::memory::memory_stats(&inner).await
    })
  }

  /// The MEMORY USAGE command reports the number of bytes that a key and its value require to be stored in RAM.
  ///
  /// <https://redis.io/commands/memory-usage>
  fn memory_usage<K>(&self, key: K, samples: Option<u32>) -> AsyncResult<Option<u64>>
  where
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::memory::memory_usage(&inner, key, samples).await
    })
  }
}
