#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]
#![allow(dead_code)]

use fred::{prelude::*, types::Scanner};
use futures::stream::TryStreamExt;

async fn create_fake_data(client: &RedisClient) -> Result<(), RedisError> {
  let values: Vec<(String, i64)> = (0 .. 50).map(|i| (format!("foo-{}", i), i)).collect();
  client.mset(values).await
}

async fn delete_fake_data(client: &RedisClient) -> Result<(), RedisError> {
  let keys: Vec<_> = (0 .. 50).map(|i| format!("foo-{}", i)).collect();
  client.del(keys).await?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();
  client.init().await?;
  create_fake_data(&client).await?;

  // scan all keys in the keyspace, returning 10 keys per page
  let mut scan_stream = client.scan("foo*", Some(10), None);
  while let Some(mut page) = scan_stream.try_next().await? {
    if let Some(keys) = page.take_results() {
      // create a client from the scan result, reusing the existing connection(s)
      let client = page.create_client();

      for key in keys.into_iter() {
        let value: RedisValue = client.get(&key).await?;
        println!("Scanned {} -> {:?}", key.as_str_lossy(), value);
      }
    }

    // **important:** move on to the next page now that we're done reading the values
    let _ = page.next();
  }

  delete_fake_data(&client).await?;
  client.quit().await?;
  Ok(())
}

/// Example showing how to print the memory usage of all keys in a cluster with a `RedisPool`.
///
/// When using a clustered deployment the keyspace will be spread across multiple nodes. However, the cursor in each
/// SCAN command is used to iterate over keys within a single node. There are several ways to concurrently scan
/// all keys on all nodes:
///
/// 1. Use `scan_cluster`.
/// 2. Use `split_cluster` and `scan`.
/// 3. Use `with_cluster_node` and `scan`.
///
/// The best option depends on several factors, but `scan_cluster` is often the easiest approach for most use
/// cases.
async fn pool_scan_cluster_memory_example(pool: &RedisPool) -> Result<(), RedisError> {
  // the majority of the client traffic in this scenario comes from the MEMORY USAGE call on each key, so we'll use a
  // pool to round-robin these commands among multiple clients. a clustered client with `auto_pipeline: true` can scan
  // all nodes in the cluster concurrently, so we use a single client rather than a pool to issue the SCAN calls.
  let mut total_size = 0;
  // if the pattern contains a hash tag then callers can use `scan` instead of `scan_cluster`
  let mut scanner = pool.next().scan_cluster("*", Some(100), None);

  while let Some(mut page) = scanner.try_next().await? {
    if let Some(page) = page.take_results() {
      // pipeline the `MEMORY USAGE` calls
      let pipeline = pool.next().pipeline();
      for key in page.iter() {
        pipeline.memory_usage(key, Some(0)).await?;
      }
      let sizes: Vec<Option<u64>> = pipeline.all().await?;
      assert_eq!(page.len(), sizes.len());

      for (idx, key) in page.into_iter().enumerate() {
        let size = sizes[idx].unwrap_or(0);
        println!("{}: {}", key.as_str_lossy(), size);
        total_size += size;
      }
    }

    let _ = page.next();
  }

  println!("Total size: {}", total_size);
  Ok(())
}
