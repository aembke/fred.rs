#![allow(dead_code)]
use fred::{prelude::*, types::Scanner};
use futures::TryStreamExt;
use tokio_stream::StreamExt;

const SCAN_KEYS: i64 = 100;

#[cfg(feature = "i-keys")]
pub async fn should_scan_keyspace(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..SCAN_KEYS {
    client
      .set(format!("foo-{}-{}", idx, "{1}"), idx, None, None, false)
      .await?;
  }

  let count = client
    .scan("foo*{1}", Some(10), None)
    .try_fold(0, |mut count, mut result| async move {
      if let Some(results) = result.take_results() {
        count += results.len() as i64;
        // scanning wont return results in any particular order, so we just check the format of the key

        for key in results.into_iter() {
          let parts: Vec<&str> = key.as_str().unwrap().split('-').collect();
          assert!(parts[1].parse::<i64>().is_ok());
        }
      } else {
        panic!("Empty results in scan.");
      }

      result.next()?;
      Ok(count)
    })
    .await?;

  assert_eq!(count, SCAN_KEYS);
  Ok(())
}

#[cfg(feature = "i-hashes")]
pub async fn should_hscan_hash(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..SCAN_KEYS {
    let value = (format!("bar-{}", idx), idx);
    client.hset("foo", value).await?;
  }

  let count = client
    .hscan("foo", "bar*", Some(10))
    .try_fold(0_i64, |mut count, mut result| async move {
      if let Some(results) = result.take_results() {
        count += results.len() as i64;

        // scanning wont return results in any particular order, so we just check the format of the key
        for (key, _) in results.iter() {
          let parts: Vec<&str> = key.as_str().unwrap().split('-').collect();
          assert!(parts[1].parse::<i64>().is_ok());
        }
      } else {
        panic!("Empty results in hscan.");
      }

      result.next()?;
      Ok(count)
    })
    .await?;

  assert_eq!(count, SCAN_KEYS);
  Ok(())
}

#[cfg(feature = "i-sets")]
pub async fn should_sscan_set(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..SCAN_KEYS {
    client.sadd("foo", idx).await?;
  }

  let count = client
    .sscan("foo", "*", Some(10))
    .try_fold(0_i64, |mut count, mut result| async move {
      if let Some(results) = result.take_results() {
        count += results.len() as i64;

        for value in results.into_iter() {
          assert!(value.as_i64().is_some());
        }
      } else {
        panic!("Empty sscan result");
      }

      result.next()?;
      Ok(count)
    })
    .await?;

  assert_eq!(count, SCAN_KEYS);
  Ok(())
}

#[cfg(feature = "i-sorted-sets")]
pub async fn should_zscan_sorted_set(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..SCAN_KEYS {
    let (score, value) = (idx as f64, format!("foo-{}", idx));
    client.zadd("foo", None, None, false, false, (score, value)).await?;
  }

  let count = client
    .zscan("foo", "*", Some(10))
    .try_fold(0_i64, |mut count, mut result| async move {
      if let Some(results) = result.take_results() {
        count += results.len() as i64;

        for (value, score) in results.into_iter() {
          let value_str = value.as_str().unwrap();
          let parts: Vec<&str> = value_str.split('-').collect();
          let value_suffix = parts[1].parse::<f64>().unwrap();

          assert_eq!(value_suffix, score);
        }
      } else {
        panic!("Empty zscan result");
      }

      result.next()?;
      Ok(count)
    })
    .await?;

  assert_eq!(count, SCAN_KEYS);
  Ok(())
}

#[cfg(feature = "i-keys")]
pub async fn should_scan_cluster(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..2000 {
    client.set(idx, idx, None, None, false).await?;
  }

  let mut count = 0;
  let mut scan_stream = client.scan_cluster("*", Some(10), None);
  while let Some(Ok(mut page)) = scan_stream.next().await {
    let results = page.take_results();
    count += results.unwrap().len();
    let _ = page.next();
  }

  assert_eq!(count, 2000);
  Ok(())
}
