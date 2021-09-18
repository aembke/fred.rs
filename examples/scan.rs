use fred::prelude::*;
use futures::stream::{StreamExt, TryStreamExt};
use std::collections::VecDeque;

static COUNT: u32 = 50;

async fn create_fake_data(client: &RedisClient) -> Result<(), RedisError> {
  for idx in 0..COUNT {
    let _ = client.set(format!("foo-{}", idx), idx, None, None, false).await?;
  }
  Ok(())
}

async fn delete_fake_data(client: &RedisClient) -> Result<(), RedisError> {
  for idx in 0..COUNT {
    let _ = client.del(format!("foo-{}", idx)).await?;
  }
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let client = RedisClient::new(config);

  let jh = client.connect(None);
  let _ = client.wait_for_connect().await?;
  let _ = delete_fake_data(&client).await?;
  let _ = create_fake_data(&client).await?;

  // build up a buffer of (key, value) pairs from pages with 10 keys per page
  let mut buffer = Vec::with_capacity(COUNT as usize);
  let mut scan_stream = client.scan("foo*", Some(10), None);

  while let Some(Ok(mut page)) = scan_stream.next().await {
    if let Some(keys) = page.take_results() {
      // create a client from the scan result, reusing the existing connection(s)
      let client = page.create_client();

      for key in keys.into_iter() {
        let value: RedisValue = client.get(&key).await?;
        println!("Scanned {} -> {:?}", key.as_str_lossy(), value);
        buffer.push((key, value));
      }
    }

    // move on to the next page now that we're done reading the values
    // or move this before we call `get` on each key to scan results in the background as fast as possible
    let _ = page.next();
  }

  assert_eq!(buffer.len(), COUNT as usize);
  let _ = delete_fake_data(&client).await?;

  let _ = client.quit().await?;
  let _ = jh.await;
  Ok(())
}
