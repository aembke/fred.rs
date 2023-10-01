#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{prelude::*, types::Scanner};
use futures::stream::StreamExt;

static COUNT: usize = 50;

async fn create_fake_data(client: &RedisClient) -> Result<(), RedisError> {
  for idx in 0 .. COUNT {
    client.set(format!("foo-{}", idx), idx, None, None, false).await?;
  }
  Ok(())
}

async fn delete_fake_data(client: &RedisClient) -> Result<(), RedisError> {
  for idx in 0 .. COUNT {
    client.del(format!("foo-{}", idx)).await?;
  }
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();
  let _ = client.connect();
  client.wait_for_connect().await?;
  create_fake_data(&client).await?;

  // build up a buffer of (key, value) pairs from pages (~10 keys per page)
  let mut buffer = Vec::with_capacity(COUNT);
  let mut scan_stream = client.scan("foo*", Some(10), None);

  while let Some(result) = scan_stream.next().await {
    let mut page = result.expect("SCAN failed with error");

    if let Some(keys) = page.take_results() {
      // create a client from the scan result, reusing the existing connection(s)
      let client = page.create_client();

      for key in keys.into_iter() {
        let value: RedisValue = client.get(&key).await?;
        println!("Scanned {} -> {:?}", key.as_str_lossy(), value);
        buffer.push((key, value));
      }
    }

    // **important:** move on to the next page now that we're done reading the values. or move this before we call
    // `get` on each key to scan results in the background as quickly as possible.
    let _ = page.next();
  }

  delete_fake_data(&client).await?;
  client.quit().await?;
  Ok(())
}
