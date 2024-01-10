use bytes_utils::Str;
use fred::{prelude::*, types::XReadResponse};
use std::time::Duration;
use tokio::time::sleep;

static VALUES: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

/// Example showing how to use streams to communicate between Tokio tasks.
#[tokio::main]
async fn main() {
  pretty_env_logger::init();

  let reader_task = tokio::spawn(async move {
    let client = Builder::default_centralized()
      .with_config(|config| {
        config.password = Some("bar".into());
      })
      .build()?;
    client.connect();
    client.wait_for_connect().await?;

    // initialize the stream first
    client.del("foo").await?;
    client.xgroup_create("foo", "group", "$", true).await?;

    let mut count = 0;
    loop {
      // call XREAD for new records in a loop, blocking up to 10 sec each time
      let entry: XReadResponse<Str, Str, Str, Str> = client.xread_map(Some(1), Some(10_000), "foo", "$").await?;
      count += 1;

      for (key, records) in entry.into_iter() {
        for (id, fields) in records.into_iter() {
          println!("Reader recv {} - {}: {:?}", key, id, fields);
        }
      }

      if count * 2 >= VALUES.len() {
        break;
      }
    }

    client.quit().await?;
    Ok::<_, RedisError>(())
  });

  let writer_task = tokio::spawn(async move {
    // give the reader a chance to call XREAD first
    sleep(Duration::from_secs(1)).await;

    let client = Builder::default_centralized()
      .with_config(|config| {
        config.password = Some("bar".into());
      })
      .build()?;
    client.connect();
    client.wait_for_connect().await?;

    // add values in groups of 2. this should create the following stream contents:
    // [{"bar":"a","baz":"b"}, {"bar":"c","baz":"d"}, {"bar":"e","baz":"f"}, ...]
    for values in VALUES.chunks(2) {
      let id: Str = client
        .xadd("foo", false, None, "*", vec![
          ("field1", values[0]),
          ("field2", values[1]),
        ])
        .await?;

      println!("Writer added stream entry with ID: {}", id);
      sleep(Duration::from_secs(1)).await;
    }

    client.quit().await?;
    Ok::<_, RedisError>(())
  });

  futures::future::try_join_all([writer_task, reader_task])
    .await
    .expect("Error:");
}

// example output:
// Writer added stream entry with ID: 1704862102584-0
// Reader recv foo - 1704862102584-0: {"field2": "b", "field1": "a"}
// Writer added stream entry with ID: 1704862103589-0
// Reader recv foo - 1704862103589-0: {"field1": "c", "field2": "d"}
// Writer added stream entry with ID: 1704862104594-0
// Reader recv foo - 1704862104594-0: {"field1": "e", "field2": "f"}
// Writer added stream entry with ID: 1704862105598-0
// Reader recv foo - 1704862105598-0: {"field1": "g", "field2": "h"}
// Writer added stream entry with ID: 1704862106603-0
// Reader recv foo - 1704862106603-0: {"field1": "i", "field2": "j"}
