#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

#[allow(unused_imports)]
use fred::clients::SubscriberClient;
use fred::{prelude::*, types::PerformanceConfig};
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let publisher_client = RedisClient::default();
  let subscriber_client = publisher_client.clone_new();

  let _ = publisher_client.connect();
  let _ = subscriber_client.connect();
  publisher_client.wait_for_connect().await?;
  subscriber_client.wait_for_connect().await?;

  let subscribe_task = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    while let Ok(message) = message_stream.recv().await {
      println!(
        "Recv {} on channel {}",
        message.value.convert::<i64>()?,
        message.channel
      );
    }
    Ok::<_, RedisError>(())
  });

  for idx in 0 .. 50 {
    let _ = publisher_client.publish("foo", idx).await?;
    sleep(Duration::from_secs(1)).await;
  }

  let _ = subscribe_task.abort();
  Ok(())
}

#[cfg(feature = "subscriber-client")]
async fn subscriber_example() -> Result<(), RedisError> {
  let subscriber = Builder::default_centralized().build_subscriber_client()?;
  let _ = subscriber.connect();
  subscriber.wait_for_connect().await?;

  let mut message_stream = subscriber.on_message();
  let _ = tokio::spawn(async move {
    while let Ok(message) = message_stream.recv().await {
      println!("Recv {:?} on channel {}", message.value, message.channel);
    }

    Ok::<_, RedisError>(())
  });

  // spawn a task to sync subscriptions whenever the client reconnects
  let _ = subscriber.manage_subscriptions();

  let _ = subscriber.subscribe("foo").await?;
  let _ = subscriber.psubscribe(vec!["bar*", "baz*"]).await?;
  let _ = subscriber.ssubscribe("abc{123}").await?;
  // upon reconnecting the client will automatically re-subscribe to the above channels and patterns
  println!("Subscriber channels: {:?}", subscriber.tracked_channels()); // "foo"
  println!("Subscriber patterns: {:?}", subscriber.tracked_patterns()); // "bar*", "baz*"
  println!("Subscriber shard channels: {:?}", subscriber.tracked_shard_channels()); // "abc{123}"

  let _ = subscriber.unsubscribe("foo").await?;
  // now it will only re-subscribe to "bar*", "baz*", and "abc{123}" after reconnecting

  // force a re-subscription call to all channels or patterns
  let _ = subscriber.resubscribe_all().await?;
  // unsubscribe from all channels and patterns
  let _ = subscriber.unsubscribe_all().await?;
  let _ = subscriber.quit().await;
  Ok(())
}
