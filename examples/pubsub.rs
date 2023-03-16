#[allow(unused_imports)]
use fred::clients::SubscriberClient;
use fred::{prelude::*, types::PerformanceConfig};
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

const COUNT: usize = 60;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let perf = PerformanceConfig::default();
  let policy = ReconnectPolicy::new_linear(0, 5000, 500);
  let publisher_client = RedisClient::new(config, Some(perf), Some(policy));
  let subscriber_client = publisher_client.clone_new();

  let _ = publisher_client.connect();
  let _ = subscriber_client.connect();
  let _ = publisher_client.wait_for_connect().await?;
  let _ = subscriber_client.wait_for_connect().await?;

  let subscribe_task = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    while let Ok(message) = message_stream.recv().await {
      println!("Recv {:?} on channel {}", message.value, message.channel);
    }
    Ok::<_, RedisError>(())
  });

  for idx in 0 .. COUNT {
    let _ = publisher_client.publish("foo", idx).await?;
    sleep(Duration::from_millis(1000)).await;
  }

  let _ = subscribe_task.abort();
  Ok(())
}

#[cfg(feature = "subscriber-client")]
async fn subscriber_example() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let perf = PerformanceConfig::default();
  let policy = ReconnectPolicy::new_linear(0, 5000, 500);
  let subscriber = SubscriberClient::new(config, Some(perf), Some(policy));
  let _ = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;

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
  println!("Subscriber channels: {:?}", subscriber.tracked_channels()); // "foo"
  println!("Subscriber patterns: {:?}", subscriber.tracked_patterns()); // "bar*", "baz*"
  println!("Subscriber shard channels: {:?}", subscriber.tracked_shard_channels()); // "abc{123}"

  let _ = subscriber.unsubscribe("foo").await?;
  // now it will only automatically re-subscribe to "bar*", "baz*", and "abc{123}" after reconnecting

  // force a re-subscription call to all channels or patterns
  let _ = subscriber.resubscribe_all().await?;
  // unsubscribe from all channels and patterns
  let _ = subscriber.unsubscribe_all().await?;
  // the subscriber client also supports all the basic redis commands
  let _ = subscriber.quit().await;
  Ok(())
}
