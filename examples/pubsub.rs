#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

#[allow(unused_imports)]
use fred::clients::SubscriberClient;
use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let publisher_client = RedisClient::default();
  let subscriber_client = publisher_client.clone_new();
  publisher_client.init().await?;
  subscriber_client.init().await?;

  // or use `message_rx()` to use the underlying `BroadcastReceiver` directly without spawning a new task
  let message_task = subscriber_client.on_message(|message| {
    println!("{}: {}", message.channel, message.value.convert::<i64>()?);
    Ok::<_, RedisError>(())
  });

  for idx in 0 .. 50 {
    let _ = publisher_client.publish("foo", idx).await?;
    sleep(Duration::from_secs(1)).await;
  }

  publisher_client.quit().await?;
  subscriber_client.quit().await?;
  let _ = message_task.await;
  Ok(())
}

#[cfg(feature = "subscriber-client")]
async fn subscriber_example() -> Result<(), RedisError> {
  let subscriber = Builder::default_centralized().build_subscriber_client()?;
  subscriber.init().await?;

  // or use the `on_message` shorthand
  let mut message_stream = subscriber.message_rx();
  let subscriber_task = tokio::spawn(async move {
    while let Ok(message) = message_stream.recv().await {
      println!("Recv {:?} on channel {}", message.value, message.channel);
    }

    Ok::<_, RedisError>(())
  });

  // spawn a task to sync subscriptions whenever the client reconnects
  let resubscribe_task = subscriber.manage_subscriptions();

  subscriber.subscribe("foo").await?;
  subscriber.psubscribe(vec!["bar*", "baz*"]).await?;
  subscriber.ssubscribe("abc{123}").await?;
  // upon reconnecting the client will automatically re-subscribe to the above channels and patterns
  println!("Subscriber channels: {:?}", subscriber.tracked_channels()); // "foo"
  println!("Subscriber patterns: {:?}", subscriber.tracked_patterns()); // "bar*", "baz*"
  println!("Subscriber shard channels: {:?}", subscriber.tracked_shard_channels()); // "abc{123}"

  subscriber.unsubscribe("foo").await?;
  // now it will only re-subscribe to "bar*", "baz*", and "abc{123}" after reconnecting

  // force a re-subscription call to all channels or patterns
  subscriber.resubscribe_all().await?;
  // unsubscribe from all channels and patterns
  subscriber.unsubscribe_all().await?;

  subscriber.quit().await?;
  let _ = subscriber_task.await;
  let _ = resubscribe_task.await;
  Ok(())
}
