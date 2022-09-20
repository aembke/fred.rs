#[allow(unused_imports)]
use fred::clients::SubscriberClient;
use fred::prelude::*;
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

const COUNT: usize = 60;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let config = RedisConfig::default();
  let policy = ReconnectPolicy::new_linear(0, 5000, 500);
  let publisher_client = RedisClient::new(config.clone());
  let subscriber_client = RedisClient::new(config);

  let _ = tokio::spawn(subscriber_client.on_error().for_each(|e| async move {
    println!("Subscriber client connection error: {:?}", e);
  }));
  let _ = tokio::spawn(publisher_client.on_error().for_each(|e| async move {
    println!("Publisher client connection error: {:?}", e);
  }));

  let _ = tokio::spawn(subscriber_client.on_reconnect().for_each(|client| async move {
    println!("Subscriber client reconnected.");
    if let Err(e) = client.subscribe("foo").await {
      println!("Error resubscribing: {:?}", e);
    }
  }));

  let _ = publisher_client.connect(Some(policy.clone()));
  let _ = subscriber_client.connect(Some(policy));
  let _ = publisher_client.wait_for_connect().await?;
  let _ = subscriber_client.wait_for_connect().await?;

  let subscribe_task = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    while let Some((channel, message)) = message_stream.next().await {
      println!("Recv {:?} on channel {}", message, channel);
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

#[allow(dead_code)]
// requires the `subscriber-client` feature
async fn subscriber_example() -> Result<(), RedisError> {
  let subscriber = SubscriberClient::new(RedisConfig::default());
  let _ = subscriber.connect(Some(ReconnectPolicy::default()));
  let _ = subscriber.wait_for_connect().await?;

  let jh = tokio::spawn(subscriber.on_message().for_each(|(channel, message)| {
    println!("Recv {:?} on channel {}", message, channel);
    Ok(())
  }));
  // spawn a task to manage subscription state automatically whenever the client reconnects
  let _ = subscriber.manage_subscriptions();

  let _ = subscriber.subscribe("foo").await?;
  let _ = subscriber.psubscribe(vec!["bar*", "baz*"]).await?;
  // if the connection closes after this point for any reason the client will automatically re-subscribe to "foo",
  // "bar*", and "baz*" after reconnecting

  println!("Subscriber channels: {:?}", subscriber.tracked_channels()); // "foo"
  println!("Subscriber patterns: {:?}", subscriber.tracked_patterns()); // "bar*", "baz*"

  let _ = subscriber.unsubscribe("foo").await?;
  // now it will only automatically re-subscribe to "bar*" and "baz*" after reconnecting

  // force a re-subscription call to all channels or patterns
  let _ = subscriber.resubscribe_all().await?;
  // unsubscribe from all channels and patterns
  let _ = subscriber.unsubscribe_all().await?;
  // the subscriber client also supports all the basic redis commands
  let _ = subscriber.quit().await;
  let _ = jh.await;
}
