use fred::prelude::*;
use futures::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

const CHANNEL1: &'static str = "foo";
const CHANNEL2: &'static str = "bar";
const CHANNEL3: &'static str = "baz";
const FAKE_MESSAGE: &'static str = "wibble";
const NUM_MESSAGES: i64 = 20;

pub async fn should_publish_and_recv_messages(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber_client = client.clone_new();
  let policy = client.client_reconnect_policy();
  let _ = subscriber_client.connect(policy);
  let _ = subscriber_client.wait_for_connect().await?;
  let _ = subscriber_client.subscribe(CHANNEL1).await?;

  let subscriber_jh = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    let mut count = 0;
    while count < NUM_MESSAGES {
      if let Some((channel, message)) = message_stream.next().await {
        let message = message.as_str().unwrap();

        assert_eq!(CHANNEL1, channel);
        assert_eq!(format!("{}-{}", FAKE_MESSAGE, count), message);
        count += 1;
      }
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0..NUM_MESSAGES {
    // https://redis.io/commands/publish#return-value
    let _ = client.publish(CHANNEL1, format!("{}-{}", FAKE_MESSAGE, idx)).await?;

    // pubsub messages may arrive out of order due to cross-cluster broadcasting
    sleep(Duration::from_millis(50)).await;
  }
  let _ = subscriber_jh.await?;

  Ok(())
}

pub async fn should_psubscribe_and_recv_messages(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let channels = vec![CHANNEL1, CHANNEL2, CHANNEL3];
  let subscriber_channels = channels.clone();

  let subscriber_client = client.clone_new();
  let policy = client.client_reconnect_policy();
  let _ = subscriber_client.connect(policy);
  let _ = subscriber_client.wait_for_connect().await?;
  let _ = subscriber_client.psubscribe(channels.clone()).await?;

  let subscriber_jh = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    let mut count = 0;
    while count < NUM_MESSAGES {
      if let Some((channel, message)) = message_stream.next().await {
        let message = message.as_str().unwrap();

        assert!(subscriber_channels.contains(&channel.as_str()));
        assert_eq!(format!("{}-{}", FAKE_MESSAGE, count), message);
        count += 1;
      }
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0..NUM_MESSAGES {
    let channel = channels[idx as usize % channels.len()];

    // https://redis.io/commands/publish#return-value
    let _ = client.publish(channel, format!("{}-{}", FAKE_MESSAGE, idx)).await?;

    // pubsub messages may arrive out of order due to cross-cluster broadcasting
    sleep(Duration::from_millis(50)).await;
  }
  let _ = subscriber_jh.await?;

  Ok(())
}
