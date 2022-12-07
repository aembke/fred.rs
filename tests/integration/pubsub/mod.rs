use fred::{interfaces::PubsubInterface, prelude::*};
use futures::{Stream, StreamExt};
use std::time::Duration;
use tokio::time::sleep;

const CHANNEL1: &'static str = "foo";
const CHANNEL2: &'static str = "bar";
const CHANNEL3: &'static str = "baz";
const FAKE_MESSAGE: &'static str = "wibble";
const NUM_MESSAGES: i64 = 20;

// when using chaos monkey pubsub messages can be lost since they're fire and forget and these arent stored in aof
// files
#[cfg(feature = "chaos-monkey")]
const EXTRA_MESSAGES: i64 = 10;
#[cfg(not(feature = "chaos-monkey"))]
const EXTRA_MESSAGES: i64 = 0;

#[cfg(feature = "chaos-monkey")]
const ASSERT_COUNT: bool = false;
#[cfg(not(feature = "chaos-monkey"))]
const ASSERT_COUNT: bool = true;

pub async fn should_publish_and_recv_messages(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber_client = client.clone_new();
  let _ = subscriber_client.connect();
  let _ = subscriber_client.wait_for_connect().await?;
  let _ = subscriber_client.subscribe(CHANNEL1).await?;

  let subscriber_jh = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    let mut count = 0;
    while count < NUM_MESSAGES {
      if let Ok(message) = message_stream.recv().await {
        assert_eq!(CHANNEL1, message.channel);
        if ASSERT_COUNT {
          assert_eq!(format!("{}-{}", FAKE_MESSAGE, count), message.value.as_str().unwrap());
        }
        count += 1;
      }
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0 .. NUM_MESSAGES + EXTRA_MESSAGES {
    // https://redis.io/commands/publish#return-value
    let _: () = client.publish(CHANNEL1, format!("{}-{}", FAKE_MESSAGE, idx)).await?;

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
  let _ = subscriber_client.connect();
  let _ = subscriber_client.wait_for_connect().await?;
  let _ = subscriber_client.psubscribe(channels.clone()).await?;

  let subscriber_jh = tokio::spawn(async move {
    let mut message_stream = subscriber_client.on_message();

    let mut count = 0;
    while count < NUM_MESSAGES {
      if let Ok(message) = message_stream.recv().await {
        assert!(subscriber_channels.contains(&&*message.channel));
        if ASSERT_COUNT {
          assert_eq!(format!("{}-{}", FAKE_MESSAGE, count), message.value.as_str().unwrap());
        }
        count += 1;
      }
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0 .. NUM_MESSAGES + EXTRA_MESSAGES {
    let channel = channels[idx as usize % channels.len()];

    // https://redis.io/commands/publish#return-value
    let _: () = client.publish(channel, format!("{}-{}", FAKE_MESSAGE, idx)).await?;

    // pubsub messages may arrive out of order due to cross-cluster broadcasting
    sleep(Duration::from_millis(50)).await;
  }
  let _ = subscriber_jh.await?;

  Ok(())
}
