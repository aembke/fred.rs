use fred::{interfaces::PubsubInterface, prelude::*};
use futures::{Stream, StreamExt};
use std::{collections::HashMap, time::Duration};
use tokio::time::sleep;

const CHANNEL1: &'static str = "foo";
const CHANNEL2: &'static str = "bar";
const CHANNEL3: &'static str = "baz";
const FAKE_MESSAGE: &'static str = "wibble";
const NUM_MESSAGES: i64 = 20;

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
        assert_eq!(format!("{}-{}", FAKE_MESSAGE, count), message.value.as_str().unwrap());
        count += 1;
      }
    }

    Ok::<_, RedisError>(())
  });

  sleep(Duration::from_secs(1)).await;
  for idx in 0 .. NUM_MESSAGES {
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
        assert_eq!(format!("{}-{}", FAKE_MESSAGE, count), message.value.as_str().unwrap());
        count += 1;
      }
    }

    Ok::<_, RedisError>(())
  });

  sleep(Duration::from_secs(1)).await;
  for idx in 0 .. NUM_MESSAGES {
    let channel = channels[idx as usize % channels.len()];

    // https://redis.io/commands/publish#return-value
    let _: () = client.publish(channel, format!("{}-{}", FAKE_MESSAGE, idx)).await?;

    // pubsub messages may arrive out of order due to cross-cluster broadcasting
    sleep(Duration::from_millis(50)).await;
  }
  let _ = subscriber_jh.await?;

  Ok(())
}

pub async fn should_unsubscribe_from_all(publisher: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber = publisher.clone_new();
  let connection = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;
  let _ = subscriber.subscribe(vec![CHANNEL1, CHANNEL2, CHANNEL3]).await?;
  let mut message_stream = subscriber.on_message();

  let _ = tokio::spawn(async move {
    while let Ok(message) = message_stream.recv().await {
      // unsubscribe without args will result in 3 messages in this case, and none should show up here
      panic!("Recv unexpected pubsub message: {:?}", message);
    }

    Ok::<_, RedisError>(())
  });

  let _ = subscriber.unsubscribe(()).await?;
  sleep(Duration::from_secs(1)).await;

  // do some incr commands to make sure the response buffer is flushed correctly by this point
  assert_eq!(subscriber.incr::<i64, _>("abc{1}").await?, 1);
  assert_eq!(subscriber.incr::<i64, _>("abc{1}").await?, 2);
  assert_eq!(subscriber.incr::<i64, _>("abc{1}").await?, 3);

  let _ = subscriber.quit().await?;
  let _ = connection.await?;
  Ok(())
}

pub async fn should_get_pubsub_channels(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber = client.clone_new();
  let _ = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;

  let channels: Vec<String> = client.pubsub_channels("*").await?;
  #[cfg(feature = "sentinel-tests")]
  assert_eq!(channels.len(), 1); // "__sentinel__:hello" is always there
  #[cfg(not(feature = "sentinel-tests"))]
  assert!(channels.is_empty());

  let _: () = subscriber.subscribe("foo").await?;
  let _: () = subscriber.subscribe("bar").await?;
  let mut channels: Vec<String> = client.pubsub_channels("*").await?;
  channels.sort();

  #[cfg(feature = "sentinel-tests")]
  assert_eq!(channels, vec![
    "__sentinel__:hello".into(),
    "bar".to_string(),
    "foo".to_string()
  ]);
  #[cfg(not(feature = "sentinel-tests"))]
  assert_eq!(channels, vec!["bar".to_string(), "foo".to_string()]);

  Ok(())
}

pub async fn should_get_pubsub_numpat(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber = client.clone_new();
  let _ = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;

  assert_eq!(client.pubsub_numpat::<i64>().await?, 0);
  let _: () = subscriber.psubscribe("foo*").await?;
  let _: () = subscriber.psubscribe("bar*").await?;
  assert_eq!(client.pubsub_numpat::<i64>().await?, 2);

  Ok(())
}

pub async fn should_get_pubsub_nunmsub(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber = client.clone_new();
  let _ = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;

  let mut expected: HashMap<String, i64> = HashMap::new();
  expected.insert("foo".into(), 0);
  expected.insert("bar".into(), 0);
  let channels: HashMap<String, i64> = client.pubsub_numsub(vec!["foo", "bar"]).await?;
  assert_eq!(channels, expected);

  let _: () = subscriber.subscribe("foo").await?;
  let _: () = subscriber.subscribe("bar").await?;
  let channels: HashMap<String, i64> = client.pubsub_numsub(vec!["foo", "bar"]).await?;

  let mut expected: HashMap<String, i64> = HashMap::new();
  expected.insert("foo".into(), 1);
  expected.insert("bar".into(), 1);
  assert_eq!(channels, expected);

  Ok(())
}

pub async fn should_get_pubsub_shard_channels(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber = client.clone_new();
  let _ = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;

  let channels: Vec<String> = client.pubsub_shardchannels("{1}*").await?;
  assert!(channels.is_empty());

  let _: () = subscriber.ssubscribe("{1}foo").await?;
  let _: () = subscriber.ssubscribe("{1}bar").await?;

  let mut channels: Vec<String> = client.pubsub_shardchannels("{1}*").await?;
  channels.sort();
  assert_eq!(channels, vec!["{1}bar".to_string(), "{1}foo".to_string()]);

  Ok(())
}

pub async fn should_get_pubsub_shard_numsub(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let subscriber = client.clone_new();
  let _ = subscriber.connect();
  let _ = subscriber.wait_for_connect().await?;

  let mut expected: HashMap<String, i64> = HashMap::new();
  expected.insert("foo{1}".into(), 0);
  expected.insert("bar{1}".into(), 0);
  let channels: HashMap<String, i64> = client.pubsub_shardnumsub(vec!["foo{1}", "bar{1}"]).await?;
  assert_eq!(channels, expected);

  let _: () = subscriber.ssubscribe("foo{1}").await?;
  let _: () = subscriber.ssubscribe("bar{1}").await?;
  let channels: HashMap<String, i64> = client.pubsub_shardnumsub(vec!["foo{1}", "bar{1}"]).await?;

  let mut expected: HashMap<String, i64> = HashMap::new();
  expected.insert("foo{1}".into(), 1);
  expected.insert("bar{1}".into(), 1);
  assert_eq!(channels, expected);

  Ok(())
}
