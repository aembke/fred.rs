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
    Ok(())
  });

  for idx in 0..COUNT {
    let _ = publisher_client.publish("foo", idx).await?;
    sleep(Duration::from_millis(1000)).await;
  }

  let _ = subscribe_task.abort();
  Ok(())
}
