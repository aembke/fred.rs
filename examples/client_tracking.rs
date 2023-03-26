use fred::{interfaces::TrackingInterface, prelude::*, types::RespVersion};

#[tokio::main]
// see https://redis.io/docs/manual/client-side-caching/ for more information
async fn main() -> Result<(), RedisError> {
  let policy = ReconnectPolicy::new_constant(0, 1000);
  let mut config = RedisConfig::default();
  // RESP3 is required for the higher level interface provided by the `TrackingInterface` trait
  config.version = RespVersion::RESP3;

  let client = RedisClient::new(config, None, Some(policy));
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  // use the higher level interface that works with all deployment types
  let mut invalidations = client.on_invalidation();
  tokio::spawn(async move {
    while let Ok(invalidation) = invalidations.recv().await {
      println!("{}: Invalidate {:?}", invalidation.server, invalidation.keys);
    }
  });

  // enable client tracking on all connections. it's usually a good idea to do this in an `on_reconnect` block.
  let _ = client.start_tracking(None, false, false, false, false).await?;

  // send `CLIENT CACHING yes|no` before subsequent commands. the preceding `CLIENT CACHING yes|no` command will be
  // sent when the command is retried as well.
  println!("foo: {}", client.caching(false).incr::<i64, _>("foo").await?);
  println!("foo: {}", client.caching(true).incr::<i64, _>("foo").await?);
  let _ = client.stop_tracking().await?;

  // or use the basic interface against a centralized server. the above interface will be easier and more reliable for
  // almost all use cases, however.
  let subscriber = RedisClient::default();
  let client = RedisClient::default();

  // RESP2 requires two connections
  let _ = subscriber.connect();
  let _ = client.connect();
  let _ = subscriber.wait_for_connect().await?;
  let _ = client.wait_for_connect().await?;

  // the invalidation subscriber interface is the same as above even in RESP2 mode
  let mut invalidations = subscriber.on_invalidation();
  tokio::spawn(async move {
    while let Ok(invalidation) = invalidations.recv().await {
      println!("{}: Invalidate {:?}", invalidation.server, invalidation.keys);
    }
  });

  // enable client tracking, sending invalidation messages to the subscriber client
  let (_, connection_id) = subscriber
    .connection_ids()
    .await
    .into_iter()
    .next()
    .expect("Failed to read subscriber connection ID");
  let _ = client
    .client_tracking("on", Some(connection_id), None, false, false, false, false)
    .await?;

  println!("Tracking info: {:?}", client.client_trackinginfo::<RedisValue>().await?);
  println!("Redirection: {}", client.client_getredir::<i64>().await?);

  let pipeline = client.pipeline();
  // it's recommended to pipeline `CLIENT CACHING yes|no` if the client is used across multiple tasks
  let _: () = pipeline.client_caching(true).await?;
  let _: () = pipeline.incr("foo").await?;
  println!("Foo: {}", pipeline.last::<i64>().await?);

  Ok(())
}
