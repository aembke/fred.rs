use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // the `auto_pipeline` config option determines whether the client will pipeline commands across tasks.
  // this example shows how to pipeline commands within one task.
  let client = RedisClient::default();
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  let pipeline = client.pipeline();
  // commands are queued in memory
  let result: RedisValue = pipeline.incr("foo").await?;
  assert!(result.is_queued());
  let result: RedisValue = pipeline.incr("foo").await?;
  assert!(result.is_queued());

  // send the pipeline and return all the results in order
  let (first, second): (i64, i64) = pipeline.all().await?;
  assert_eq!((first, second), (1, 2));

  let _: () = client.del("foo").await?;
  // or send the pipeline and only return the last result
  let pipeline = client.pipeline();
  let _: () = pipeline.incr("foo").await?;
  let _: () = pipeline.incr("foo").await?;
  assert_eq!(pipeline.last::<i64>().await?, 2);

  let _: () = client.del("foo").await?;
  // or handle each command result individually
  let pipeline = client.pipeline();
  let _: () = pipeline.incr("foo").await?;
  let _: () = pipeline.hgetall("foo").await?; // this will result in a `WRONGTYPE` error
  let results = pipeline.try_all::<i64>().await;
  assert_eq!(results[0].clone().unwrap(), 1);
  assert!(results[1].is_err());

  let _ = client.quit().await?;
  Ok(())
}
