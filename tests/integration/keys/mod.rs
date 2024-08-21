use bytes::Bytes;
use fred::{
  clients::{RedisClient, RedisPool},
  error::RedisError,
  interfaces::*,
  types::{Expiration, ReconnectPolicy, RedisConfig, RedisMap, RedisValue},
};
use futures::{pin_mut, StreamExt};
use std::{collections::HashMap, time::Duration};
use tokio::{self, time::sleep};

#[cfg(feature = "default-nil-types")]
pub async fn should_handle_missing_keys(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.get::<Bytes, _>("foo").await?.is_empty());
  Ok(())
}

#[cfg(not(feature = "default-nil-types"))]
pub async fn should_handle_missing_keys(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.get::<Bytes, _>("foo").await.is_err());
  Ok(())
}

pub async fn should_set_and_get_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", None, None, false).await?;

  assert_eq!(client.get::<String, _>("foo").await?, "bar");
  Ok(())
}

pub async fn should_set_and_del_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let result: Option<String> = client.set("foo", "bar", None, None, true).await?;
  assert!(result.is_none());

  assert_eq!(client.get::<String, _>("foo").await?, "bar");
  assert_eq!(client.del::<i64, _>("foo").await?, 1);

  Ok(())
}

pub async fn should_set_with_get_argument(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", None, None, false).await?;

  let result: String = client.set("foo", "baz", None, None, true).await?;
  assert_eq!(result, "bar");

  let result: String = client.get("foo").await?;
  assert_eq!(result, "baz");

  Ok(())
}

pub async fn should_rename(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("{foo}.1", "baz", None, None, false).await?;

  client.rename("{foo}.1", "{foo}.2").await?;
  let result: String = client.get("{foo}.2").await?;
  assert_eq!(result, "baz");

  Ok(())
}

pub async fn should_error_rename_does_not_exist(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.rename("{foo}", "{foo}.bar").await
}

pub async fn should_renamenx(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("{foo}.1", "baz", None, None, false).await?;

  client.renamenx("{foo}.1", "{foo}.2").await?;
  let result: String = client.get("{foo}.2").await?;
  assert_eq!(result, "baz");

  Ok(())
}

pub async fn should_error_renamenx_does_not_exist(
  client: RedisClient,
  _config: RedisConfig,
) -> Result<(), RedisError> {
  client.renamenx("{foo}", "{foo}.bar").await
}

pub async fn should_unlink(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("{foo}1", "bar", None, None, false).await?;

  assert_eq!(client.get::<String, _>("{foo}1").await?, "bar");
  assert_eq!(
    client
      .unlink::<i64, _>(vec!["{foo}1", "{foo}", "{foo}:something"])
      .await?,
    1
  );

  Ok(())
}

pub async fn should_incr_and_decr_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let count: u64 = client.incr("foo").await?;
  assert_eq!(count, 1);
  let count: u64 = client.incr_by("foo", 2).await?;
  assert_eq!(count, 3);
  let count: u64 = client.decr("foo").await?;
  assert_eq!(count, 2);
  let count: u64 = client.decr_by("foo", 2).await?;
  assert_eq!(count, 0);

  Ok(())
}

pub async fn should_incr_by_float(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let count: f64 = client.incr_by_float("foo", 1.5).await?;
  assert_eq!(count, 1.5);
  let count: f64 = client.incr_by_float("foo", 2.2).await?;
  assert_eq!(count, 3.7);
  let count: f64 = client.incr_by_float("foo", -1.2).await?;
  assert_eq!(count, 2.5);

  Ok(())
}

pub async fn should_mset_a_non_empty_map(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  let mut map: HashMap<String, RedisValue> = HashMap::new();
  // MSET args all have to map to the same cluster node
  map.insert("a{1}".into(), 1.into());
  map.insert("b{1}".into(), 2.into());
  map.insert("c{1}".into(), 3.into());

  client.mset(map).await?;
  let a: i64 = client.get("a{1}").await?;
  let b: i64 = client.get("b{1}").await?;
  let c: i64 = client.get("c{1}").await?;

  assert_eq!(a, 1);
  assert_eq!(b, 2);
  assert_eq!(c, 3);

  Ok(())
}

// should panic
pub async fn should_error_mset_empty_map(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.mset(RedisMap::new()).await.map(|_| ())
}

pub async fn should_expire_key(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", None, None, false).await?;

  client.expire("foo", 1).await?;
  sleep(Duration::from_millis(1500)).await;
  let foo: Option<String> = client.get("foo").await?;
  assert!(foo.is_none());

  Ok(())
}

pub async fn should_persist_key(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let removed: bool = client.persist("foo").await?;
  assert!(removed);

  let ttl: i64 = client.ttl("foo").await?;
  assert_eq!(ttl, -1);

  Ok(())
}

pub async fn should_check_ttl(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let ttl: i64 = client.ttl("foo").await?;
  assert!(ttl > 0 && ttl < 6);

  Ok(())
}

pub async fn should_check_pttl(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let ttl: i64 = client.pttl("foo").await?;
  assert!(ttl > 0 && ttl < 5001);

  Ok(())
}

pub async fn should_dump_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "abc123", None, None, false).await?;
  let dump: RedisValue = client.dump("foo").await?;
  assert!(dump.is_bytes());

  Ok(())
}

pub async fn should_dump_and_restore_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = "abc123";

  client.set("foo", expected, None, None, false).await?;
  let dump = client.dump("foo").await?;
  client.del("foo").await?;

  client.restore("foo", 0, dump, false, false, None, None).await?;
  let value: String = client.get("foo").await?;
  assert_eq!(value, expected);

  Ok(())
}

pub async fn should_modify_ranges(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.set("foo", "0123456789", None, None, false).await?;

  let range: String = client.getrange("foo", 0, 4).await?;
  assert_eq!(range, "01234");

  client.setrange("foo", 4, "abc").await?;
  let value: String = client.get("foo").await?;
  assert_eq!(value, "0123abc789");

  Ok(())
}

pub async fn should_getset_value(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let value: Option<String> = client.getset("foo", "bar").await?;
  assert!(value.is_none());
  let value: String = client.getset("foo", "baz").await?;
  assert_eq!(value, "bar");
  let value: String = client.get("foo").await?;
  assert_eq!(value, "baz");

  Ok(())
}

pub async fn should_getdel_value(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let value: Option<String> = client.getdel("foo").await?;
  assert!(value.is_none());

  client.set("foo", "bar", None, None, false).await?;
  let value: String = client.getdel("foo").await?;
  assert_eq!(value, "bar");
  let value: Option<String> = client.get("foo").await?;
  assert!(value.is_none());

  Ok(())
}

pub async fn should_get_strlen(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = "abcdefghijklmnopqrstuvwxyz";
  client.set("foo", expected, None, None, false).await?;
  let len: usize = client.strlen("foo").await?;
  assert_eq!(len, expected.len());

  Ok(())
}

pub async fn should_mget_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into()), ("c{1}", 3.into())];
  for (key, value) in expected.iter() {
    client.set(*key, value.clone(), None, None, false).await?;
  }
  let values: Vec<i64> = client.mget(vec!["a{1}", "b{1}", "c{1}"]).await?;
  assert_eq!(values, vec![1, 2, 3]);

  Ok(())
}

pub async fn should_msetnx_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into())];

  // do it first, check they're there
  let values: i64 = client.msetnx(expected.clone()).await?;
  assert_eq!(values, 1);
  let a: i64 = client.get("a{1}").await?;
  let b: i64 = client.get("b{1}").await?;
  assert_eq!(a, 1);
  assert_eq!(b, 2);

  client.del(vec!["a{1}", "b{1}"]).await?;
  client.set("a{1}", 3, None, None, false).await?;

  let values: i64 = client.msetnx(expected.clone()).await?;
  assert_eq!(values, 0);
  let b: Option<i64> = client.get("b{1}").await?;
  assert!(b.is_none());

  Ok(())
}

pub async fn should_copy_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  client.set("a{1}", "bar", None, None, false).await?;
  let result: i64 = client.copy("a{1}", "b{1}", None, false).await?;
  assert_eq!(result, 1);

  let b: String = client.get("b{1}").await?;
  assert_eq!(b, "bar");

  client.set("a{1}", "baz", None, None, false).await?;
  let result: i64 = client.copy("a{1}", "b{1}", None, false).await?;
  assert_eq!(result, 0);

  let result: i64 = client.copy("a{1}", "b{1}", None, true).await?;
  assert_eq!(result, 1);
  let b: String = client.get("b{1}").await?;
  assert_eq!(b, "baz");

  Ok(())
}

pub async fn should_get_keys_from_pool_in_a_stream(
  client: RedisClient,
  config: RedisConfig,
) -> Result<(), RedisError> {
  client.set("foo", "bar", None, None, false).await?;

  let pool = RedisPool::new(config, None, None, None, 5)?;
  pool.connect();
  pool.wait_for_connect().await?;

  let stream =
    tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(Duration::from_millis(100))).then(move |_| {
      let pool = pool.clone();

      async move {
        let value: Option<String> = pool.get("foo").await.unwrap();
        value
      }
    });
  pin_mut!(stream);

  let mut count = 0;
  while let Some(value) = stream.next().await {
    assert_eq!(value, Some("bar".into()));
    count += 1;

    if count >= 10 {
      break;
    }
  }

  Ok(())
}

pub async fn should_pexpire_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.set("foo", "bar", None, None, false).await?;
  assert_eq!(client.pexpire::<i64, _>("foo", 100, None).await?, 1);

  sleep(Duration::from_millis(150)).await;
  assert_eq!(client.get::<Option<String>, _>("foo").await?, None);
  Ok(())
}
