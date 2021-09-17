use fred::client::RedisClient;
use fred::error::RedisError;
use fred::prelude::Expiration;
use fred::types::{RedisConfig, RedisMap, RedisValue};
use std::collections::HashMap;

use std::time::Duration;
use tokio;
use tokio::time::sleep;

pub async fn should_set_and_get_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _: () = client.set("foo", "bar", None, None, false).await?;

  assert_eq!(client.get::<String, _>("foo").await?, "bar");
  Ok(())
}

pub async fn should_set_and_del_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result: Option<String> = client.set("foo", "bar", None, None, true).await?;
  assert!(result.is_none());

  assert_eq!(client.get::<String, _>("foo").await?, "bar");
  assert_eq!(client.del::<i64, _>("foo").await?, 1);

  check_null!(client, "foo");
  Ok(())
}

pub async fn should_set_with_get_argument(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _: () = client.set("foo", "bar", None, None, false).await?;

  let result: String = client.set("foo", "baz", None, None, true).await?;
  assert_eq!(result, "bar");

  let result: String = client.get("foo").await?;
  assert_eq!(result, "baz");

  Ok(())
}

pub async fn should_incr_and_decr_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

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
  check_null!(client, "foo");

  let count: f64 = client.incr_by_float("foo", 1.5).await?;
  assert_eq!(count, 1.5);
  let count: f64 = client.incr_by_float("foo", 2.2).await?;
  assert_eq!(count, 3.7);
  let count: f64 = client.incr_by_float("foo", -1.2).await?;
  assert_eq!(count, 2.5);

  Ok(())
}

pub async fn should_mset_a_non_empty_map(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");
  check_null!(client, "c{1}");

  let mut map: HashMap<String, RedisValue> = HashMap::new();
  // MSET args all have to map to the same cluster node
  map.insert("a{1}".into(), 1.into());
  map.insert("b{1}".into(), 2.into());
  map.insert("c{1}".into(), 3.into());

  let _ = client.mset(map).await?;
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
  check_null!(client, "foo");
  let _: () = client.set("foo", "bar", None, None, false).await?;

  let _: () = client.expire("foo", 1).await?;
  sleep(Duration::from_millis(1500)).await;
  let foo: Option<String> = client.get("foo").await?;
  assert!(foo.is_none());

  Ok(())
}

pub async fn should_persist_key(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _: () = client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let removed: bool = client.persist("foo").await?;
  assert!(removed);

  let ttl: i64 = client.ttl("foo").await?;
  assert_eq!(ttl, -1);

  Ok(())
}

pub async fn should_check_ttl(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _: () = client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let ttl: i64 = client.ttl("foo").await?;
  assert!(ttl > 0 && ttl < 6);

  Ok(())
}

pub async fn should_check_pttl(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _: () = client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let ttl: i64 = client.pttl("foo").await?;
  assert!(ttl > 0 && ttl < 5001);

  Ok(())
}

pub async fn should_dump_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _: () = client.set("foo", "abc123", None, None, false).await?;
  let dump = client.dump("foo").await?;
  assert!(dump.is_bytes());

  Ok(())
}

pub async fn should_dump_and_restore_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let expected = "abc123";

  let _: () = client.set("foo", expected, None, None, false).await?;
  let dump = client.dump("foo").await?;
  let _: () = client.del("foo").await?;

  let _ = client.restore("foo", 0, dump, false, false, None, None).await?;
  let value: String = client.get("foo").await?;
  assert_eq!(value, expected);

  Ok(())
}

pub async fn should_modify_ranges(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _: () = client.set("foo", "0123456789", None, None, false).await?;

  let range: String = client.getrange("foo", 0, 4).await?;
  assert_eq!(range, "01234");

  let _: () = client.setrange("foo", 4, "abc").await?;
  let value: String = client.get("foo").await?;
  assert_eq!(value, "0123abc789");

  Ok(())
}

pub async fn should_getset_value(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let value: Option<String> = client.getset("foo", "bar").await?;
  assert!(value.is_none());
  let value: String = client.getset("foo", "baz").await?;
  assert_eq!(value, "bar");
  let value: String = client.get("foo").await?;
  assert_eq!(value, "baz");

  Ok(())
}

pub async fn should_getdel_value(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let value: Option<String> = client.getdel("foo").await?;
  assert!(value.is_none());

  let _: () = client.set("foo", "bar", None, None, false).await?;
  let value: String = client.getdel("foo").await?;
  assert_eq!(value, "bar");
  let value: Option<String> = client.get("foo").await?;
  assert!(value.is_none());

  Ok(())
}

pub async fn should_get_strlen(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let expected = "abcdefghijklmnopqrstuvwxyz";
  let _: () = client.set("foo", expected, None, None, false).await?;
  let len: usize = client.strlen("foo").await?;
  assert_eq!(len, expected.len());

  Ok(())
}

pub async fn should_mget_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");
  check_null!(client, "c{1}");

  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into()), ("c{1}", 3.into())];
  for (key, value) in expected.iter() {
    let _: () = client.set(*key, value.clone(), None, None, false).await?;
  }
  let values: Vec<i64> = client.mget(vec!["a{1}", "b{1}", "c{1}"]).await?;
  assert_eq!(values, vec![1, 2, 3]);

  Ok(())
}

pub async fn should_msetnx_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");

  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into())];

  // do it first, check they're there
  let values: i64 = client.msetnx(expected.clone()).await?;
  assert_eq!(values, 1);
  let a: i64 = client.get("a{1}").await?;
  let b: i64 = client.get("b{1}").await?;
  assert_eq!(a, 1);
  assert_eq!(b, 2);

  let _: () = client.del(vec!["a{1}", "b{1}"]).await?;
  let _: () = client.set("a{1}", 3, None, None, false).await?;

  let values: i64 = client.msetnx(expected.clone()).await?;
  assert_eq!(values, 0);
  let b: Option<i64> = client.get("b{1}").await?;
  assert!(b.is_none());

  Ok(())
}

pub async fn should_copy_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");

  let _: () = client.set("a{1}", "bar", None, None, false).await?;
  let result: i64 = client.copy("a{1}", "b{1}", None, false).await?;
  assert_eq!(result, 1);

  let b: String = client.get("b{1}").await?;
  assert_eq!(b, "bar");

  let _: () = client.set("a{1}", "baz", None, None, false).await?;
  let result: i64 = client.copy("a{1}", "b{1}", None, false).await?;
  assert_eq!(result, 0);

  let result: i64 = client.copy("a{1}", "b{1}", None, true).await?;
  assert_eq!(result, 1);
  let b: String = client.get("b{1}").await?;
  assert_eq!(b, "baz");

  Ok(())
}
