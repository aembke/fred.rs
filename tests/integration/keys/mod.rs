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

  let _ = client.set("foo", "bar", None, None, false).await?;

  assert_eq!(client.get("foo").await?, "bar".into());
  Ok(())
}

pub async fn should_set_and_del_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.set("foo", "bar", None, None, true).await?;
  assert!(result.is_null());

  assert_eq!(client.get("foo").await?, "bar".into());
  assert_eq!(client.del("foo").await?.as_i64(), Some(1));

  check_null!(client, "foo");
  Ok(())
}

pub async fn should_set_with_get_argument(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client.set("foo", "bar", None, None, false).await?;

  let result = client.set("foo", "baz", None, None, true).await?;
  assert_eq!(result, RedisValue::String("bar".into()));

  let result = client.get("foo").await?;
  assert_eq!(result, RedisValue::String("baz".into()));

  Ok(())
}

pub async fn should_incr_and_decr_a_value(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let count = client.incr("foo").await?.as_u64().unwrap();
  assert_eq!(count, 1);
  let count = client.incr_by("foo", 2).await?.as_u64().unwrap();
  assert_eq!(count, 3);
  let count = client.decr("foo").await?.as_u64().unwrap();
  assert_eq!(count, 2);
  let count = client.decr_by("foo", 2).await?.as_u64().unwrap();
  assert_eq!(count, 0);

  Ok(())
}

pub async fn should_incr_by_float(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let count = client.incr_by_float("foo", 1.5).await?.as_f64().unwrap();
  assert_eq!(count, 1.5);
  let count = client.incr_by_float("foo", 2.2).await?.as_f64().unwrap();
  assert_eq!(count, 3.7);
  let count = client.incr_by_float("foo", -1.2).await?.as_f64().unwrap();
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
  let a = client.get("a{1}").await?;
  let b = client.get("b{1}").await?;
  let c = client.get("c{1}").await?;

  assert_eq!(a.as_i64().unwrap(), 1);
  assert_eq!(b.as_i64().unwrap(), 2);
  assert_eq!(c.as_i64().unwrap(), 3);

  Ok(())
}

// should panic
pub async fn should_error_mset_empty_map(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  client.mset(RedisMap::new()).await.map(|_| ())
}

pub async fn should_expire_key(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.set("foo", "bar", None, None, false).await?;

  let _ = client.expire("foo", 1).await?;
  sleep(Duration::from_millis(1500)).await;
  let foo = client.get("foo").await?;
  assert!(foo.is_null());

  Ok(())
}

pub async fn should_persist_key(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let removed = client.persist("foo").await?.as_bool().unwrap();
  assert!(removed);

  let ttl = client.ttl("foo").await?;
  assert_eq!(ttl.as_i64().unwrap(), -1);

  Ok(())
}

pub async fn should_check_ttl(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let ttl = client.ttl("foo").await?.as_i64().unwrap();
  assert!(ttl > 0 && ttl < 6);

  Ok(())
}

pub async fn should_check_pttl(client: RedisClient, _config: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.set("foo", "bar", Some(Expiration::EX(5)), None, false).await?;

  let ttl = client.pttl("foo").await?.as_i64().unwrap();
  assert!(ttl > 0 && ttl < 5001);

  Ok(())
}

pub async fn should_dump_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client.set("foo", "abc123", None, None, false).await?;
  let dump = client.dump("foo").await?;
  assert!(dump.is_bytes());

  Ok(())
}

pub async fn should_dump_and_restore_key(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let expected = "abc123";

  let _ = client.set("foo", expected, None, None, false).await?;
  let dump = client.dump("foo").await?;
  let _ = client.del("foo").await?;

  let _ = client.restore("foo", 0, dump, false, false, None, None).await?;
  let value = client.get("foo").await?;
  assert_eq!(value.as_str().unwrap(), expected);

  Ok(())
}

pub async fn should_modify_ranges(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.set("foo", "0123456789", None, None, false).await?;

  let range = client.getrange("foo", 0, 4).await?;
  assert_eq!(range.as_str().unwrap(), "01234");

  let _ = client.setrange("foo", 4, "abc").await?;
  let value = client.get("foo").await?;
  assert_eq!(value.as_str().unwrap(), "0123abc789");

  Ok(())
}

pub async fn should_getset_value(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let value = client.getset("foo", "bar").await?;
  assert!(value.is_null());
  let value = client.getset("foo", "baz").await?;
  assert_eq!(value.as_str().unwrap(), "bar");
  let value = client.get("foo").await?;
  assert_eq!(value.as_str().unwrap(), "baz");

  Ok(())
}

pub async fn should_getdel_value(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let value = client.getdel("foo").await?;
  assert!(value.is_null());

  let _ = client.set("foo", "bar", None, None, false).await?;
  let value = client.getdel("foo").await?;
  assert_eq!(value.as_str().unwrap(), "bar");
  let value = client.get("foo").await?;
  assert!(value.is_null());

  Ok(())
}

pub async fn should_get_strlen(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let expected = "abcdefghijklmnopqrstuvwxyz";
  let _ = client.set("foo", expected, None, None, false).await?;
  let len = client.strlen("foo").await?;
  assert_eq!(len.as_usize().unwrap(), expected.len());

  Ok(())
}

pub async fn should_mget_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");
  check_null!(client, "c{1}");

  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into()), ("c{1}", 3.into())];
  for (key, value) in expected.iter() {
    let _ = client.set(*key, value.clone(), None, None, false).await?;
  }
  let values = client.mget(vec!["a{1}", "b{1}", "c{1}"]).await?;

  if let RedisValue::Array(values) = values {
    // redis does send these back as bulk strings, not integers, which is why `into_integer` exists on RedisValue
    assert_eq!(values, vec!["1".into(), "2".into(), "3".into()]);
  } else {
    return_err!("Expected redis map");
  }

  Ok(())
}

pub async fn should_msetnx_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");

  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into())];

  // do it first, check they're there
  let values = client.msetnx(expected.clone()).await?;
  assert_eq!(values.as_i64().unwrap(), 1);
  let a = client.get("a{1}").await?;
  let b = client.get("b{1}").await?;
  assert_eq!(a.as_i64().unwrap(), 1);
  assert_eq!(b.as_i64().unwrap(), 2);

  let _ = client.del(vec!["a{1}", "b{1}"]).await?;
  let _ = client.set("a{1}", 3, None, None, false).await?;

  let values = client.msetnx(expected.clone()).await?;
  assert_eq!(values.as_i64().unwrap(), 0);
  assert!(!values.as_bool().unwrap());
  let b = client.get("b{1}").await?;
  assert!(b.is_null());

  Ok(())
}

pub async fn should_copy_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");

  let _ = client.set("a{1}", "bar", None, None, false).await?;
  let result = client.copy("a{1}", "b{1}", None, false).await?;
  assert_eq!(result.as_i64().unwrap(), 1);

  let b = client.get("b{1}").await?;
  assert_eq!(b.as_str().unwrap(), "bar");

  let _ = client.set("a{1}", "baz", None, None, false).await?;
  let result = client.copy("a{1}", "b{1}", None, false).await?;
  assert_eq!(result.as_i64().unwrap(), 0);

  let result = client.copy("a{1}", "b{1}", None, true).await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let b = client.get("b{1}").await?;
  assert_eq!(b.as_str().unwrap(), "baz");

  Ok(())
}
