use fred::client::RedisClient;
use fred::error::RedisError;
use fred::types::{RedisConfig, RedisMap, RedisValue};

use std::collections::{HashSet};


fn assert_contains<'a, T: Eq + PartialEq>(values: Vec<T>, item: &'a T) {
  for value in values.iter() {
    if value == item {
      return;
    }
  }

  panic!("Failed to find item in set.");
}

fn assert_diff_len(values: Vec<&'static str>, value: RedisValue, len: usize) {
  if let RedisValue::Array(items) = value {
    let mut expected = HashSet::with_capacity(values.len());
    for value in values.into_iter() {
      expected.insert(value.to_owned());
    }
    let mut actual = HashSet::with_capacity(items.len());
    for item in items.into_iter() {
      let s = &*item.as_str().unwrap();
      actual.insert(s.to_owned());
    }

    let diff = expected.difference(&actual).fold(0, |m, _| m + 1);
    assert_eq!(diff, len);
  } else {
    panic!("Expected value array");
  }
}

pub async fn should_hset_and_hget(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.hset("foo", ("a", 1.into())).await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.hset("foo", vec![("b", 2.into()), ("c", 3.into())]).await?;
  assert_eq!(result.as_i64().unwrap(), 2);

  let a = client.hget("foo", "a").await?;
  assert_eq!(a.as_i64().unwrap(), 1);
  let b = client.hget("foo", "b").await?;
  assert_eq!(b.as_i64().unwrap(), 2);
  let c = client.hget("foo", "c").await?;
  assert_eq!(c.as_i64().unwrap(), 3);

  Ok(())
}

pub async fn should_hset_and_hdel(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client
    .hset("foo", vec![("a", 1.into()), ("b", 2.into()), ("c", 3.into())])
    .await?;
  assert_eq!(result.as_i64().unwrap(), 3);
  let result = client.hdel("foo", vec!["a", "b"]).await?;
  assert_eq!(result.as_i64().unwrap(), 2);
  let result = client.hdel("foo", "c").await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.hget("foo", "a").await?;
  assert!(result.is_null());

  Ok(())
}

pub async fn should_hexists(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client.hset("foo", ("a", 1.into())).await?;
  let a = client.hexists("foo", "a").await?;
  assert!(a.as_bool().unwrap());
  let b = client.hexists("foo", "b").await?;
  assert_eq!(b.as_bool().unwrap(), false);

  Ok(())
}

pub async fn should_hgetall(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client
    .hset("foo", vec![("a", 1.into()), ("b", 2.into()), ("c", 3.into())])
    .await?;
  let values = client.hgetall("foo").await?;
  let values_map = values.into_map()?;

  assert_eq!(values_map.len(), 3);
  let mut expected = RedisMap::new();
  expected.insert("a".into(), "1".into());
  expected.insert("b".into(), "2".into());
  expected.insert("c".into(), "3".into());
  assert_eq!(values_map, expected);

  Ok(())
}

pub async fn should_hincryby(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.hincrby("foo", "a", 1).await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.hincrby("foo", "a", 2).await?;
  assert_eq!(result.as_i64().unwrap(), 3);

  Ok(())
}

pub async fn should_hincryby_float(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.hincrbyfloat("foo", "a", 0.5).await?;
  assert_eq!(result.as_f64().unwrap(), 0.5);
  let result = client.hincrbyfloat("foo", "a", 3.7).await?;
  assert_eq!(result.as_f64().unwrap(), 4.2);

  Ok(())
}

pub async fn should_get_keys(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client
    .hset("foo", vec![("a", 1.into()), ("b", 2.into()), ("c", 3.into())])
    .await?;

  let keys = client.hkeys("foo").await?;
  assert_diff_len(vec!["a", "b", "c"], keys, 0);

  Ok(())
}

pub async fn should_hmset(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client
    .hmset("foo", vec![("a", 1.into()), ("b", 2.into()), ("c", 3.into())])
    .await?;

  let a = client.hget("foo", "a").await?;
  assert_eq!(a.as_i64().unwrap(), 1);
  let b = client.hget("foo", "b").await?;
  assert_eq!(b.as_i64().unwrap(), 2);
  let c = client.hget("foo", "c").await?;
  assert_eq!(c.as_i64().unwrap(), 3);

  Ok(())
}

pub async fn should_hmget(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client
    .hmset("foo", vec![("a", 1.into()), ("b", 2.into()), ("c", 3.into())])
    .await?;

  let result = client.hmget("foo", vec!["a", "b"]).await?;
  assert_eq!(result, RedisValue::Array(vec!["1".into(), "2".into()]));

  Ok(())
}

pub async fn should_hsetnx(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client.hset("foo", ("a", 1.into())).await?;
  let result = client.hsetnx("foo", "a", 2).await?;
  assert_eq!(result.as_bool().unwrap(), false);
  let result = client.hget("foo", "a").await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.hsetnx("foo", "b", 2).await?;
  assert!(result.as_bool().unwrap());
  let result = client.hget("foo", "b").await?;
  assert_eq!(result.as_i64().unwrap(), 2);

  Ok(())
}

pub async fn should_get_random_field(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client
    .hmset("foo", vec![("a", 1.into()), ("b", 2.into()), ("c", 3.into())])
    .await?;

  let field = client.hrandfield("foo", None).await?;
  let field = field.as_str().unwrap();
  assert_contains(vec!["a", "b", "c"], &&**&field);

  let fields = client.hrandfield("foo", Some((2, false))).await?;
  assert_diff_len(vec!["a", "b", "c"], fields, 1);

  let fields = client.hrandfield("foo", Some((2, true))).await?;
  let actual = fields.into_map()?;
  assert_eq!(actual.len(), 2);

  let mut expected = RedisMap::new();
  expected.insert("a".into(), "1".into());
  expected.insert("b".into(), "2".into());
  expected.insert("c".into(), "3".into());

  for (key, value) in actual.inner().into_iter() {
    let expected_val = expected.get(&key).unwrap();
    assert_eq!(&value, expected_val);
  }

  Ok(())
}

pub async fn should_get_strlen(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let expected = "abcdefhijklmnopqrstuvwxyz";
  let _ = client.hset("foo", ("a", expected.clone().into())).await?;

  let len = client.hstrlen("foo", "a").await?;
  assert_eq!(len.as_usize().unwrap(), expected.len());

  Ok(())
}

pub async fn should_get_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client.hmset("foo", vec![("a", "1".into()), ("b", "2".into())]).await?;

  let values = client.hvals("foo").await?;
  assert!(values.is_array());
  assert_diff_len(vec!["1", "2"], values, 0);

  Ok(())
}
