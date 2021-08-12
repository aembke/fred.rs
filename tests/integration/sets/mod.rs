use fred::prelude::*;
use std::collections::HashSet;

#[cfg(feature = "index-map")]
use indexmap::set::IndexSet;

fn vec_to_set(data: Vec<RedisValue>) -> HashSet<RedisValue> {
  let mut out = HashSet::with_capacity(data.len());
  for value in data.into_iter() {
    out.insert(value);
  }
  out
}

#[cfg(feature = "index-map")]
fn sets_eq(lhs: &IndexSet<RedisValue>, rhs: &HashSet<RedisValue>) -> bool {
  let lhs: HashSet<RedisValue> = lhs.iter().map(|v| v.clone()).collect();
  &lhs == rhs
}

#[cfg(not(feature = "index-map"))]
fn sets_eq(lhs: &HashSet<RedisValue>, rhs: &HashSet<RedisValue>) -> bool {
  lhs == rhs
}

pub async fn should_sadd_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.sadd("foo", "a").await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.sadd("foo", vec!["b", "c"]).await?;
  assert_eq!(result.as_i64().unwrap(), 2);
  let result = client.sadd("foo", vec!["c", "d"]).await?;
  assert_eq!(result.as_i64().unwrap(), 1);

  Ok(())
}

pub async fn should_scard_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.scard("foo").await?;
  assert_eq!(result.as_i64().unwrap(), 0);

  let result = client.sadd("foo", vec!["1", "2", "3", "4", "5"]).await?;
  assert_eq!(result.as_i64().unwrap(), 5);
  let result = client.scard("foo").await?;
  assert_eq!(result.as_i64().unwrap(), 5);

  Ok(())
}

pub async fn should_sdiff_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", vec![3, 4, 5, 6, 7, 8]).await?;
  let result = client.sdiff(vec!["foo{1}", "bar{1}"]).await?;

  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec!["1".into(), "2".into()])
  ));

  Ok(())
}

pub async fn should_sdiffstore_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", vec![3, 4, 5, 6, 7, 8]).await?;
  let result = client.sdiffstore("baz{1}", vec!["foo{1}", "bar{1}"]).await?;
  assert_eq!(result.as_i64().unwrap(), 2);
  let result = client.smembers("baz{1}").await?;

  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec!["1".into(), "2".into()])
  ));

  Ok(())
}

pub async fn should_sinter_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", vec![3, 4, 5, 6, 7, 8]).await?;
  let result = client.sinter(vec!["foo{1}", "bar{1}"]).await?;

  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec!["3".into(), "4".into(), "5".into(), "6".into()])
  ));

  Ok(())
}

pub async fn should_sinterstore_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", vec![3, 4, 5, 6, 7, 8]).await?;
  let result = client.sinterstore("baz{1}", vec!["foo{1}", "bar{1}"]).await?;
  assert_eq!(result.as_i64().unwrap(), 4);
  let result = client.smembers("baz{1}").await?;

  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec!["3".into(), "4".into(), "5".into(), "6".into()])
  ));

  Ok(())
}

pub async fn should_check_sismember(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.sadd("foo", vec![1, 2, 3, 4, 5, 6]).await?;

  let result = client.sismember("foo", 1).await?;
  assert_eq!(result.as_bool().unwrap(), true);
  let result = client.sismember("foo", 7).await?;
  assert_eq!(result.as_bool().unwrap(), false);

  Ok(())
}

pub async fn should_check_smismember(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = client.sadd("foo", vec![1, 2, 3, 4, 5, 6]).await?;

  let result = client.smismember("foo", vec![1, 2, 7]).await?;
  let result = result.into_array();
  assert_eq!(result[0].as_bool().unwrap(), true);
  assert_eq!(result[1].as_bool().unwrap(), true);
  assert_eq!(result[2].as_bool().unwrap(), false);

  let result = client.sismember("foo", 7).await?;
  assert_eq!(result.as_bool().unwrap(), false);

  Ok(())
}

pub async fn should_read_smembers(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let _ = client.sadd("foo", vec![1, 2, 3, 4, 5, 6]).await?;
  let result = client.smembers("foo").await?;
  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec![
      "1".into(),
      "2".into(),
      "3".into(),
      "4".into(),
      "5".into(),
      "6".into()
    ])
  ));

  Ok(())
}

pub async fn should_smove_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", 5).await?;

  let result = client.smove("foo{1}", "bar{1}", 7).await?;
  assert_eq!(result.as_i64().unwrap(), 0);
  let result = client.smove("foo{1}", "bar{1}", 5).await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.smove("foo{1}", "bar{1}", 1).await?;
  assert_eq!(result.as_i64().unwrap(), 1);

  let foo = client.smembers("foo{1}").await?;
  let bar = client.smembers("bar{1}").await?;
  assert!(sets_eq(
    &foo.into_set().unwrap(),
    &vec_to_set(vec!["2".into(), "3".into(), "4".into(), "6".into()])
  ));
  assert!(sets_eq(
    &bar.into_set().unwrap(),
    &vec_to_set(vec!["5".into(), "1".into()])
  ));

  Ok(())
}

pub async fn should_spop_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let expected = vec_to_set(vec!["1".into(), "2".into(), "3".into()]);
  let _ = client.sadd("foo", vec![1, 2, 3]).await?;

  let result = client.spop("foo", None).await?;
  assert!(expected.contains(&result));

  let result = client.spop("foo", Some(3)).await?;
  for value in result.into_array() {
    assert!(expected.contains(&value));
  }

  Ok(())
}

pub async fn should_get_random_member(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let expected = vec_to_set(vec!["1".into(), "2".into(), "3".into()]);
  let _ = client.sadd("foo", vec![1, 2, 3]).await?;

  let result = client.srandmember("foo", None).await?;
  assert!(expected.contains(&result));
  let result = client.srandmember("foo", Some(3)).await?;
  for value in result.into_array() {
    assert!(expected.contains(&value));
  }

  Ok(())
}

pub async fn should_remove_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result = client.srem("foo", 1).await?;
  assert_eq!(result.as_i64().unwrap(), 0);

  let _ = client.sadd("foo", vec![1, 2, 3, 4, 5, 6]).await?;
  let result = client.srem("foo", 1).await?;
  assert_eq!(result.as_i64().unwrap(), 1);
  let result = client.srem("foo", vec![2, 3, 4, 7]).await?;
  assert_eq!(result.as_i64().unwrap(), 3);

  let result = client.smembers("foo").await?;
  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec!["5".into(), "6".into()])
  ));

  Ok(())
}

pub async fn should_sunion_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", vec![3, 4, 5, 6, 7, 8]).await?;
  let result = client.sunion(vec!["foo{1}", "bar{1}"]).await?;

  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec![
      "1".into(),
      "2".into(),
      "3".into(),
      "4".into(),
      "5".into(),
      "6".into(),
      "7".into(),
      "8".into()
    ])
  ));

  Ok(())
}

pub async fn should_sunionstore_elements(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let _ = client.sadd("foo{1}", vec![1, 2, 3, 4, 5, 6]).await?;
  let _ = client.sadd("bar{1}", vec![3, 4, 5, 6, 7, 8]).await?;
  let result = client.sunionstore("baz{1}", vec!["foo{1}", "bar{1}"]).await?;
  assert_eq!(result.as_i64().unwrap(), 8);
  let result = client.smembers("baz{1}").await?;

  assert!(sets_eq(
    &result.into_set().unwrap(),
    &vec_to_set(vec![
      "1".into(),
      "2".into(),
      "3".into(),
      "4".into(),
      "5".into(),
      "6".into(),
      "7".into(),
      "8".into()
    ])
  ));

  Ok(())
}
