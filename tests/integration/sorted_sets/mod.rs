use float_cmp::approx_eq;
use fred::{
  prelude::*,
  types::{Ordering, ZRange, ZRangeKind, ZSort},
};
use std::{cmp::Ordering as CmpOrdering, convert::TryInto, time::Duration};
use tokio::time::sleep;

const COUNT: i64 = 10;

fn f64_cmp(lhs: f64, rhs: f64) -> CmpOrdering {
  if approx_eq!(f64, lhs, rhs, ulps = 2) {
    CmpOrdering::Equal
  } else if lhs < rhs {
    CmpOrdering::Less
  } else {
    CmpOrdering::Greater
  }
}

async fn create_lex_data(client: &RedisClient, key: &str) -> Result<Vec<(f64, RedisValue)>, RedisError> {
  let values: Vec<(f64, String)> = "abcdefghijklmnopqrstuvwxyz"
    .chars()
    .map(|c| (0.0, c.to_string()))
    .collect();

  client.zadd(key, None, None, false, false, values.clone()).await?;
  Ok(values.into_iter().map(|(f, v)| (f, v.into())).collect())
}

async fn create_count_data(client: &RedisClient, key: &str) -> Result<Vec<(f64, RedisValue)>, RedisError> {
  let values: Vec<(f64, RedisValue)> = (0 .. COUNT)
    .into_iter()
    .map(|idx| (idx as f64, idx.to_string().into()))
    .collect();

  client.zadd(key, None, None, false, false, values.clone()).await?;
  Ok(values)
}

pub async fn should_bzpopmin(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let publisher_client = client.clone_new();
  let policy = client.client_reconnect_policy();
  let _ = publisher_client.connect(policy);
  publisher_client.wait_for_connect().await?;

  let jh = tokio::task::spawn(async move {
    for idx in 0 .. COUNT {
      let result: (String, i64, f64) = client.bzpopmin("foo", 60.0).await?;
      assert_eq!(result, ("foo".into(), idx, idx as f64));
    }

    Ok::<(), RedisError>(())
  });

  for idx in 0 .. COUNT {
    let result: i64 = publisher_client
      .zadd("foo", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let _ = jh.await?;
  Ok(())
}

pub async fn should_bzpopmax(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let publisher_client = client.clone_new();
  let policy = client.client_reconnect_policy();
  let _ = publisher_client.connect(policy);
  publisher_client.wait_for_connect().await?;

  let jh = tokio::task::spawn(async move {
    for idx in 0 .. COUNT {
      let result: (String, i64, f64) = client.bzpopmax("foo", 60.0).await?;
      assert_eq!(result, ("foo".into(), idx, idx as f64));
    }

    Ok::<(), RedisError>(())
  });

  for idx in 0 .. COUNT {
    sleep(Duration::from_millis(50)).await;

    let result: i64 = publisher_client
      .zadd("foo", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let _ = jh.await?;
  Ok(())
}

pub async fn should_zadd_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result: i64 = client
    .zadd("foo", None, None, false, false, vec![(0.0, 0), (1.0, 1)])
    .await?;
  assert_eq!(result, 2);

  for idx in 2 .. COUNT {
    let value: i64 = client.zadd("foo", None, None, false, false, (idx as f64, idx)).await?;
    assert_eq!(value, 1);
  }

  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, COUNT);

  let result: f64 = client.zadd("foo", None, None, true, true, (0.1_f64, 0_i64)).await?;
  assert!(approx_eq!(f64, result, 0.1, ulps = 2));

  let result: i64 = client
    .zadd(
      "foo",
      Some(SetOptions::NX),
      None,
      true,
      false,
      ((COUNT + 1) as f64, COUNT + 1),
    )
    .await?;
  assert_eq!(result, 1);
  let result: i64 = client
    .zadd(
      "foo",
      Some(SetOptions::XX),
      None,
      true,
      false,
      ((COUNT + 2) as f64, COUNT + 2),
    )
    .await?;
  assert_eq!(result, 0);

  let result: i64 = client
    .zadd(
      "foo",
      None,
      Some(Ordering::GreaterThan),
      true,
      false,
      (COUNT as f64, COUNT + 1),
    )
    .await?;
  assert_eq!(result, 0);
  let result: i64 = client
    .zadd(
      "foo",
      None,
      Some(Ordering::LessThan),
      true,
      false,
      ((COUNT + 2) as f64, COUNT + 1),
    )
    .await?;
  assert_eq!(result, 0);
  let result: i64 = client
    .zadd(
      "foo",
      None,
      Some(Ordering::GreaterThan),
      true,
      false,
      ((COUNT + 2) as f64, COUNT + 1),
    )
    .await?;
  assert_eq!(result, 1);
  let result: i64 = client
    .zadd(
      "foo",
      None,
      Some(Ordering::LessThan),
      true,
      false,
      ((COUNT + 1) as f64, COUNT + 1),
    )
    .await?;
  assert_eq!(result, 1);

  Ok(())
}

pub async fn should_zcard_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  check_null!(client, "bar");

  for idx in 0 .. COUNT {
    let values = vec![(idx as f64, idx), ((idx + COUNT) as f64, idx + COUNT)];
    let result: i64 = client.zadd("foo", None, None, false, false, values).await?;
    assert_eq!(result, 2);
  }

  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, COUNT * 2);
  let result: i64 = client.zcard("bar").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zcount_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0 .. COUNT {
    let values = vec![(idx as f64, idx), ((idx + COUNT) as f64, idx + COUNT)];
    let result: i64 = client.zadd("foo", None, None, false, false, values).await?;
    assert_eq!(result, 2);
  }

  let result: i64 = client.zcount("foo", 0.0, (COUNT * 2) as f64).await?;
  assert_eq!(result, COUNT * 2);
  let result: i64 = client.zcount("foo", 0.0, COUNT as f64 - 0.1).await?;
  assert_eq!(result, COUNT);
  let result: i64 = client.zcount("foo", -1.0, -0.1).await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zdiff_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");

  let mut expected: Vec<(f64, RedisValue)> = Vec::with_capacity(COUNT as usize);
  for idx in 0 .. COUNT {
    expected.push((idx as f64, idx.to_string().into()));
    let result: i64 = client
      .zadd("foo{1}", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let result: Vec<RedisValue> = client.zdiff(vec!["foo{1}", "bar{1}"], false).await?;
  let _expected: Vec<RedisValue> = expected.iter().map(|(_, v)| v.clone()).collect();
  assert_eq!(result, _expected);

  client
    .zadd(
      "bar{1}",
      None,
      None,
      false,
      false,
      expected[0 .. expected.len() - 1].to_vec(),
    )
    .await?;
  let result: RedisValue = client.zdiff(vec!["foo{1}", "bar{1}"], true).await?;
  let expected: Vec<(RedisValue, f64)> = expected.into_iter().map(|(s, v)| (v, s)).collect();
  assert_eq!(
    result.into_zset_result().unwrap(),
    expected[expected.len() - 1 ..].to_vec()
  );

  Ok(())
}

pub async fn should_zdiffstore_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let mut expected: Vec<(f64, RedisValue)> = Vec::with_capacity(COUNT as usize);
  for idx in 0 .. COUNT {
    expected.push((idx as f64, idx.to_string().into()));
    let result: i64 = client
      .zadd("foo{1}", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let result: i64 = client.zdiffstore("baz{1}", vec!["foo{1}", "bar{1}"]).await?;
  assert_eq!(result, COUNT);

  client
    .zadd(
      "bar{1}",
      None,
      None,
      false,
      false,
      expected[0 .. expected.len() - 1].to_vec(),
    )
    .await?;
  let result: i64 = client.zdiffstore("baz{1}", vec!["foo{1}", "bar{1}"]).await?;
  assert_eq!(result, 1);

  Ok(())
}

pub async fn should_zincrby_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");

  let result: f64 = client.zincrby("foo", 1.0, "a").await?;
  assert_eq!(result, 1.0);
  let result: f64 = client.zincrby("foo", 2.5, "a").await?;
  assert_eq!(result, 3.5);
  let result: f64 = client.zincrby("foo", 1.2, "b").await?;
  assert_eq!(result, 1.2);

  Ok(())
}

pub async fn should_zinter_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");

  let mut expected: Vec<(f64, RedisValue)> = Vec::with_capacity(COUNT as usize);
  for idx in 0 .. COUNT {
    expected.push((idx as f64, idx.to_string().into()));
    let result: i64 = client
      .zadd("foo{1}", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let result: Vec<RedisValue> = client.zinter(vec!["foo{1}", "bar{1}"], None, None, false).await?;
  assert!(result.is_empty());

  client
    .zadd(
      "bar{1}",
      None,
      None,
      false,
      false,
      expected[0 .. expected.len() - 1].to_vec(),
    )
    .await?;
  let result: RedisValue = client.zinter(vec!["foo{1}", "bar{1}"], None, None, true).await?;
  // scores are added together with a weight of 1 in this example
  let mut expected: Vec<(RedisValue, f64)> = expected.into_iter().map(|(s, v)| (v, s * 2.0)).collect();
  // zinter returns results in descending order based on score
  expected.reverse();

  assert_eq!(
    result.into_zset_result().unwrap(),
    expected[1 .. expected.len()].to_vec()
  );
  Ok(())
}

pub async fn should_zinterstore_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let mut expected: Vec<(f64, RedisValue)> = Vec::with_capacity(COUNT as usize);
  for idx in 0 .. COUNT {
    expected.push((idx as f64, idx.to_string().into()));
    let result: i64 = client
      .zadd("foo{1}", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let result: i64 = client
    .zinterstore("baz{1}", vec!["foo{1}", "bar{1}"], None, None)
    .await?;
  assert_eq!(result, 0);

  client
    .zadd(
      "bar{1}",
      None,
      None,
      false,
      false,
      expected[0 .. expected.len() - 1].to_vec(),
    )
    .await?;
  let result: i64 = client
    .zinterstore("baz{1}", vec!["foo{1}", "bar{1}"], None, None)
    .await?;
  assert_eq!(result, COUNT - 1);

  Ok(())
}

pub async fn should_zlexcount(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo");
  let _ = create_lex_data(&client, "foo").await?;

  let result: i64 = client.zlexcount("foo", "-", "+").await?;
  assert_eq!(result, 26);
  let result: i64 = client.zlexcount("foo", "a", "b").await?;
  assert_eq!(result, 2);
  let result: i64 = client.zlexcount("foo", "a", "(b").await?;
  assert_eq!(result, 1);

  Ok(())
}

pub async fn should_zpopmax(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  for idx in 0 .. COUNT {
    let result: RedisValue = client.zpopmax("foo", None).await?;
    let (member, score) = result.into_zset_result().unwrap().pop().unwrap();
    assert_eq!(score, (COUNT - idx - 1) as f64);
    assert_eq!(member, (COUNT - idx - 1).to_string().into());
  }
  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zpopmin(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  for idx in 0 .. COUNT {
    let result: RedisValue = client.zpopmin("foo", None).await?;
    let (member, score) = result.into_zset_result().unwrap().pop().unwrap();
    assert_eq!(score, idx as f64);
    assert_eq!(member, idx.to_string().into());
  }
  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zrandmember(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  for _ in 0 .. COUNT * 2 {
    let result: RedisValue = client.zrandmember("foo", Some((1, true))).await?;
    let (member, score) = result.into_zset_result().unwrap().pop().unwrap();
    assert!(score >= 0.0 && score < COUNT as f64);
    assert_eq!(member.into_string().unwrap(), score.to_string());
  }

  let result: RedisValue = client.zrandmember("foo", Some((COUNT, true))).await?;
  let result = result.into_zset_result().unwrap();
  for (member, score) in result.into_iter() {
    assert!(score >= 0.0 && score < COUNT as f64);
    assert_eq!(member.into_string().unwrap(), score.to_string())
  }

  Ok(())
}

pub async fn should_zrangestore_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  let _ = create_count_data(&client, "foo{1}").await?;

  let result: i64 = client
    .zrangestore("bar{1}", "foo{1}", 0, COUNT, None, false, None)
    .await?;
  assert_eq!(result, COUNT);
  let result: i64 = client.zcard("bar{1}").await?;
  assert_eq!(result, COUNT);

  Ok(())
}

pub async fn should_zrangebylex(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_lex_data(&client, "foo").await?;
  let expected_values: Vec<RedisValue> = expected.iter().map(|(_, v)| v.clone()).collect();

  let old_result: RedisValue = client.zrangebylex("foo", "-", "+", None).await?;
  let new_result = client
    .zrange("foo", "-", "+", Some(ZSort::ByLex), false, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values);

  let old_result: RedisValue = client.zrangebylex("foo", "a", "[c", None).await?;
  let new_result = client
    .zrange("foo", "a", "[c", Some(ZSort::ByLex), false, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values[0 .. 3]);

  Ok(())
}

pub async fn should_zrevrangebylex(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_lex_data(&client, "foo").await?;
  let mut expected_values: Vec<RedisValue> = expected.iter().map(|(_, v)| v.clone()).collect();
  expected_values.reverse();

  let old_result: RedisValue = client.zrevrangebylex("foo", "+", "-", None).await?;
  let new_result = client
    .zrange("foo", "+", "-", Some(ZSort::ByLex), true, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values);

  let old_result: RedisValue = client.zrevrangebylex("foo", "c", "[a", None).await?;
  let new_result = client
    .zrange("foo", "[c", "a", Some(ZSort::ByLex), true, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values[expected_values.len() - 3 ..]);

  Ok(())
}

pub async fn should_zrangebyscore(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;
  let expected_values: Vec<RedisValue> = expected.iter().map(|(_, v)| v.clone()).collect();

  let old_result: RedisValue = client.zrangebyscore("foo", "-inf", "+inf", false, None).await?;
  let new_result = client
    .zrange("foo", "-inf", "+inf", Some(ZSort::ByScore), false, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values);

  let old_result: RedisValue = client
    .zrangebyscore("foo", (COUNT / 2) as f64, COUNT as f64, false, None)
    .await?;
  let new_result = client
    .zrange(
      "foo",
      (COUNT / 2) as f64,
      COUNT as f64,
      Some(ZSort::ByScore),
      false,
      None,
      false,
    )
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values[(COUNT / 2) as usize ..]);

  let lower = ZRange {
    kind:  ZRangeKind::Inclusive,
    range: ((COUNT / 2) as f64).try_into()?,
  };
  let upper = ZRange {
    kind:  ZRangeKind::Inclusive,
    range: (COUNT as f64).try_into()?,
  };
  let old_result: RedisValue = client.zrangebyscore("foo", &lower, &upper, false, None).await?;
  let new_result = client
    .zrange("foo", &lower, &upper, Some(ZSort::ByScore), false, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values[(COUNT / 2) as usize ..]);

  Ok(())
}

pub async fn should_zrevrangebyscore(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;
  let mut expected_values: Vec<RedisValue> = expected.iter().map(|(_, v)| v.clone()).collect();
  expected_values.reverse();

  let old_result: RedisValue = client.zrevrangebyscore("foo", "+inf", "-inf", false, None).await?;
  let new_result = client
    .zrange("foo", "+inf", "-inf", Some(ZSort::ByScore), true, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values);

  let old_result: RedisValue = client
    .zrevrangebyscore("foo", COUNT as f64, (COUNT / 2) as f64, false, None)
    .await?;
  let new_result = client
    .zrange(
      "foo",
      COUNT as f64,
      (COUNT / 2) as f64,
      Some(ZSort::ByScore),
      true,
      None,
      false,
    )
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values[0 .. (COUNT / 2) as usize]);

  let lower = ZRange {
    kind:  ZRangeKind::Inclusive,
    range: ((COUNT / 2) as f64).try_into()?,
  };
  let upper = ZRange {
    kind:  ZRangeKind::Inclusive,
    range: (COUNT as f64).try_into()?,
  };
  let old_result: RedisValue = client.zrevrangebyscore("foo", &upper, &lower, false, None).await?;
  let new_result = client
    .zrange("foo", &upper, &lower, Some(ZSort::ByScore), true, None, false)
    .await?;
  assert_eq!(old_result, new_result);
  assert_eq!(old_result.into_array(), expected_values[0 .. (COUNT / 2) as usize]);

  Ok(())
}

pub async fn should_zrank_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  for idx in 0 .. COUNT {
    let result: i64 = client.zrank("foo", idx).await?;
    assert_eq!(result, idx);
  }

  let result: Option<i64> = client.zrank("foo", COUNT + 1).await?;
  assert!(result.is_none());

  Ok(())
}

pub async fn should_zrem_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  let result: i64 = client.zrem("foo", COUNT + 1).await?;
  assert_eq!(result, 0);

  for idx in 0 .. COUNT {
    let result: i64 = client.zrem("foo", idx).await?;
    assert_eq!(result, 1);
    let result: i64 = client.zcard("foo").await?;
    assert_eq!(result, COUNT - (idx + 1));
  }

  let result: i64 = client.zrem("foo", 0).await?;
  assert_eq!(result, 0);
  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zremrangebylex(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_lex_data(&client, "foo").await?;
  let result: usize = client.zremrangebylex("foo", "-", "+").await?;
  assert_eq!(result, expected.len());
  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, 0);

  let _ = create_lex_data(&client, "foo").await?;
  for (_, value) in expected.iter() {
    let value_str = value.as_str().unwrap().to_string();

    let result: i64 = client.zremrangebylex("foo", &value_str, &value_str).await?;
    assert_eq!(result, 1);
  }
  let result: usize = client.zcard("foo").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zremrangebyrank(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;
  let result: usize = client.zremrangebyrank("foo", 0, COUNT).await?;
  assert_eq!(result, expected.len());
  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, 0);

  let _ = create_count_data(&client, "foo").await?;
  for _ in 0 .. COUNT {
    // this modifies the set so the idx cant change
    let result: usize = client.zremrangebyrank("foo", 0, 0).await?;
    assert_eq!(result, 1);
  }
  let result: usize = client.zcard("foo").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zremrangebyscore(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;
  let result: usize = client.zremrangebyscore("foo", 0 as f64, COUNT as f64).await?;
  assert_eq!(result, expected.len());
  let result: i64 = client.zcard("foo").await?;
  assert_eq!(result, 0);

  let _ = create_count_data(&client, "foo").await?;
  for idx in 0 .. COUNT {
    let result: usize = client.zremrangebyscore("foo", idx as f64, idx as f64).await?;
    assert_eq!(result, 1);
  }
  let result: usize = client.zcard("foo").await?;
  assert_eq!(result, 0);

  Ok(())
}

pub async fn should_zrevrank_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  let result: Option<i64> = client.zrevrank("foo", COUNT + 1).await?;
  assert!(result.is_none());

  for idx in 0 .. COUNT {
    let result: i64 = client.zrevrank("foo", idx).await?;
    assert_eq!(result, COUNT - (idx + 1));
  }

  Ok(())
}

pub async fn should_zscore_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  for idx in 0 .. COUNT {
    let result: f64 = client.zscore("foo", idx).await?;
    assert_eq!(result, idx as f64);
  }

  let result: Option<f64> = client.zscore("foo", COUNT + 1).await?;
  assert!(result.is_none());

  Ok(())
}

pub async fn should_zunion_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");

  let mut expected: Vec<(f64, RedisValue)> = Vec::with_capacity(COUNT as usize);
  for idx in 0 .. COUNT {
    expected.push((idx as f64, idx.to_string().into()));
    let result: i64 = client
      .zadd("foo{1}", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let result = client.zunion(vec!["foo{1}", "bar{1}"], None, None, false).await?;
  let _expected: Vec<RedisValue> = expected.iter().map(|(_, v)| v.clone()).collect();
  assert_eq!(result.into_array(), _expected);

  client
    .zadd(
      "bar{1}",
      None,
      None,
      false,
      false,
      expected[0 .. expected.len() - 1].to_vec(),
    )
    .await?;
  let result = client.zunion(vec!["foo{1}", "bar{1}"], None, None, true).await?;
  // scores are added together with a weight of 1 in this example
  let mut _expected: Vec<(RedisValue, f64)> = expected[0 .. expected.len() - 1]
    .iter()
    .map(|(s, v)| (v.clone(), s * 2.0))
    .collect();

  let (score, value) = expected.last().unwrap().clone();
  _expected.push((value, score));

  // zinter returns results in descending order based on score
  _expected.sort_by(|(_, a), (_, b)| f64_cmp(*b, *a));

  assert_eq!(result.into_zset_result().unwrap(), _expected.to_vec());
  Ok(())
}

pub async fn should_zunionstore_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  check_null!(client, "bar{1}");
  check_null!(client, "baz{1}");

  let mut expected: Vec<(f64, RedisValue)> = Vec::with_capacity(COUNT as usize);
  for idx in 0 .. COUNT {
    expected.push((idx as f64, idx.to_string().into()));
    let result: i64 = client
      .zadd("foo{1}", None, None, false, false, (idx as f64, idx))
      .await?;
    assert_eq!(result, 1);
  }

  let result: i64 = client
    .zunionstore("baz{1}", vec!["foo{1}", "bar{1}"], None, None)
    .await?;
  assert_eq!(result, COUNT);

  client
    .zadd(
      "bar{1}",
      None,
      None,
      false,
      false,
      expected[0 .. expected.len() - 1].to_vec(),
    )
    .await?;
  let result: i64 = client
    .zunionstore("baz{1}", vec!["foo{1}", "bar{1}"], None, None)
    .await?;
  assert_eq!(result, COUNT);

  Ok(())
}

pub async fn should_zmscore_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0 .. COUNT {
    client.zadd("foo", None, None, false, false, (idx as f64, idx)).await?;
  }

  let result: Vec<f64> = client.zmscore("foo", vec![0, 1]).await?;
  assert_eq!(result, vec![0.0, 1.0]);
  let result: Option<f64> = client.zmscore("foo", vec![11]).await?;
  assert!(result.is_none());

  Ok(())
}
