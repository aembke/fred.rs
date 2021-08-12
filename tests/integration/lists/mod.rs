use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

const COUNT: i64 = 10;

async fn create_count_data(client: &RedisClient, key: &str) -> Result<Vec<RedisValue>, RedisError> {
  let mut values = Vec::with_capacity(COUNT as usize);
  for idx in 0..COUNT {
    let _ = client.rpush(key, idx).await?;
    values.push(idx.to_string().into());
  }

  Ok(values)
}

pub async fn should_blpop_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let publisher = client.clone_new();
  let _ = publisher.connect(None, !client.is_pipelined());
  let _ = publisher.wait_for_connect().await?;

  let jh = tokio::spawn(async move {
    for idx in 0..COUNT {
      let result = client.blpop("foo", 30.0).await?;
      assert_eq!(result.into_array().pop().unwrap().as_i64().unwrap(), idx);
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0..COUNT {
    // the assertion below checks the length of the list, so we have to make sure not to push faster than elements are removed
    sleep(Duration::from_millis(50)).await;
    let result = publisher.rpush("foo", idx).await?;
    assert_eq!(result.as_i64().unwrap(), 1);
  }

  let _ = jh.await?;
  Ok(())
}

pub async fn should_brpop_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let publisher = client.clone_new();
  let _ = publisher.connect(None, !client.is_pipelined());
  let _ = publisher.wait_for_connect().await?;

  let jh = tokio::spawn(async move {
    for idx in 0..COUNT {
      let result = client.brpop("foo", 30.0).await?;
      assert_eq!(result.into_array().pop().unwrap().as_i64().unwrap(), idx);
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0..COUNT {
    // the assertion below checks the length of the list, so we have to make sure not to push faster than elements are removed
    sleep(Duration::from_millis(50)).await;
    let result = publisher.lpush("foo", idx).await?;
    assert_eq!(result.as_i64().unwrap(), 1);
  }

  let _ = jh.await?;
  Ok(())
}

pub async fn should_brpoplpush_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let publisher = client.clone_new();
  let _ = publisher.connect(None, !client.is_pipelined());
  let _ = publisher.wait_for_connect().await?;

  let jh = tokio::spawn(async move {
    for idx in 0..COUNT {
      let result = client.brpoplpush("foo{1}", "bar{1}", 30.0).await?;
      assert_eq!(result.as_i64().unwrap(), idx);
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0..COUNT {
    let result = publisher.lpush("foo{1}", idx).await?;
    assert!(result.as_i64().unwrap() > 0);
  }
  let _ = jh.await?;

  for idx in 0..COUNT {
    let result = publisher.rpop("bar{1}", None).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }

  Ok(())
}

pub async fn should_blmove_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let publisher = client.clone_new();
  let _ = publisher.connect(None, !client.is_pipelined());
  let _ = publisher.wait_for_connect().await?;

  let jh = tokio::spawn(async move {
    for idx in 0..COUNT {
      let result = client
        .blmove("foo{1}", "bar{1}", LMoveDirection::Right, LMoveDirection::Left, 30.0)
        .await?;
      assert_eq!(result.as_i64().unwrap(), idx);
    }

    Ok::<_, RedisError>(())
  });

  for idx in 0..COUNT {
    let result = publisher.lpush("foo{1}", idx).await?;
    assert!(result.as_i64().unwrap() > 0);
  }
  let _ = jh.await?;

  for idx in 0..COUNT {
    let result = publisher.rpop("bar{1}", None).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }

  Ok(())
}

pub async fn should_lindex_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;

  for (idx, expected_value) in expected.into_iter().enumerate() {
    let result = client.lindex("foo", idx as i64).await?;
    assert_eq!(result, expected_value);
  }

  Ok(())
}

pub async fn should_linsert_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result = client.linsert("foo", ListLocation::Before, 1, 0).await?;
  assert_eq!(result.as_usize().unwrap(), 0);
  let result = client.llen("foo").await?;
  assert_eq!(result.as_usize().unwrap(), 0);

  let _ = client.lpush("foo", 0).await?;
  let mut expected: Vec<RedisValue> = vec!["0".into()];
  for idx in 1..COUNT {
    let result = client.linsert("foo", ListLocation::After, idx - 1, idx).await?;
    assert_eq!(result.as_i64().unwrap(), idx + 1);
    expected.push(idx.to_string().into());
  }
  let values = client.lrange("foo", 0, COUNT).await?;
  assert_eq!(values.into_array(), expected);

  Ok(())
}

pub async fn should_lpop_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;

  for idx in 0..COUNT {
    let result = client.lpop("foo", None).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }

  let _ = create_count_data(&client, "foo").await?;
  let result = client.lpop("foo", Some(COUNT as usize)).await?;
  assert_eq!(result.into_array(), expected);

  Ok(())
}

pub async fn should_lpos_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;

  for idx in 0..COUNT {
    let result = client.lpos("foo", idx, None, None, None).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }

  let _ = create_count_data(&client, "foo").await?;
  let _ = create_count_data(&client, "foo").await?;

  for idx in 0..COUNT {
    let result = client.lpos("foo", idx, Some(2), None, None).await?;
    assert_eq!(result, (idx + COUNT).into());
    let result = client.lpos("foo", idx, Some(3), None, None).await?;
    assert_eq!(result, (idx + COUNT * 2).into());

    let result = client.lpos("foo", idx, None, Some(2), None).await?;
    let expected = vec![idx.into(), (idx + COUNT).into()];
    assert_eq!(result.into_array(), expected);

    let result = client.lpos("foo", idx, None, Some(3), None).await?;
    let expected = vec![idx.into(), (idx + COUNT).into(), (idx + COUNT * 2).into()];
    assert_eq!(result.into_array(), expected);
  }

  Ok(())
}

pub async fn should_lpush_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..COUNT {
    let result = client.lpush("foo", idx).await?;
    assert_eq!(result.as_i64().unwrap(), idx + 1);
    let result = client.lrange("foo", 0, 0).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }
  let result = client.llen("foo").await?;
  assert_eq!(result.as_i64().unwrap(), COUNT);

  Ok(())
}

pub async fn should_lpushx_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result = client.lpushx("foo", 0).await?;
  assert_eq!(result.as_i64().unwrap(), 0);

  let _ = client.lpush("foo", 0).await?;
  for idx in 0..COUNT {
    let result = client.lpushx("foo", idx).await?;
    assert_eq!(result.as_i64().unwrap(), idx + 2);
    let result = client.lrange("foo", 0, 0).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }
  let result = client.llen("foo").await?;
  assert_eq!(result.as_i64().unwrap(), COUNT + 1);

  Ok(())
}

pub async fn should_lrange_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;

  let result = client.lrange("foo", 0, COUNT).await?;
  assert_eq!(result.into_array(), expected);

  for idx in 0..COUNT {
    let result = client.lrange("foo", idx, idx).await?;
    assert_eq!(result.as_i64().unwrap(), idx.into());
  }

  Ok(())
}

pub async fn should_lrem_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_count_data(&client, "foo").await?;
  for idx in 0..COUNT {
    let result = client.lrem("foo", 1, idx).await?;
    assert_eq!(result.as_usize().unwrap(), 1);
  }
  let result = client.llen("foo").await?;
  assert_eq!(result.as_usize().unwrap(), 0);

  let _ = create_count_data(&client, "foo").await?;
  let _ = create_count_data(&client, "foo").await?;
  for idx in 0..COUNT {
    let result = client.lrem("foo", 2, idx).await?;
    assert_eq!(result.as_usize().unwrap(), 2);
  }
  let result = client.llen("foo").await?;
  assert_eq!(result.as_usize().unwrap(), 0);

  Ok(())
}

pub async fn should_lset_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.lset("foo", 1, 0).await.is_err());
  let mut expected = create_count_data(&client, "foo").await?;
  expected.reverse();

  for idx in 0..COUNT {
    let _ = client.lset("foo", idx, COUNT - (idx + 1)).await?;
  }
  let result = client.lrange("foo", 0, COUNT).await?;
  assert_eq!(result.into_array(), expected);

  Ok(())
}

pub async fn should_ltrim_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected = create_count_data(&client, "foo").await?;

  let _ = client.ltrim("foo", 0, COUNT).await?;
  let result = client.lrange("foo", 0, COUNT).await?;
  assert_eq!(result.into_array(), expected);

  for idx in 0..COUNT {
    let _ = client.ltrim("foo", 0, idx).await?;
    let result = client.lrange("foo", 0, COUNT).await?;
    assert_eq!(result.into_array(), expected[0..(idx + 1) as usize]);

    let _ = client.del("foo").await?;
    let _ = create_count_data(&client, "foo").await?;
  }

  Ok(())
}

pub async fn should_rpop_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let mut expected = create_count_data(&client, "foo").await?;
  expected.reverse();

  for idx in 0..COUNT {
    let result = client.rpop("foo", None).await?;
    assert_eq!(result.as_i64().unwrap(), COUNT - (idx + 1));
  }

  let _ = create_count_data(&client, "foo").await?;
  let result = client.rpop("foo", Some(COUNT as usize)).await?;
  assert_eq!(result.into_array(), expected);

  Ok(())
}

pub async fn should_rpoplpush_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..COUNT {
    let result = client.lpush("foo{1}", idx).await?;
    assert_eq!(result.as_i64().unwrap(), 1);
    let result = client.rpoplpush("foo{1}", "bar{1}").await?;
    assert_eq!(result.as_i64().unwrap(), idx);
    let result = client.rpop("bar{1}", None).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }

  Ok(())
}

pub async fn should_lmove_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..COUNT {
    let result = client.lpush("foo{1}", idx).await?;
    assert_eq!(result.as_i64().unwrap(), 1);
    let result = client
      .lmove("foo{1}", "bar{1}", LMoveDirection::Right, LMoveDirection::Left)
      .await?;
    assert_eq!(result.as_i64().unwrap(), idx);
    let result = client.rpop("bar{1}", None).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }

  Ok(())
}

pub async fn should_rpush_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  for idx in 0..COUNT {
    let result = client.rpush("foo", idx).await?;
    assert_eq!(result.as_i64().unwrap(), idx + 1);
    let result = client.lrange("foo", -1, -1).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }
  let result = client.llen("foo").await?;
  assert_eq!(result.as_i64().unwrap(), COUNT);

  Ok(())
}

pub async fn should_rpushx_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result = client.rpushx("foo", 0).await?;
  assert_eq!(result.as_i64().unwrap(), 0);

  let _ = client.rpush("foo", 0).await?;
  for idx in 0..COUNT {
    let result = client.rpushx("foo", idx).await?;
    assert_eq!(result.as_i64().unwrap(), idx + 2);
    let result = client.lrange("foo", -1, -1).await?;
    assert_eq!(result.as_i64().unwrap(), idx);
  }
  let result = client.llen("foo").await?;
  assert_eq!(result.as_i64().unwrap(), COUNT + 1);

  Ok(())
}
