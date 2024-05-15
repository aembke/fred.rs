use fred::{
  error::RedisError,
  prelude::*,
  types::{FtCreateOptions, FtSearchOptions, IndexKind, RedisMap, SearchSchema, SearchSchemaKind},
  util::NONE,
};
use maplit::hashmap;
use rand::{thread_rng, Rng};
use redis_protocol::resp3::types::RespVersion;
use std::{collections::HashMap, time::Duration};

pub async fn should_list_indexes(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.ft_list::<Vec<String>>().await?.is_empty());

  client
    .ft_create("foo", FtCreateOptions::default(), vec![SearchSchema {
      field_name: "bar".into(),
      alias:      Some("baz".into()),
      kind:       SearchSchemaKind::Numeric {
        sortable: false,
        unf:      false,
        noindex:  false,
      },
    }])
    .await?;

  assert_eq!(client.ft_list::<Vec<String>>().await?, vec!["foo".to_string()]);
  Ok(())
}

pub async fn should_index_and_info_basic_hash(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.ft_list::<Vec<String>>().await?.is_empty());

  client
    .ft_create(
      "foo_idx",
      FtCreateOptions {
        on: Some(IndexKind::Hash),
        ..Default::default()
      },
      vec![SearchSchema {
        field_name: "bar".into(),
        alias:      Some("baz".into()),
        kind:       SearchSchemaKind::Text {
          sortable:       false,
          unf:            false,
          noindex:        false,
          phonetic:       None,
          weight:         None,
          withsuffixtrie: false,
          nostem:         false,
        },
      }],
    )
    .await?;

  client.hset("foo", ("bar", "abc123")).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  let mut info: HashMap<String, RedisValue> = client.ft_info("foo_idx").await?;
  assert_eq!(info.remove("num_docs").unwrap_or(RedisValue::Null).convert::<i64>()?, 1);

  Ok(())
}

pub async fn should_index_and_search_hash(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.ft_list::<Vec<String>>().await?.is_empty());

  client
    .ft_create(
      "foo_idx",
      FtCreateOptions {
        on: Some(IndexKind::Hash),
        prefixes: vec!["record:".into()],
        ..Default::default()
      },
      vec![SearchSchema {
        field_name: "bar".into(),
        alias:      None,
        kind:       SearchSchemaKind::Text {
          sortable:       false,
          unf:            false,
          noindex:        false,
          phonetic:       None,
          weight:         None,
          withsuffixtrie: false,
          nostem:         false,
        },
      }],
    )
    .await?;

  client.hset("record:1", ("bar", "abc 123")).await?;
  client.hset("record:2", ("bar", "abc 345")).await?;
  client.hset("record:3", ("bar", "def 678")).await?;
  tokio::time::sleep(Duration::from_millis(100)).await;

  if client.protocol_version() == RespVersion::RESP3 {
    // RESP3 uses maps and includes extra metadata fields
    let mut results: HashMap<String, RedisValue> =
      client.ft_search("foo_idx", "*", FtSearchOptions::default()).await?;
    assert_eq!(
      results
        .get("total_results")
        .cloned()
        .unwrap_or(RedisValue::Null)
        .convert::<i64>()?,
      3
    );

    // {"attributes":[],"format":"STRING","results":[{"extra_attributes":{"bar":"abc
    // 123"},"id":"record:1","values":[]},{"extra_attributes":{"bar":"abc
    // 345"},"id":"record:2","values":[]},{"extra_attributes":{"bar":"def
    // 678"},"id":"record:3","values":[]}],"total_results":3,"warning":[]}
    let results: Vec<HashMap<String, RedisValue>> = results.remove("results").unwrap().convert()?;
    let expected = vec![
      hashmap! {
        "id" => "record:1".into(),
        "values" => RedisValue::Array(vec![]),
        "extra_attributes" => hashmap! {
          "bar" => "abc 123"
        }.try_into()?
      },
      hashmap! {
        "id" => "record:2".into(),
        "values" => RedisValue::Array(vec![]),
        "extra_attributes" => hashmap! {
          "bar" => "abc 345"
        }.try_into()?
      },
      hashmap! {
        "id" => "record:3".into(),
        "values" => RedisValue::Array(vec![]),
        "extra_attributes" => hashmap! {
          "bar" => "def 678"
        }
        .try_into()?
      },
    ]
    .into_iter()
    .map(|m| {
      m.into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<HashMap<String, RedisValue>>()
    })
    .collect::<Vec<_>>();
    assert_eq!(results, expected);

    // TODO search by @foo:(abc)
  } else {
    // RESP2 uses an array format w/o extra metadata
    let results: (usize, RedisKey, RedisKey, RedisKey) = client
      .ft_search("foo_idx", "*", FtSearchOptions {
        nocontent: true,
        ..Default::default()
      })
      .await?;
    assert_eq!(results, (3, "record:1".into(), "record:2".into(), "record:3".into()));
    let results: (usize, RedisKey, RedisKey) = client
      .ft_search("foo_idx", "@bar:(abc)", FtSearchOptions {
        nocontent: true,
        ..Default::default()
      })
      .await?;
    assert_eq!(results, (2, "record:1".into(), "record:2".into()));
    let results: (usize, RedisKey, (String, String)) = client
      .ft_search("foo_idx", "@bar:(def)", FtSearchOptions::default())
      .await?;
    assert_eq!(results, (1, "record:3".into(), ("bar".into(), "def 678".into())));
  }

  Ok(())
}

pub async fn should_index_and_aggregate_timestamps(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.ft_list::<Vec<String>>().await?.is_empty());

  // https://redis.io/docs/latest/develop/interact/search-and-query/advanced-concepts/aggregations/
  client
    .ft_create(
      "timestamp_idx",
      FtCreateOptions {
        on: Some(IndexKind::Hash),
        prefixes: vec!["record:".into()],
        ..Default::default()
      },
      vec![SearchSchema {
        field_name: "timestamp".into(),
        alias:      None,
        kind:       SearchSchemaKind::Numeric {
          sortable: true,
          unf:      false,
          noindex:  false,
        },
      }],
    )
    .await?;

  for idx in 0 .. 1000 {
    let rand: u64 = thread_rng().gen_range(0 .. 10000);
    client
      .hset(format!("record:{}", idx), [("timestamp", idx + rand), ("user_id", idx)])
      .await?;
  }
  tokio::time::sleep(Duration::from_millis(100)).await;

  // TODO
  // FT.AGGREGATE myIndex "*"
  //   APPLY "@timestamp - (@timestamp % 3600)" AS hour

  // FT.AGGREGATE myIndex "*"
  //   APPLY "@timestamp - (@timestamp % 3600)" AS hour
  //   GROUPBY 1 @hour
  //   	REDUCE COUNT_DISTINCT 1 @user_id AS num_users

  // FT.AGGREGATE myIndex "*"
  //   APPLY "@timestamp - (@timestamp % 3600)" AS hour
  //   GROUPBY 1 @hour
  //   	REDUCE COUNT_DISTINCT 1 @user_id AS num_users
  //   SORTBY 2 @hour ASC

  // FT.AGGREGATE myIndex "*"
  //   APPLY "@timestamp - (@timestamp % 3600)" AS hour
  //   GROUPBY 1 @hour
  //   	REDUCE COUNT_DISTINCT 1 @user_id AS num_users
  //   SORTBY 2 @hour ASC
  //   APPLY timefmt(@hour) AS hour

  if client.protocol_version() == RespVersion::RESP3 {
    // RESP3 uses maps and includes extra metadata fields

    unimplemented!()
  } else {
    //
    unimplemented!()
  }

  Ok(())
}
