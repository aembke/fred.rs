use fred::prelude::*;
use fred::types::{RedisKey, RedisMap, XCap, XCapKind, XCapTrim, XReadResponse, XID};
use maplit::hashmap;
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hash;
use std::time::Duration;
use tokio::time::sleep;

type FakeExpectedValues = Vec<HashMap<String, HashMap<String, usize>>>;

async fn create_fake_group_and_stream(client: &RedisClient, key: &str) -> Result<(), RedisError> {
  client.xgroup_create(key, "group1", "$", true).await
}

async fn add_stream_entries(
  client: &RedisClient,
  key: &str,
  count: usize,
) -> Result<(Vec<String>, FakeExpectedValues), RedisError> {
  let mut ids = Vec::with_capacity(count);
  let mut expected = Vec::with_capacity(count);
  for idx in 0..count {
    let id: String = client.xadd(key, false, None, "*", ("count", idx)).await?;
    ids.push(id.clone());

    let mut outer = HashMap::with_capacity(1);
    let mut inner = HashMap::with_capacity(1);
    inner.insert("count".into(), idx);
    outer.insert(id, inner);
    expected.push(outer);
  }

  Ok((ids, expected))
}

fn has_expected_value(expected: &FakeExpectedValues, actual: &FakeExpectedValues) -> bool {
  actual.iter().enumerate().fold(true, |b, (i, v)| b && v == &expected[i])
}

pub async fn should_xinfo_consumers(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Result<(), RedisError> = client.xinfo_consumers("foo{1}", "group1").await;
  assert!(result.is_err());

  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _: () = client.xgroup_createconsumer("foo{1}", "group1", "consumer1").await?;
  let consumers: Vec<HashMap<String, String>> = client.xinfo_consumers("foo{1}", "group1").await?;
  assert_eq!(consumers.len(), 1);
  assert_eq!(consumers[0].get("name"), Some(&"consumer1".to_owned()));

  let _: () = client.xgroup_createconsumer("foo{1}", "group1", "consumer2").await?;
  let consumers: Vec<HashMap<String, String>> = client.xinfo_consumers("foo{1}", "group1").await?;
  assert_eq!(consumers.len(), 2);
  assert_eq!(consumers[0].get("name"), Some(&"consumer1".to_owned()));
  assert_eq!(consumers[1].get("name"), Some(&"consumer2".to_owned()));

  Ok(())
}

pub async fn should_xinfo_groups(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Result<(), RedisError> = client.xinfo_groups("foo{1}").await;
  assert!(result.is_err());

  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let result: Vec<HashMap<String, String>> = client.xinfo_groups("foo{1}").await?;
  assert_eq!(result.len(), 1);
  assert_eq!(result[0].get("name"), Some(&"group1".to_owned()));

  let _: () = client.xgroup_create("foo{1}", "group2", "$", true).await?;
  let result: Vec<HashMap<String, String>> = client.xinfo_groups("foo{1}").await?;
  assert_eq!(result.len(), 2);
  assert_eq!(result[0].get("name"), Some(&"group1".to_owned()));
  assert_eq!(result[1].get("name"), Some(&"group2".to_owned()));

  Ok(())
}

pub async fn should_xinfo_streams(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Result<(), RedisError> = client.xinfo_stream("foo{1}", true, None).await;
  assert!(result.is_err());

  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let mut result: HashMap<String, RedisValue> = client.xinfo_stream("foo{1}", true, None).await?;
  assert_eq!(result.len(), 6);
  assert_eq!(result.get("length"), Some(&RedisValue::Integer(0)));

  let groups: HashMap<String, RedisValue> = result.remove("groups").unwrap().convert()?;
  assert_eq!(groups.get("name"), Some(&RedisValue::from("group1")));

  Ok(())
}

pub async fn should_xadd_auto_id_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: String = client.xadd("foo{1}", false, None, "*", ("a", "b")).await?;
  assert!(!result.is_empty());

  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);
  Ok(())
}

pub async fn should_xadd_manual_id_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: String = client.xadd("foo{1}", false, None, "1-0", ("a", "b")).await?;
  assert_eq!(result, "1-0");

  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);
  Ok(())
}

pub async fn should_xadd_with_cap_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _: () = client
    .xadd("foo{1}", false, ("MAXLEN", "=", 1), "*", ("a", "b"))
    .await?;

  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);
  Ok(())
}

pub async fn should_xadd_nomkstream_to_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let result: Option<String> = client.xadd("foo{1}", true, None, "*", ("a", "b")).await?;
  assert!(result.is_none());

  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _: () = client.xadd("foo{1}", true, None, "*", ("a", "b")).await?;
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);
  Ok(())
}

pub async fn should_xtrim_a_stream_approx_cap(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _ = add_stream_entries(&client, "foo{1}", 3).await?;

  let deleted: usize = client.xtrim("foo{1}", ("MAXLEN", "~", 1)).await?;
  assert!(deleted < 3);
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 3 - deleted);

  let _ = client.del("foo{1}").await?;
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _ = add_stream_entries(&client, "foo{1}", 3).await?;
  let deleted: usize = client
    .xtrim("foo{1}", (XCapKind::MaxLen, XCapTrim::AlmostExact, 1))
    .await?;
  assert!(deleted < 3);
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 3 - deleted);

  Ok(())
}

pub async fn should_xtrim_a_stream_eq_cap(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _ = add_stream_entries(&client, "foo{1}", 3).await?;

  let deleted: usize = client.xtrim("foo{1}", ("MAXLEN", "=", 1)).await?;
  assert_eq!(deleted, 2);
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);

  let _ = client.del("foo{1}").await?;
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _ = add_stream_entries(&client, "foo{1}", 3).await?;
  let deleted: usize = client.xtrim("foo{1}", (XCapKind::MaxLen, XCapTrim::Exact, 1)).await?;
  assert_eq!(deleted, 2);
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);

  Ok(())
}

pub async fn should_xdel_one_id_in_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (ids, _) = add_stream_entries(&client, "foo{1}", 2).await?;

  let deleted: usize = client.xdel("foo{1}", &ids[0]).await?;
  assert_eq!(deleted, 1);
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);
  Ok(())
}

pub async fn should_xdel_multiple_ids_in_a_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (ids, _) = add_stream_entries(&client, "foo{1}", 3).await?;

  let deleted: usize = client.xdel("foo{1}", ids[0..2].to_vec()).await?;
  assert_eq!(deleted, 2);
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 1);
  Ok(())
}

pub async fn should_xrange_no_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (_, expected) = add_stream_entries(&client, "foo{1}", 3).await?;

  println!(
    "{:?}",
    client.xrange::<RedisValue, _, _, _>("foo{1}", "-", "+", None).await?
  );
  let result: FakeExpectedValues = client.xrange("foo{1}", "-", "+", None).await?;
  println!("Comparing {:?} => {:?}", result, expected);
  assert_eq!(result, expected);
  Ok(())
}

pub async fn should_xrange_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (_, expected) = add_stream_entries(&client, "foo{1}", 3).await?;

  let result: RedisValue = client.xrange("foo{1}", "-", "+", Some(1)).await?;
  println!("RESULT VALUE {:?}", result);

  let result: FakeExpectedValues = client.xrange("foo{1}", "-", "+", Some(1)).await?;
  println!("Comparing {:?} => {:?}", result, expected);
  assert!(has_expected_value(&expected, &result));
  Ok(())
}

pub async fn should_xrevrange_no_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (_, mut expected) = add_stream_entries(&client, "foo{1}", 3).await?;
  expected.reverse();

  let result: FakeExpectedValues = client.xrevrange("foo{1}", "+", "-", None).await?;
  assert_eq!(result, expected);
  Ok(())
}

pub async fn should_xrevrange_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (_, mut expected) = add_stream_entries(&client, "foo{1}", 3).await?;
  expected.reverse();

  let result: FakeExpectedValues = client.xrevrange("foo{1}", "-", "+", Some(1)).await?;
  assert!(has_expected_value(&expected, &result));
  Ok(())
}

pub async fn should_run_xlen_on_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "foo{1}");
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 0);

  let _ = add_stream_entries(&client, "foo{1}", 3).await?;
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 3);
  Ok(())
}

pub async fn should_xread_one_key_count_1(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let (mut ids, mut expected) = add_stream_entries(&client, "foo{1}", 3).await?;
  let _ = ids.pop().unwrap();
  let most_recent_expected = expected.pop().unwrap();
  let second_recent_id = ids.pop().unwrap();

  let mut expected = HashMap::new();
  expected.insert("foo{1}".into(), vec![most_recent_expected]);

  let result: HashMap<String, Vec<HashMap<String, HashMap<String, usize>>>> = client
    .xread::<RedisValue, _, _>(Some(1), None, "foo{1}", second_recent_id)
    .await?
    .flatten_array_values(1)
    .convert()?;
  assert_eq!(result, expected);

  Ok(())
}

pub async fn should_xread_multiple_keys_count_2(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _ = create_fake_group_and_stream(&client, "bar{1}").await?;
  let (mut foo_ids, mut foo_inner) = add_stream_entries(&client, "foo{1}", 3).await?;
  let (mut bar_ids, mut bar_inner) = add_stream_entries(&client, "bar{1}", 3).await?;

  let mut expected = HashMap::new();
  expected.insert("foo{1}".into(), foo_inner[1..].to_vec());
  expected.insert("bar{1}".into(), bar_inner[1..].to_vec());

  let ids: Vec<XID> = vec![foo_ids[0].as_str().into(), bar_ids[0].as_str().into()];
  let result: HashMap<String, Vec<HashMap<String, HashMap<String, usize>>>> = client
    .xread::<RedisValue, _, _>(Some(2), None, vec!["foo{1}", "bar{1}"], ids)
    .await?
    .flatten_array_values(1)
    .convert()?;
  assert_eq!(result, expected);

  Ok(())
}

pub async fn should_xread_with_blocking(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let expected_id = "123456789-0";
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;

  let mut expected = HashMap::new();
  let mut inner = HashMap::new();
  let mut fields = HashMap::new();
  fields.insert("count".into(), 100);
  inner.insert(expected_id.into(), fields);
  expected.insert("foo{1}".into(), vec![inner]);

  let add_client = client.clone_new();
  tokio::spawn(async move {
    let _ = add_client.connect(None);
    let _ = add_client.wait_for_connect().await?;
    sleep(Duration::from_millis(500)).await;

    let _: () = add_client
      .xadd("foo{1}", false, None, expected_id, ("count", 100))
      .await?;
    let _ = add_client.quit().await?;
    Ok::<(), RedisError>(())
  });

  let result: HashMap<String, Vec<HashMap<String, HashMap<String, usize>>>> = client
    .xread::<RedisValue, _, _>(None, Some(5000), "foo{1}", XID::Max)
    .await?
    .flatten_array_values(1)
    .convert()?;
  assert_eq!(result, expected);

  Ok(())
}

pub async fn should_xgroup_create_no_mkstream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let result: Result<RedisValue, RedisError> = client.xgroup_create("foo{1}", "group1", "$", false).await;
  assert!(result.is_err());
  let _: () = client.xadd("foo{1}", false, None, "*", ("count", 1)).await?;
  let _: () = client.xgroup_create("foo{1}", "group1", "$", false).await?;

  Ok(())
}

pub async fn should_xgroup_create_mkstream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _: () = client.xgroup_create("foo{1}", "group1", "$", true).await?;
  let len: usize = client.xlen("foo{1}").await?;
  assert_eq!(len, 0);

  Ok(())
}

pub async fn should_xgroup_createconsumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let len: usize = client.xgroup_createconsumer("foo{1}", "group1", "consumer1").await?;
  assert_eq!(len, 1);

  let consumers: Vec<HashMap<String, RedisValue>> = client.xinfo_consumers("foo{1}", "group1").await?;
  assert_eq!(consumers[0].get("name").unwrap(), &RedisValue::from("consumer1"));
  assert_eq!(consumers[0].get("pending").unwrap(), &RedisValue::from(0));

  Ok(())
}

pub async fn should_xgroup_delconsumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let len: usize = client.xgroup_createconsumer("foo{1}", "group1", "consumer1").await?;
  assert_eq!(len, 1);

  let len: usize = client.xgroup_delconsumer("foo{1}", "group1", "consumer1").await?;
  assert_eq!(len, 0);

  let consumers: Vec<HashMap<String, RedisValue>> = client.xinfo_consumers("foo{1}", "group1").await?;
  assert!(consumers.is_empty());
  Ok(())
}

pub async fn should_xgroup_destroy(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let len: usize = client.xgroup_destroy("foo{1}", "group1").await?;
  assert_eq!(len, 1);

  Ok(())
}

pub async fn should_xgroup_setid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _: () = client.xgroup_setid("foo{1}", "group1", "12345-0").await?;

  Ok(())
}

pub async fn should_xreadgroup_one_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  let _ = create_fake_group_and_stream(&client, "foo{1}").await?;
  let _ = add_stream_entries(&client, "foo{1}", 3).await?;
  let _: () = client.xgroup_createconsumer("foo{1}", "group1", "consumer1").await?;

  let result: XReadResponse<String, String, String, usize> = client
    .xreadgroup_map("group1", "consumer1", None, None, false, "foo{1}", ">")
    .await?;

  assert_eq!(result.len(), 1);
  for (idx, record) in result.get("foo{1}").unwrap().into_iter().enumerate() {
    let (_id, record) = record.into_iter().next().unwrap();
    let value = record.get("count").expect("Failed to read count.");
    assert_eq!(idx, *value)
  }

  Ok(())
}

pub async fn should_xreadgroup_multiple_stream(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xreadgroup_block(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xack_one_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xack_multiple_ids(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_one_id(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_multiple_ids(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_idle(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_time(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_retrycount(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_force(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xclaim_with_justid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_default(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_count(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_justid(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xpending_default(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xpending_with_idle_no_consumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}

pub async fn should_xautoclaim_with_idle_and_consumer(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  Ok(())
}
