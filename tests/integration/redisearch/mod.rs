use fred::{
  error::RedisError,
  prelude::*,
  types::{FtCreateOptions, SearchSchema, SearchSchemaKind},
  util::NONE,
};
use serde_json::{json, Value};
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

pub async fn should_index_and_info_basic_json(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  assert!(client.ft_list::<Vec<String>>().await?.is_empty());

  client
    .ft_create("foo_json", FtCreateOptions::default(), vec![SearchSchema {
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
    }])
    .await?;

  let value = json!({ "bar": "abc123" });
  let _: () = client.json_set("foo", "$", value.clone(), None).await?;
  let result: Value = client.json_get("foo", NONE, NONE, NONE, "$").await?;
  assert_eq!(value, result[0]);

  tokio::time::sleep(Duration::from_secs(1)).await;
  let info: HashMap<String, RedisValue> = client.ft_info("foo_json").await?;
  panic!("{:?}", info.get("num_docs"));

  Ok(())
}
