use fred::client::util as fred_utils;
use fred::prelude::*;

static SCRIPTS: &'static [&'static str] = &[
  "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
  "return {KEYS[2],KEYS[1],ARGV[1],ARGV[2]}",
  "return {KEYS[1],KEYS[2],ARGV[2],ARGV[1]}",
  "return {KEYS[2],KEYS[1],ARGV[2],ARGV[1]}",
];

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let client = RedisClient::new(config);

  let jh = client.connect(None, false);
  let _ = client.wait_for_connect().await?;

  for script in SCRIPTS.iter() {
    let hash = fred_utils::sha1_hash(script);

    if !client.script_exists(&hash).await?.pop().unwrap_or(false) {
      let _ = client.script_load(*script).await?;
    }

    let results = client.evalsha(&hash, vec!["foo", "bar"], vec![1, 2]).await?;
    println!("Script results for {}: {:?}", hash, results);
  }

  // or use eval without script_load
  let result = client.eval(SCRIPTS[0], vec!["foo", "bar"], vec![1, 2]).await?;
  println!("First script result: {:?}", result);

  let _ = client.quit().await;
  Ok(())
}
