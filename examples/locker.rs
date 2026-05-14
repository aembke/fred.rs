#![allow(clippy::disallowed_names)]

use fred::{
  clients::{Locker, LockerConfig},
  prelude::*,
  types::RespVersion,
};
use std::time::Duration;

// Run a redis server locally on the default port, then:
//   cargo run --example locker --features locker
//
// The example acquires a lock, does a few seconds of "work" while the
// background extender keeps the TTL alive, and releases the lock cleanly.
#[tokio::main]
async fn main() -> Result<(), Error> {
  // RESP3 is needed for the OPTOUT client-tracking fast path. To run against
  // RESP2 build the locker with `LockerConfig::new().disable_caching(true)`
  // and the waiter loop will fall back to polling.
  let client = Builder::default_centralized()
    .with_config(|c| c.version = RespVersion::RESP3)
    .build()?;
  client.init().await?;

  let locker = Locker::from_client(
    LockerConfig::new()
      .key_prefix("example:lock")
      .key_validity(Duration::from_secs(6)),
    client.clone(),
  );
  locker.init().await?;

  let guard = locker.acquire("demo").await?;
  println!("acquired {} = {}", guard.key(), guard.token());

  let work = async {
    for i in 0 .. 5 {
      tokio::time::sleep(Duration::from_secs(1)).await;
      println!("tick {i}");
    }
  };
  tokio::select! {
    _ = work => println!("work finished"),
    _ = guard.cancelled() => println!("lock lost mid-work"),
  }

  guard.release().await;
  locker.close().await;
  client.quit().await?;
  Ok(())
}
