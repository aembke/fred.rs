use fred::{
  clients::{Locker, LockerConfig},
  prelude::*,
  types::RespVersion,
};
use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};
use tokio::time::{sleep, timeout};

// All tests here assume RESP3. The `centralized_test!` / `cluster_test!`
// macros fan each test across both protocol versions, so RESP2 just bails
// out as a no-op.
fn skip_if_resp2(client: &Client) -> bool {
  client.protocol_version() == RespVersion::RESP2
}

async fn fresh_locker(client: Client, cfg: LockerConfig) -> Result<Locker, Error> {
  let locker = Locker::from_client(cfg, client);
  locker.init().await?;
  Ok(locker)
}

fn unique_prefix() -> String {
  use std::time::{SystemTime, UNIX_EPOCH};
  let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  format!("fred-locker-test:{nanos:x}")
}

pub async fn should_acquire_and_release(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let locker = fresh_locker(client, LockerConfig::new().key_prefix(unique_prefix())).await?;

  let guard = locker.acquire("a").await?;
  assert!(!guard.is_lost());
  guard.release().await;

  // After release the same name should be immediately acquirable again.
  let g2 = locker.try_acquire("a").await?;
  g2.release().await;
  locker.close().await;
  Ok(())
}

pub async fn should_try_acquire_returns_not_found_when_held(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let locker = fresh_locker(client, LockerConfig::new().key_prefix(unique_prefix())).await?;

  let g1 = locker.try_acquire("b").await?;
  match locker.try_acquire("b").await {
    Err(e) if *e.kind() == ErrorKind::NotFound => {},
    other => panic!("expected NotFound, got {other:?}"),
  }
  g1.release().await;
  locker.close().await;
  Ok(())
}

pub async fn should_wait_until_holder_releases(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let locker = fresh_locker(client, LockerConfig::new().key_prefix(unique_prefix())).await?;
  let locker2 = locker.clone();

  let g1 = locker.acquire("c").await?;
  let waiter = tokio::spawn(async move {
    let started = Instant::now();
    let g = locker2.acquire("c").await.expect("waiter acquire failed");
    (started.elapsed(), g)
  });

  sleep(Duration::from_millis(150)).await;
  g1.release().await;

  let (waited, g2) = timeout(Duration::from_secs(2), waiter)
    .await
    .map_err(|_| Error::new(ErrorKind::Unknown, "second acquirer did not wake"))?
    .map_err(|e| Error::new(ErrorKind::Unknown, format!("join: {e}")))?;
  assert!(waited >= Duration::from_millis(140), "waited only {waited:?}");
  g2.release().await;
  locker.close().await;
  Ok(())
}

pub async fn should_auto_extend_past_validity(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let validity = Duration::from_millis(600);
  let locker = fresh_locker(
    client,
    LockerConfig::new()
      .key_prefix(unique_prefix())
      .key_validity(validity)
      .extend_interval(Duration::from_millis(200)),
  )
  .await?;

  let guard = locker.acquire("d").await?;
  // Sleep ~3x the validity. Without extension the key would have expired
  // and the try_acquire below would succeed.
  sleep(validity * 3).await;
  assert!(!guard.is_lost(), "guard reported lost before any takeover");

  match locker.try_acquire("d").await {
    Err(e) if *e.kind() == ErrorKind::NotFound => {},
    other => panic!("expected NotFound (lock still held), got {other:?}"),
  }
  guard.release().await;
  locker.close().await;
  Ok(())
}

pub async fn should_signal_cancelled_on_force_takeover(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let prefix = unique_prefix();
  let locker_a = fresh_locker(
    client.clone(),
    LockerConfig::new()
      .key_prefix(prefix.clone())
      .key_validity(Duration::from_secs(10))
      .extend_interval(Duration::from_secs(5)),
  )
  .await?;

  // Second locker on its own client — simulates a different process taking
  // over.
  let client_b = client.clone_new();
  client_b.init().await?;
  let locker_b = fresh_locker(
    client_b,
    LockerConfig::new()
      .key_prefix(prefix)
      .key_validity(Duration::from_secs(10)),
  )
  .await?;

  let guard = locker_a.acquire("e").await?;
  let cancelled = Arc::new(AtomicUsize::new(0));
  let watcher = {
    let cancelled = cancelled.clone();
    tokio::spawn(async move {
      guard.cancelled().await;
      cancelled.fetch_add(1, Ordering::SeqCst);
      guard.is_lost()
    })
  };

  // Let the dispatcher settle.
  sleep(Duration::from_millis(100)).await;
  let g2 = locker_b.acquire_force("e").await?;

  // Invalidation push should arrive well before the 5s extend tick.
  let observed_lost = timeout(Duration::from_secs(2), watcher)
    .await
    .map_err(|_| Error::new(ErrorKind::Unknown, "cancelled() did not fire after force takeover"))?
    .map_err(|e| Error::new(ErrorKind::Unknown, format!("watcher join: {e}")))?;
  assert_eq!(cancelled.load(Ordering::SeqCst), 1);
  assert!(observed_lost);

  g2.release().await;
  locker_a.close().await;
  locker_b.close().await;
  Ok(())
}

pub async fn should_rearm_tracking_after_reconnect(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let prefix = unique_prefix();
  let locker_a = fresh_locker(
    client.clone(),
    LockerConfig::new()
      .key_prefix(prefix.clone())
      .key_validity(Duration::from_secs(10))
      // Long extend cadence so any takeover detection has to come from an
      // invalidation push, not a timer tick.
      .extend_interval(Duration::from_secs(5)),
  )
  .await?;

  let client_b = client.clone_new();
  client_b.init().await?;
  let locker_b = fresh_locker(
    client_b,
    LockerConfig::new()
      .key_prefix(prefix)
      .key_validity(Duration::from_secs(10)),
  )
  .await?;

  let guard = locker_a.acquire("g").await?;

  // Force a reconnect on locker_a's underlying client. fred does not auto-
  // restore CLIENT TRACKING across reconnects, so this would silently drop
  // tracking without the locker's on_reconnect hook re-arming it.
  client.force_reconnection().await?;
  // Give the on_reconnect handler a moment to land and re-issue tracking.
  sleep(Duration::from_millis(400)).await;

  let g2 = locker_b.acquire_force("g").await?;

  // If tracking is still armed the takeover triggers an invalidation push
  // that lands on locker_a within RTT; if it isn't, the cancelled() future
  // wouldn't fire for the full 5s extend tick and this timeout fails.
  timeout(Duration::from_secs(2), guard.cancelled())
    .await
    .map_err(|_| Error::new(ErrorKind::Unknown, "cancelled() did not fire after reconnect + takeover"))?;
  assert!(guard.is_lost());

  g2.release().await;
  locker_a.close().await;
  locker_b.close().await;
  Ok(())
}

pub async fn should_cancel_outstanding_guards_on_close(client: Client, _: Config) -> Result<(), Error> {
  if skip_if_resp2(&client) {
    return Ok(());
  }
  let locker = fresh_locker(
    client,
    LockerConfig::new()
      .key_prefix(unique_prefix())
      .key_validity(Duration::from_secs(30)),
  )
  .await?;

  let guard = locker.acquire("f").await?;
  assert!(!guard.is_lost());

  // close() should signal every outstanding extender. The guard's
  // cancelled() future should fire promptly even though validity is 30s.
  locker.close().await;
  timeout(Duration::from_secs(2), guard.cancelled())
    .await
    .map_err(|_| Error::new(ErrorKind::Unknown, "close did not cancel outstanding guard"))?;
  assert!(guard.is_lost());
  Ok(())
}
