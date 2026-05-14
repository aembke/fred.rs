//! Distributed locks backed by Redis with automatic lease extension and
//! RESP3 client-side-cache invalidation push notifications.
//!
//! This is a Rust port of Go's
//! [`rueidislock`](https://pkg.go.dev/github.com/redis/rueidis/rueidislock).
//! Each acquired lock spawns a background task that periodically extends the
//! key's TTL with a CAS Lua script, and the same task listens for RESP3
//! invalidation pushes on the lock key — so the holder learns the moment
//! someone else takes the lock or the server evicts it, rather than waiting
//! for the next extension tick to fail.
//!
//! Quick start:
//!
//! ```no_run
//! use fred::{clients::LockerConfig, prelude::*, types::RespVersion};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Error> {
//! let client = Builder::default_centralized()
//!   .with_config(|c| c.version = RespVersion::RESP3)
//!   .build()?;
//! client.init().await?;
//!
//! let locker = Locker::from_client(
//!   LockerConfig::new()
//!     .key_prefix("myapp:lock")
//!     .key_validity(Duration::from_secs(10)),
//!   client,
//! );
//! locker.init().await?;
//!
//! let guard = locker.acquire("cron:purge").await?;
//! tokio::select! {
//!   _ = do_work() => {}
//!   _ = guard.cancelled() => {
//!     // someone else took the lock — bail
//!   }
//! }
//! guard.release().await;
//! # Ok(()) }
//! # async fn do_work() {}
//! ```
//!
//! Notes:
//!
//! * RESP3 is required when client tracking is enabled (the default). Call
//!   [`LockerConfig::disable_caching`] to fall back to a polling waiter loop
//!   under RESP2; correctness is the same, just slower to react when other
//!   waiters are queued behind the same name.
//! * The locker takes exclusive ownership of the client's invalidation
//!   stream. Don't pair it with a separate [`on_invalidation`] consumer on
//!   the same client.
//! * Single-key only at this revision. The API was shaped so a multi-key
//!   Redlock variant can land later as `LockerConfig::key_majority(n)`
//!   without breaking changes.
//!
//! [`on_invalidation`]: crate::interfaces::TrackingInterface::on_invalidation

use crate::{
  clients::Client,
  error::{Error, ErrorKind},
  interfaces::{ClientLike, EventInterface, TrackingInterface},
  runtime::{spawn, AtomicBool, AtomicUsize, JoinHandle, Mutex, RefCount},
  types::{client::Invalidation, config::Server, scripts::Script},
};
use bytes_utils::Str;
use rand::{distributions::Alphanumeric, Rng};
use std::{
  collections::HashMap,
  fmt,
  sync::{atomic::Ordering, Weak},
  time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast::error::RecvError as BroadcastRecvError, Notify};

// `SET NX PXAT` + a trailing `GET`. The `GET` is what arms RESP3 client
// tracking on this key — the server only pushes invalidations for keys the
// connection has actually read.
const ACQUIRE_AT_LUA: &str = r#"
local r = redis.call("SET", KEYS[1], ARGV[1], "NX", "PXAT", ARGV[2]);
redis.call("GET", KEYS[1]);
return r;
"#;

// Same but `PX` (millisecond TTL) instead of `PXAT`, for redis < 6.2.
const ACQUIRE_MS_LUA: &str = r#"
local r = redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]);
redis.call("GET", KEYS[1]);
return r;
"#;

// Takeover variant: writes unconditionally, evicting any existing holder.
const FORCE_AT_LUA: &str = r#"
local r = redis.call("SET", KEYS[1], ARGV[1], "PXAT", ARGV[2]);
redis.call("GET", KEYS[1]);
return r;
"#;
const FORCE_MS_LUA: &str = r#"
local r = redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2]);
redis.call("GET", KEYS[1]);
return r;
"#;

// Renew the deadline iff the value still matches our token.
const EXTEND_LUA: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  local r = redis.call("PEXPIREAT", KEYS[1], ARGV[2]);
  redis.call("GET", KEYS[1]);
  return r;
end;
return 0;
"#;

// CAS delete: only drop the key if we still own it.
const DEL_LUA: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1]);
end;
return 0;
"#;

const DEFAULT_KEY_PREFIX: &str = "fredlock";
const DEFAULT_KEY_VALIDITY: Duration = Duration::from_secs(5);
const DEFAULT_TRY_NEXT_AFTER: Duration = Duration::from_millis(20);
const TOKEN_LEN: usize = 32;
// Truncated token shown in log lines so noise stays bounded.
const TOKEN_LOG_PREFIX: usize = 8;

/// Tunable knobs for a [`Locker`]. The defaults match `rueidislock`:
/// `fredlock` key prefix, 5s validity, `validity / 2` extension cadence,
/// 20ms polling fallback, RESP3 OPTOUT tracking enabled.
#[derive(Clone, Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "locker")))]
pub struct LockerConfig {
  prefix:       Str,
  validity:     Duration,
  extend_every: Option<Duration>,
  try_next:     Duration,
  no_loop:      bool,
  fallback_px:  bool,
  no_caching:   bool,
}

impl Default for LockerConfig {
  fn default() -> Self {
    Self::new()
  }
}

impl LockerConfig {
  pub fn new() -> Self {
    LockerConfig {
      prefix:       Str::from_static(DEFAULT_KEY_PREFIX),
      validity:     DEFAULT_KEY_VALIDITY,
      extend_every: None,
      try_next:     DEFAULT_TRY_NEXT_AFTER,
      no_loop:      false,
      fallback_px:  false,
      no_caching:   false,
    }
  }

  /// Prefix applied to every redis key the locker writes. Keys take the
  /// shape `{prefix}:{name}`. Defaults to `fredlock`.
  pub fn key_prefix(mut self, prefix: impl Into<Str>) -> Self {
    self.prefix = prefix.into();
    self
  }

  /// How long an acquisition stays valid in redis before its TTL would
  /// expire without an extension. The default (5s) is tuned to absorb a
  /// reasonable amount of network jitter at the default extension cadence.
  pub fn key_validity(mut self, validity: Duration) -> Self {
    self.validity = validity;
    self
  }

  /// How often the background extender bumps the TTL. Defaults to
  /// `validity / 2`. Larger values mean less network chatter but a longer
  /// window between a stalled holder and the TTL elapsing on the server.
  pub fn extend_interval(mut self, interval: Duration) -> Self {
    self.extend_every = Some(interval);
    self
  }

  /// Poll interval used by [`Locker::acquire`] when client tracking is
  /// disabled. No effect when tracking is enabled — waiters are woken by
  /// RESP3 invalidation pushes instead.
  pub fn try_next_after(mut self, dur: Duration) -> Self {
    self.try_next = dur;
    self
  }

  /// Send `NOLOOP` with `CLIENT TRACKING` so the connection does not
  /// receive invalidations triggered by its own writes. Requires Redis
  /// >= 7.0.5.
  pub fn no_loop_tracking(mut self, enabled: bool) -> Self {
    self.no_loop = enabled;
    self
  }

  /// Use `SET ... NX PX <ms>` instead of `SET ... NX PXAT <deadline>`. Set
  /// this when targeting Redis < 6.2 which lacks `PXAT`.
  pub fn fallback_set_px(mut self, enabled: bool) -> Self {
    self.fallback_px = enabled;
    self
  }

  /// Disable RESP3 client tracking. Waiters fall back to polling driven by
  /// [`try_next_after`], and held locks only notice eviction at the next
  /// extension tick. Required when the underlying client speaks RESP2.
  ///
  /// [`try_next_after`]: LockerConfig::try_next_after
  pub fn disable_caching(mut self, disabled: bool) -> Self {
    self.no_caching = disabled;
    self
  }
}

/// Cheaply cloneable handle to a redis-backed distributed lock manager.
///
/// Construct via [`Locker::from_client`] or [`crate::types::Builder::build_locker`],
/// then call [`init`] before the first acquire. A single instance can hand
/// out many concurrent locks (one per unique name).
///
/// [`init`]: Locker::init
#[derive(Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "locker")))]
pub struct Locker {
  inner: RefCount<LockerInner>,
}

impl fmt::Debug for Locker {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Locker")
      .field("prefix", &self.inner.prefix)
      .field("validity", &self.inner.validity)
      .field("extend_every", &self.inner.extend_every)
      .field("closed", &self.inner.closed.load(Ordering::Relaxed))
      .finish()
  }
}

impl Locker {
  /// Build a locker around an existing client. The client must outlive
  /// every outstanding [`LockGuard`]. Call [`init`] before issuing the
  /// first acquire so tracking is wired up.
  ///
  /// [`init`]: Locker::init
  pub fn from_client(cfg: LockerConfig, client: Client) -> Self {
    let extend_every = cfg.extend_every.unwrap_or(cfg.validity / 2);
    let acquire_script = Script::from_lua(if cfg.fallback_px {
      ACQUIRE_MS_LUA
    } else {
      ACQUIRE_AT_LUA
    });
    let force_script = Script::from_lua(if cfg.fallback_px {
      FORCE_MS_LUA
    } else {
      FORCE_AT_LUA
    });
    let extend_script = Script::from_lua(EXTEND_LUA);
    let del_script = Script::from_lua(DEL_LUA);

    Locker {
      inner: RefCount::new(LockerInner {
        client,
        prefix: cfg.prefix,
        validity: cfg.validity,
        extend_every,
        try_next: cfg.try_next,
        fallback_px: cfg.fallback_px,
        no_caching: cfg.no_caching,
        no_loop: cfg.no_loop,
        gates: Mutex::new(HashMap::new()),
        outstanding: Mutex::new(Vec::new()),
        closed: AtomicBool::new(false),
        initialized: AtomicBool::new(false),
        init_guard: tokio::sync::Mutex::new(()),
        dispatcher: Mutex::new(None),
        reconnect: Mutex::new(None),
        scripts: Scripts {
          acquire: acquire_script,
          force:   force_script,
          extend:  extend_script,
          del:     del_script,
        },
      }),
    }
  }

  /// Preload the Lua scripts, arm `CLIENT TRACKING ... OPTOUT`, and start
  /// the invalidation dispatcher. The client must already be connected (call
  /// [`Client::init`] first); subsequent successful calls are no-ops, and a
  /// failed call leaves the locker un-initialized so retry is meaningful.
  ///
  /// [`Client::init`]: crate::interfaces::ClientLike::init
  pub async fn init(&self) -> Result<(), Error> {
    if self.inner.initialized.load(Ordering::Acquire) {
      return Ok(());
    }
    // Serialize concurrent init() calls. Without this two callers that
    // both observed `initialized = false` would each run start_tracking and
    // spawn a dispatcher, leaving one orphaned task forwarding duplicate
    // invalidations.
    let _guard = self.inner.init_guard.lock().await;
    // Re-check inside the lock — another caller may have just finished.
    if self.inner.initialized.load(Ordering::Acquire) {
      return Ok(());
    }
    if !self.inner.client.is_connected() {
      return Err(Error::new(
        ErrorKind::Config,
        "Locker requires a connected client; call Client::init() first.",
      ));
    }

    // Best-effort preload. `evalsha_with_reload` falls back to EVAL on
    // NOSCRIPT anyway, so an error here is a perf hit, not a correctness one.
    let _ = self.inner.scripts.acquire.load(&self.inner.client).await;
    let _ = self.inner.scripts.force.load(&self.inner.client).await;
    let _ = self.inner.scripts.extend.load(&self.inner.client).await;
    let _ = self.inner.scripts.del.load(&self.inner.client).await;

    if !self.inner.no_caching {
      // OPTOUT: every read is tracked unless the caller explicitly sends
      // `CLIENT CACHING no`. The `GET` inside our Lua arms tracking on the
      // lock key automatically.
      self
        .inner
        .client
        .start_tracking(None, false, false, true, self.inner.no_loop)
        .await?;
      // Dispatcher and reconnect hook both hold a `Weak<LockerInner>` to
      // break the Arc cycle that would otherwise keep the locker alive
      // forever once `close` is forgotten.
      let weak = RefCount::downgrade(&self.inner);
      let dispatcher = spawn_dispatcher(weak.clone());
      *self.inner.dispatcher.lock() = Some(dispatcher);
      let reconnect = install_reconnect_handler(&self.inner.client, weak);
      *self.inner.reconnect.lock() = Some(reconnect);
    }

    // Publish initialized=true only after every fallible step has succeeded.
    // Any earlier `?` returns leave the locker un-initialized so the caller
    // can fix the underlying issue (e.g. RESP2 mode without disable_caching)
    // and retry.
    self.inner.initialized.store(true, Ordering::Release);
    log::debug!(
      "[locker:{}] initialized (prefix={}, validity={:?}, extend_every={:?}, no_caching={})",
      self.inner.client.id(),
      self.inner.prefix,
      self.inner.validity,
      self.inner.extend_every,
      self.inner.no_caching,
    );
    Ok(())
  }

  /// Acquire the lock identified by `name`, waiting until it becomes
  /// available. Returns [`ErrorKind::Canceled`] if [`close`] runs while we
  /// were blocked on a free gate.
  ///
  /// [`close`]: Locker::close
  pub async fn acquire(&self, name: impl Into<Str>) -> Result<LockGuard, Error> {
    let name: Str = name.into();
    loop {
      if self.inner.closed.load(Ordering::Acquire) {
        return Err(locker_closed());
      }
      let Some(gate) = self.inner.get_or_create_gate(&name) else {
        return Err(locker_closed());
      };

      // Subscribe to the gate *before* the redis call so an invalidation
      // push that lands while we're still on the wire isn't lost. `enable()`
      // registers the future as a waiter eagerly — without it `notified()`
      // is a no-op until first `.await`, which would open a real race.
      let waiting = gate.free.notified();
      tokio::pin!(waiting);
      waiting.as_mut().enable();

      match self.try_once(&name, &gate, false).await {
        Ok(guard) => return Ok(guard),
        Err(e) if e.kind() == &ErrorKind::NotFound => {
          // contended — fall through to wait
        },
        Err(e) => {
          self.inner.drop_gate(&name, &gate);
          return Err(e);
        },
      }

      // No invalidation push → no wake-up. Fall back to time-based polling
      // so we still make progress on RESP2 / cache-disabled deployments.
      if self.inner.no_caching {
        let _ = tokio::time::timeout(self.inner.try_next, waiting).await;
      } else {
        waiting.await;
      }
      self.inner.drop_gate(&name, &gate);
    }
  }

  /// Try to acquire the lock without waiting. Returns [`ErrorKind::NotFound`]
  /// (the fred analogue of `rueidislock.ErrNotLocked`) when another holder
  /// already owns it.
  pub async fn try_acquire(&self, name: impl Into<Str>) -> Result<LockGuard, Error> {
    let name: Str = name.into();
    if self.inner.closed.load(Ordering::Acquire) {
      return Err(locker_closed());
    }
    let Some(gate) = self.inner.get_or_create_gate(&name) else {
      return Err(locker_closed());
    };
    let res = self.try_once(&name, &gate, false).await;
    if res.is_err() {
      self.inner.drop_gate(&name, &gate);
    }
    res
  }

  /// Forcibly take over the lock identified by `name`, evicting whoever
  /// held it. The displaced holder's [`LockGuard::cancelled`] future
  /// resolves on the next invalidation push so it can shed work cleanly.
  pub async fn acquire_force(&self, name: impl Into<Str>) -> Result<LockGuard, Error> {
    let name: Str = name.into();
    if self.inner.closed.load(Ordering::Acquire) {
      return Err(locker_closed());
    }
    let Some(gate) = self.inner.get_or_create_gate(&name) else {
      return Err(locker_closed());
    };
    let res = self.try_once(&name, &gate, true).await;
    if res.is_err() {
      self.inner.drop_gate(&name, &gate);
    }
    res
  }

  /// Underlying client. Useful when the locker should share its connection
  /// pool with the rest of the application.
  pub fn client(&self) -> &Client {
    &self.inner.client
  }

  /// Disable tracking, signal every outstanding [`LockGuard`] to release,
  /// and wake any blocked acquirers with [`ErrorKind::Canceled`].
  ///
  /// Outstanding extenders run their CAS-delete in the background after
  /// being signalled. They are detached, so `close` returns as soon as the
  /// signal is sent — callers wanting at-most-once cleanup should
  /// [`release`] each guard before calling `close`.
  ///
  /// [`release`]: LockGuard::release
  pub async fn close(&self) {
    if self.inner.closed.swap(true, Ordering::AcqRel) {
      return;
    }
    log::debug!("[locker:{}] closing", self.inner.client.id());

    // Drain & cancel every outstanding lock so each extender exits its loop
    // and runs its CAS-delete on the way out. After this point Locker is
    // unusable; new acquire attempts return Canceled.
    let cancellations: Vec<_> = std::mem::take(&mut *self.inner.outstanding.lock());
    for cancel in &cancellations {
      cancel.cancel();
    }

    self.inner.wake_all_gates();
    if !self.inner.no_caching {
      let _ = self.inner.client.stop_tracking().await;
    }
    if let Some(handle) = self.inner.dispatcher.lock().take() {
      handle.abort();
    }
    if let Some(handle) = self.inner.reconnect.lock().take() {
      handle.abort();
    }
  }

  async fn try_once(&self, name: &Str, gate: &RefCount<Gate>, force: bool) -> Result<LockGuard, Error> {
    let token = random_token();
    let key = self.inner.keyname(name);

    let deadline = SystemTime::now() + self.inner.validity;
    let arg = if self.inner.fallback_px {
      self.inner.validity.as_millis().to_string()
    } else {
      deadline_unix_millis(deadline)?.to_string()
    };
    let script = if force {
      &self.inner.scripts.force
    } else {
      &self.inner.scripts.acquire
    };

    let resp: Option<Str> = script
      .evalsha_with_reload(&self.inner.client, vec![key.clone()], vec![token.clone(), arg])
      .await?;
    if !force && resp.is_none() {
      return Err(not_locked());
    }

    let cancel = RefCount::new(Cancellation::new());
    if !self.inner.register_outstanding(&cancel) {
      // close() raced with us: it published `closed = true` and drained the
      // outstanding list before we could register. Clean up the key we just
      // wrote so it doesn't squat in redis until its TTL expires, then bail.
      let _: Result<i64, Error> = self
        .inner
        .scripts
        .del
        .evalsha_with_reload(&self.inner.client, vec![key.clone()], vec![token.clone()])
        .await;
      return Err(locker_closed());
    }

    let extender = spawn_extender(
      self.inner.clone(),
      gate.clone(),
      key.clone(),
      Str::from(token.as_str()),
      cancel.clone(),
    );

    log::debug!(
      "[locker:{}] acquired {} = {} (force={})",
      self.inner.client.id(),
      key,
      &token[.. TOKEN_LOG_PREFIX.min(token.len())],
      force,
    );

    Ok(LockGuard {
      inner:    self.inner.clone(),
      gate:     gate.clone(),
      name:     name.clone(),
      key,
      token:    Str::from(token),
      cancel,
      extender: Some(extender),
    })
  }
}

/// A scope guard representing a held distributed lock. The lock remains
/// valid until [`release`] is awaited or the guard is dropped.
///
/// [`release`]: LockGuard::release
#[cfg_attr(docsrs, doc(cfg(feature = "locker")))]
pub struct LockGuard {
  inner:    RefCount<LockerInner>,
  gate:     RefCount<Gate>,
  name:     Str,
  key:      String,
  token:    Str,
  cancel:   RefCount<Cancellation>,
  extender: Option<JoinHandle<()>>,
}

impl fmt::Debug for LockGuard {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LockGuard")
      .field("key", &self.key)
      .field("lost", &self.cancel.is_cancelled())
      .finish()
  }
}

impl LockGuard {
  /// Resolves the instant the lock is lost — TTL elapsed before our
  /// extender reached redis, server-side eviction, takeover via
  /// [`Locker::acquire_force`], or connection-level error. Useful inside
  /// a `tokio::select!` so long work can shed when it no longer owns
  /// the lock.
  pub async fn cancelled(&self) {
    self.cancel.cancelled().await;
  }

  /// Polling alternative to [`cancelled`]. Returns `true` once the lock
  /// has been lost.
  ///
  /// [`cancelled`]: LockGuard::cancelled
  pub fn is_lost(&self) -> bool {
    self.cancel.is_cancelled()
  }

  /// The redis key the lock is held under.
  pub fn key(&self) -> &str {
    &self.key
  }

  /// The unique token written under the lock key. Useful for tracing.
  pub fn token(&self) -> &str {
    &self.token
  }

  /// Release the lock gracefully: signal the extender to exit and await its
  /// CAS-delete on the way out. The closing `Drop` then wakes any
  /// in-process waiters and drops the gate.
  pub async fn release(mut self) {
    self.cancel.cancel();
    self.inner.deregister_outstanding(&self.cancel);
    if let Some(handle) = self.extender.take() {
      // The extender owns the DEL — `await` ensures the redis round-trip
      // lands before we return.
      let _ = handle.await;
    }
    log::debug!("[locker:{}] released {}", self.inner.client.id(), self.key);
    // `Drop` runs at the end of this fn, executing `notify_gate` +
    // `drop_gate` exactly once. We deliberately don't call them here to
    // avoid the refs counter wrapping around when Drop fires again on the
    // same fields.
  }
}

impl Drop for LockGuard {
  fn drop(&mut self) {
    // Signalling cancellation is enough: the detached extender's cleanup
    // branch will run the CAS-delete and exit. We do NOT spawn anything
    // from `Drop` — that would assume a tokio runtime is current, which
    // isn't always true (e.g. dropping inside `block_on`).
    //
    // `cancel.cancel()` and `deregister_outstanding` are idempotent so it's
    // safe that `release()` already called them on the explicit path.
    self.cancel.cancel();
    self.inner.deregister_outstanding(&self.cancel);
    if self.extender.is_some() {
      // Drop the JoinHandle without aborting — letting the task run to
      // completion is what cleans the redis key up. Aborting would strand
      // the key until its TTL elapses.
      let _ = self.extender.take();
    }
    // Wake local waiters last, so they see the gate ref-count drop in the
    // same sequence the redis state changes. Drop runs once per LockGuard,
    // so the gate accounting stays balanced regardless of whether the
    // caller went through `release().await` or just let the guard drop.
    self.inner.notify_gate(&self.name);
    self.inner.drop_gate(&self.name, &self.gate);
  }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

struct Scripts {
  acquire: Script,
  force:   Script,
  extend:  Script,
  del:     Script,
}

struct LockerInner {
  client:       Client,
  prefix:       Str,
  validity:     Duration,
  extend_every: Duration,
  try_next:     Duration,
  fallback_px:  bool,
  no_caching:   bool,
  no_loop:      bool,
  gates:        Mutex<HashMap<Str, RefCount<Gate>>>,
  outstanding:  Mutex<Vec<RefCount<Cancellation>>>,
  closed:       AtomicBool,
  initialized:  AtomicBool,
  // Serializes concurrent `init` calls so we never spawn duplicate
  // dispatchers or re-issue `CLIENT TRACKING` on the same client.
  init_guard:   tokio::sync::Mutex<()>,
  dispatcher:   Mutex<Option<JoinHandle<()>>>,
  // Re-arms `CLIENT TRACKING` on every reconnect. fred does not auto-
  // restore tracking when a connection drops, so without this hook every
  // active holder would silently stop receiving invalidation pushes after
  // the first reconnect.
  reconnect:    Mutex<Option<JoinHandle<Result<(), Error>>>>,
  scripts:      Scripts,
}

impl Drop for LockerInner {
  fn drop(&mut self) {
    // Fires when the last `Locker` clone is dropped without `close()` being
    // called first. Tasks captured `Weak<LockerInner>` so they don't keep
    // us alive — they'll exit on their next iteration anyway, but aborting
    // here lets the runtime clean them up immediately instead of waiting
    // for the next invalidation / reconnect to wake them.
    if let Some(handle) = self.dispatcher.get_mut().take() {
      handle.abort();
    }
    if let Some(handle) = self.reconnect.get_mut().take() {
      handle.abort();
    }
  }
}

impl LockerInner {
  fn keyname(&self, name: &str) -> String {
    let mut s = String::with_capacity(self.prefix.len() + 1 + name.len());
    s.push_str(&self.prefix);
    s.push(':');
    s.push_str(name);
    s
  }

  /// Parse `{prefix}:{name}` off an invalidation key. Returns `None` if the
  /// key isn't ours so the dispatcher can cheaply ignore unrelated keys.
  fn parse_key<'a>(&self, key: &'a str) -> Option<&'a str> {
    let prefix: &str = &self.prefix;
    key.strip_prefix(prefix)?.strip_prefix(':')
  }

  fn get_or_create_gate(&self, name: &Str) -> Option<RefCount<Gate>> {
    let mut gates = self.gates.lock();
    if self.closed.load(Ordering::Acquire) {
      return None;
    }
    let g = gates
      .entry(name.clone())
      .or_insert_with(|| RefCount::new(Gate::new()))
      .clone();
    g.refs.fetch_add(1, Ordering::AcqRel);
    Some(g)
  }

  fn drop_gate(&self, name: &Str, gate: &RefCount<Gate>) {
    let prev = gate.refs.fetch_sub(1, Ordering::AcqRel);
    if prev == 1 {
      let mut gates = self.gates.lock();
      if gate.refs.load(Ordering::Acquire) == 0 {
        if let Some(existing) = gates.get(name) {
          if RefCount::ptr_eq(existing, gate) {
            gates.remove(name);
          }
        }
      }
    }
  }

  /// Register a guard's cancellation handle so [`Locker::close`] can
  /// signal it. Returns `false` if the locker was closed under the
  /// same mutex — the caller must then bail out instead of spawning an
  /// extender, otherwise close() would never cancel the freshly-acquired
  /// guard. Serialized with close()'s drain via the `outstanding` mutex.
  fn register_outstanding(&self, cancel: &RefCount<Cancellation>) -> bool {
    let mut out = self.outstanding.lock();
    if self.closed.load(Ordering::Acquire) {
      return false;
    }
    out.push(cancel.clone());
    true
  }

  fn deregister_outstanding(&self, cancel: &RefCount<Cancellation>) {
    let mut out = self.outstanding.lock();
    out.retain(|c| !RefCount::ptr_eq(c, cancel));
  }

  /// Wake every gate. Used when the locker is closed so any pending
  /// acquirer observes the change immediately, and on broadcast lag so
  /// active holders re-verify their state.
  fn wake_all_gates(&self) {
    let gates = self.gates.lock();
    for g in gates.values() {
      g.free.notify_waiters();
      // `notify_one` queues a permit if nobody is waiting — so even a
      // freshly-spawned extender that hasn't reached its select yet won't
      // miss this wake.
      g.holder.notify_one();
    }
  }

  fn notify_gate(&self, name: &str) {
    let gates = self.gates.lock();
    if let Some(g) = gates.get(name) {
      // Single-permit semantics matching `rueidislock`'s buffered chan
      // size=1: at most one waiter wakes; later arrivals will retry on
      // their own loop iteration and see the same state.
      g.free.notify_one();
      g.holder.notify_one();
    }
  }
}

/// Per-name in-process coordination. One `Gate` per lock name, refcounted by
/// every acquirer/waiter so the entry drops from the map automatically once
/// nobody cares about the name.
struct Gate {
  /// Notified when the lock *might* be free — released by the local holder,
  /// invalidated by the server, or the polling fallback fired.
  free:   Notify,
  /// Notified when the lock key was modified server-side. The current
  /// holder's extender selects on this to react to takeovers immediately
  /// instead of waiting for the next extension tick.
  holder: Notify,
  refs:   AtomicUsize,
}

impl Gate {
  fn new() -> Self {
    Gate {
      free:   Notify::new(),
      holder: Notify::new(),
      refs:   AtomicUsize::new(0),
    }
  }
}

/// Wake state. `cancelled()` resolves the first time `cancel()` is called;
/// subsequent observers see it immediately.
///
/// Internally `notify_waiters()` is used to wake any number of concurrent
/// observers (the holding `LockGuard`'s `cancelled()` future + the extender
/// task that internally selects on it). `notify_waiters` has no backlog, so
/// observers must register themselves with `Notified::enable()` *before*
/// the second flag check — otherwise a `cancel()` racing between the load
/// and the registration would be silently lost.
struct Cancellation {
  flag:   AtomicBool,
  notify: Notify,
}

impl Cancellation {
  fn new() -> Self {
    Cancellation {
      flag:   AtomicBool::new(false),
      notify: Notify::new(),
    }
  }

  fn cancel(&self) {
    if !self.flag.swap(true, Ordering::AcqRel) {
      self.notify.notify_waiters();
    }
  }

  fn is_cancelled(&self) -> bool {
    self.flag.load(Ordering::Acquire)
  }

  async fn cancelled(&self) {
    if self.is_cancelled() {
      return;
    }
    let fut = self.notify.notified();
    tokio::pin!(fut);
    // Eager-register as a waiter so any `cancel()` racing between here and
    // the recheck below will wake us via `notify_waiters`.
    fut.as_mut().enable();
    if self.is_cancelled() {
      return;
    }
    fut.await;
  }
}

/// Subscribe to the client's invalidation channel and dispatch per-key
/// notifications to the matching `Gate`. Holds a `Weak<LockerInner>` so the
/// dispatcher never keeps the Locker alive — drop the last `Locker` clone
/// and the task wakes on its next message, sees the upgrade fail, and
/// exits. `close` (or `Drop for LockerInner`) aborts it eagerly.
fn spawn_dispatcher(inner_weak: Weak<LockerInner>) -> JoinHandle<()> {
  // We need the broadcast Receiver bound to the client; obtain it once via
  // an upgrade. After this point only the weak ref is captured.
  let Some(inner) = inner_weak.upgrade() else {
    return spawn(async {});
  };
  let mut rx = inner.client.invalidation_rx();
  let id = inner.client.id().to_string();
  drop(inner);
  spawn(async move {
    loop {
      match rx.recv().await {
        Ok(invalidation) => {
          let Some(inner) = inner_weak.upgrade() else { break };
          dispatch(&inner, &invalidation);
        },
        Err(BroadcastRecvError::Lagged(skipped)) => {
          let Some(inner) = inner_weak.upgrade() else { break };
          log::warn!(
            "[locker:{}] dropped {} invalidation messages; waking all gates",
            id,
            skipped,
          );
          inner.wake_all_gates();
        },
        Err(BroadcastRecvError::Closed) => {
          log::debug!("[locker:{}] invalidation stream closed", id);
          break;
        },
      }
    }
  })
}

/// Install an `on_reconnect` listener that re-arms `CLIENT TRACKING` after
/// every reconnect. Without this hook fred would silently drop tracking on
/// any connection bounce, leaving active holders unable to receive
/// invalidation pushes — they'd only notice takeovers at the next extend
/// tick. The handler swallows errors so a transient failure on one
/// reconnect doesn't kill the listener.
fn install_reconnect_handler(client: &Client, inner_weak: Weak<LockerInner>) -> JoinHandle<Result<(), Error>> {
  client.on_reconnect(move |server: Server| {
    let inner_weak = inner_weak.clone();
    async move {
      let Some(inner) = inner_weak.upgrade() else {
        return Ok(());
      };
      log::debug!(
        "[locker:{}] reconnect to {}; re-arming tracking",
        inner.client.id(),
        server,
      );
      if let Err(err) = inner
        .client
        .start_tracking(None, false, false, true, inner.no_loop)
        .await
      {
        log::warn!(
          "[locker:{}] failed to re-arm tracking after reconnect to {}: {}",
          inner.client.id(),
          server,
          err,
        );
      }
      // Wake every active holder so they re-verify on their next tick —
      // anything that changed during the reconnect window is fair game.
      inner.wake_all_gates();
      Ok(())
    }
  })
}

fn dispatch(inner: &LockerInner, invalidation: &Invalidation) {
  if invalidation.keys.is_empty() {
    // Empty payload — the server flushed tracking state (typically on a
    // reconnect). Wake everyone so they re-arm.
    inner.wake_all_gates();
    return;
  }
  for key in &invalidation.keys {
    let Ok(key_str) = std::str::from_utf8(key.as_bytes()) else {
      continue;
    };
    if let Some(name) = inner.parse_key(key_str) {
      inner.notify_gate(name);
    }
  }
}

/// Background task that keeps a single key's TTL alive. Exits when the
/// holding `LockGuard` cancels, when the server tells us our token is gone,
/// or when the client errors out. On exit it runs the CAS-delete so the
/// key vacates immediately rather than waiting for its TTL.
fn spawn_extender(
  inner: RefCount<LockerInner>,
  gate: RefCount<Gate>,
  key: String,
  token: Str,
  cancel: RefCount<Cancellation>,
) -> JoinHandle<()> {
  spawn(async move {
    let id = inner.client.id().to_string();
    let short = short_token(&token);
    loop {
      // Block on whichever fires first: external cancellation, the
      // extension timer, or an invalidation push for our key.
      tokio::select! {
        biased;
        _ = cancel.cancelled() => break,
        _ = crate::runtime::sleep(inner.extend_every) => {
          log::trace!("[locker:{}] tick extend {} ({})", id, key, short);
        },
        _ = gate.holder.notified() => {
          log::trace!("[locker:{}] invalidation extend {} ({})", id, key, short);
        },
      }
      if cancel.is_cancelled() {
        break;
      }
      // Re-extend regardless of which arm fired — invalidation means the key
      // changed (someone may have taken it over), and we use the extend
      // script as a CAS probe to confirm we still own it.
      let new_deadline = SystemTime::now() + inner.validity;
      let arg = if inner.fallback_px {
        inner.validity.as_millis().to_string()
      } else {
        match deadline_unix_millis(new_deadline) {
          Ok(v) => v.to_string(),
          Err(err) => {
            log::warn!("[locker:{}] extend skipped for {}: {}", id, key, err);
            break;
          },
        }
      };
      let result: Result<i64, Error> = inner
        .scripts
        .extend
        .evalsha_with_reload(&inner.client, vec![key.clone()], vec![token.to_string(), arg])
        .await;
      match result {
        Ok(1) => continue,
        Ok(_) => {
          log::debug!("[locker:{}] lost {} ({}) — token mismatch", id, key, short);
          break;
        },
        Err(err) => {
          log::warn!("[locker:{}] extend failed for {} ({}): {}", id, key, short, err);
          break;
        },
      }
    }
    // Signal first so any cancelled().await observers wake before we hit the
    // wire — the DEL is best-effort cleanup, not a synchronization point.
    cancel.cancel();
    let del: Result<i64, Error> = inner
      .scripts
      .del
      .evalsha_with_reload(&inner.client, vec![key.clone()], vec![token.to_string()])
      .await;
    if let Err(err) = del {
      log::debug!("[locker:{}] release-del best-effort failed for {}: {}", id, key, err);
    }
  })
}

fn random_token() -> String {
  rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(TOKEN_LEN)
    .map(char::from)
    .collect()
}

fn short_token(token: &str) -> &str {
  &token[.. TOKEN_LOG_PREFIX.min(token.len())]
}

fn deadline_unix_millis(when: SystemTime) -> Result<i64, Error> {
  when
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_millis() as i64)
    .map_err(|e| Error::new(ErrorKind::Unknown, format!("system clock before UNIX epoch: {e}")))
}

fn locker_closed() -> Error {
  Error::new(ErrorKind::Canceled, "Locker is closed")
}

fn not_locked() -> Error {
  Error::new(ErrorKind::NotFound, "Lock is held by another acquirer")
}
