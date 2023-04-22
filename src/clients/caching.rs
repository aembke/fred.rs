use crate::{
  interfaces::{
    AclInterface,
    ClientLike,
    FunctionInterface,
    GeoInterface,
    HashesInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    MemoryInterface,
    SetsInterface,
    SortedSetsInterface,
    StreamsInterface,
  },
  modules::inner::RedisClientInner,
  protocol::command::RedisCommand,
  utils,
};
use std::{
  fmt,
  fmt::Formatter,
  sync::{atomic::AtomicBool, Arc},
};

/// A struct for controlling [client caching](https://redis.io/commands/client-caching/) on commands.
///
/// ```rust no_run
/// # use fred::prelude::*;
///
/// async fn example(client: &RedisClient) -> Result<(), RedisError> {
///   // send `CLIENT CACHING no` before `HSET foo bar baz`
///   let _ = client.caching(false).hset("foo", "bar", "baz").await?;
///
///   // or reuse the caching interface
///   let caching = client.caching(true);
///   // send `CLIENT CACHING yes` before each `incr` command
///   println!("abc: {}", caching.incr::<i64, _>("abc").await?);
///   println!("abc: {}", caching.incr::<i64, _>("abc").await?);
///   Ok(())
/// }
/// ```
#[derive(Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "client-tracking")))]
pub struct Caching {
  inner:   Arc<RedisClientInner>,
  enabled: Arc<AtomicBool>,
}

impl fmt::Debug for Caching {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("Caching")
      .field("id", &self.inner.id)
      .field("enabled", &utils::read_bool_atomic(&self.enabled))
      .finish()
  }
}

impl Caching {
  pub(crate) fn new(inner: &Arc<RedisClientInner>, value: bool) -> Caching {
    Caching {
      inner:   inner.clone(),
      enabled: Arc::new(AtomicBool::new(value)),
    }
  }

  /// Read whether caching is enabled.
  pub fn is_enabled(&self) -> bool {
    utils::read_bool_atomic(&self.enabled)
  }

  /// Set whether caching is enabled, returning the previous value.
  pub fn set_enabled(&self, val: bool) -> bool {
    utils::set_bool_atomic(&self.enabled, val)
  }
}

impl ClientLike for Caching {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }

  #[doc(hidden)]
  fn change_command(&self, cmd: &mut RedisCommand) {
    cmd.caching = Some(utils::read_bool_atomic(&self.enabled));
  }
}

impl AclInterface for Caching {}
impl GeoInterface for Caching {}
impl HashesInterface for Caching {}
impl HyperloglogInterface for Caching {}
impl KeysInterface for Caching {}
impl ListInterface for Caching {}
impl MemoryInterface for Caching {}
impl SetsInterface for Caching {}
impl SortedSetsInterface for Caching {}
impl FunctionInterface for Caching {}
impl StreamsInterface for Caching {}
