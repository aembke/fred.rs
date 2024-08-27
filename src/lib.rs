#![allow(clippy::unnecessary_fallible_conversions)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::iter_kv_map)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::vec_init_then_push)]
#![allow(clippy::while_let_on_iterator)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::new_without_default)]
#![allow(clippy::assigning_clones)]
#![warn(clippy::large_types_passed_by_value)]
#![warn(clippy::large_stack_frames)]
#![warn(clippy::large_futures)]
#![cfg_attr(docsrs, deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![doc = include_str!("../README.md")]

#[cfg(any(feature = "dns", feature = "replicas"))]
#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate log;

pub extern crate bytes;
pub extern crate bytes_utils;
#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
pub extern crate native_tls;
#[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))))]
pub extern crate rustls;
#[cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-rustls", feature = "enable-rustls-ring"))))]
pub extern crate rustls_native_certs;
#[cfg(feature = "serde-json")]
pub extern crate serde_json;
pub extern crate socket2;
#[cfg(feature = "partial-tracing")]
#[cfg_attr(docsrs, doc(cfg(feature = "partial-tracing")))]
pub extern crate tracing;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
extern crate tracing_futures;
#[macro_use]
mod macros;

mod commands;
#[cfg(feature = "glommio")]
mod glommio;
mod modules;
mod protocol;
mod router;
mod trace;
mod utils;

/// Redis client implementations.
pub mod clients;
/// Error structs returned by Redis commands.
pub mod error;
/// Traits that implement portions of the Redis interface.
pub mod interfaces;
#[cfg(feature = "mocks")]
#[cfg_attr(docsrs, doc(cfg(feature = "mocks")))]
pub use modules::mocks;
/// An interface to run the `MONITOR` command.
#[cfg(feature = "monitor")]
#[cfg_attr(docsrs, doc(cfg(feature = "monitor")))]
pub mod monitor;
/// The structs and enums used by the Redis client.
pub mod types;

#[cfg(feature = "glommio")]
use glommio::runtime_compat;

#[cfg(not(feature = "glommio"))]
mod runtime_compat {
  use crate::glommio::runtime_types::broadcast_channel;
  use std::sync::Arc;
  use tokio::sync::broadcast::{Receiver, Sender};
  pub use tokio::{
    sync::{
      broadcast::{self, error::SendError as BroadcastSendError},
      mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
      oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender},
      RwLock as AsyncRwLock,
    },
    task::JoinHandle,
    time::sleep,
  };

  pub type RefCount<T> = Arc<T>;

  pub type BroadcastSender<T> = Sender<T>;
  pub type BroadcastReceiver<T> = Receiver<T>;

  pub fn broadcast_send<T, F: Fn(&T)>(tx: &BroadcastSender<T>, msg: &T, func: F) {
    if let Err(BroadcastSendError(val)) = tx.send(msg.clone()) {
      func(&val);
    }
  }

  pub fn broadcast_channel<T>(capacity: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    broadcast::channel(capacity)
  }
}

/// Various client utility functions.
pub mod util {
  pub use crate::utils::{f64_to_redis_string, redis_string_to_f64, static_bytes, static_str};
  use crate::{error::RedisError, types::RedisKey};
  pub use redis_protocol::redis_keyslot;
  use std::collections::{BTreeMap, VecDeque};

  /// A convenience constant for `None` values used as generic arguments.
  ///
  /// Functions that take `Option<T>` as an argument often require the caller to use a turbofish when the
  /// variant is `None`. In many cases this constant can be used instead.
  // pretty much everything in this crate supports From<String>
  pub const NONE: Option<String> = None;

  /// Calculate the SHA1 hash output as a hex string. This is provided for clients that use the Lua interface to
  /// manage their own script caches.
  #[cfg(feature = "sha-1")]
  #[cfg_attr(docsrs, doc(cfg(feature = "sha-1")))]
  pub fn sha1_hash(input: &str) -> String {
    use sha1::Digest;

    let mut hasher = sha1::Sha1::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
  }

  /// Group the provided arguments by their cluster hash slot.
  ///
  /// This can be useful with commands that require all keys map to the same hash slot, such as `SSUBSCRIBE`,
  /// `MGET`, etc.
  ///
  /// ```rust
  /// # use fred::prelude::*;
  /// async fn example(client: impl KeysInterface) -> Result<(), RedisError> {
  ///   let keys = vec!["foo", "bar", "baz", "a{1}", "b{1}", "c{1}"];
  ///   let groups = fred::util::group_by_hash_slot(keys)?;
  ///
  ///   for (slot, keys) in groups.into_iter() {
  ///     // `MGET` requires that all arguments map to the same hash slot
  ///     println!("{:?}", client.mget::<Vec<String>, _>(keys).await?);
  ///   }
  ///   Ok(())
  /// }
  /// ```
  pub fn group_by_hash_slot<T>(
    args: impl IntoIterator<Item = T>,
  ) -> Result<BTreeMap<u16, VecDeque<RedisKey>>, RedisError>
  where
    T: TryInto<RedisKey>,
    T::Error: Into<RedisError>,
  {
    let mut out = BTreeMap::new();

    for arg in args.into_iter() {
      let arg: RedisKey = to!(arg)?;
      let slot = redis_keyslot(arg.as_bytes());

      out.entry(slot).or_insert(VecDeque::new()).push_back(arg);
    }
    Ok(out)
  }
}

/// Convenience module to import a `RedisClient`, all possible interfaces, error types, and common argument types or
/// return value types.
pub mod prelude {
  #[cfg(feature = "partial-tracing")]
  #[cfg_attr(docsrs, doc(cfg(feature = "partial-tracing")))]
  pub use crate::types::TracingConfig;

  pub use crate::{
    clients::{RedisClient, RedisPool},
    error::{RedisError, RedisErrorKind},
    interfaces::*,
    types::{
      Blocking,
      Builder,
      ConnectionConfig,
      Expiration,
      FromRedis,
      Options,
      PerformanceConfig,
      ReconnectPolicy,
      RedisConfig,
      RedisKey,
      RedisValue,
      RedisValueKind,
      Server,
      ServerConfig,
      SetOptions,
      TcpConfig,
    },
  };

  #[cfg(any(
    feature = "enable-native-tls",
    feature = "enable-rustls",
    feature = "enable-rustls-ring"
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "enable-rustls",
      feature = "enable-native-tls",
      feature = "enable-rustls-ring"
    )))
  )]
  pub use crate::types::{TlsConfig, TlsConnector};
}
