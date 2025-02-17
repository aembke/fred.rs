//!
//!

/// Shared type declarations.
pub mod types;
/// Networking types.
pub mod net;
/// Response type conversion types.
pub mod responses;
/// DNS configuration interfaces.
#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub mod dns;
/// Common tracing abstractions that work with tracing feature flags.
pub mod trace;
/// Shared commands used by the base [ClientLike](runtime::ClientLike) trait.
pub mod commands;
/// Error structs returned by Valkey & Redis commands.
pub mod error;
/// Shared state between caller tasks and the router task.
pub mod inner;
/// Interfaces for interacting with the router channels.
pub mod router;
/// Runtime abstractions for Tokio, Glommio, and Monoio.
pub mod runtime;
/// Common utility functions.
pub mod utils;

/// Type alias for `Result<T, Error>`.
pub type FredResult<T> = Result<T, error::Error>;
