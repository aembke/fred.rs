//!

/// Shared commands used by the base [ClientLike](runtime::ClientLike) trait.
pub mod commands;
/// Error structs returned by Valkey & Redis commands.
pub mod error;
/// Shared state between caller tasks and the router task.
pub mod inner;
#[macro_use]
pub mod macros;
/// Networking types.
pub mod net;
/// Interfaces for interacting with the router channels.
pub mod router;
/// Runtime abstractions for Tokio, Glommio, and Monoio.
pub mod runtime;
/// Common tracing abstractions that work with tracing feature flags.
pub mod trace;
/// Shared type declarations.
pub mod types;
/// Common utility functions.
pub mod utils;

/// Type alias for `Result<T, Error>`.
pub type FredResult<T> = Result<T, error::Error>;
