//!

/// Shared commands used by the base [ClientLike](runtime::ClientLike) trait.
mod commands;
/// Error structs returned by Valkey & Redis commands.
pub mod error;
/// Shared state between caller tasks and the router task.
pub mod inner;
/// Private interfaces for interacting with the router channels.
mod router;
/// Runtime abstractions for Tokio, Glommio, and Monoio.
pub mod runtime;
/// Common utility functions.
pub mod utils;

/// Type alias for `Result<T, Error>`.
pub type FredResult<T> = Result<T, error::Error>;
