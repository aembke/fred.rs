#[cfg(not(any(feature = "glommio", feature = "smol", feature = "monoio", feature = "cloudflare")))]
mod _tokio;
#[cfg(not(any(feature = "glommio", feature = "smol", feature = "monoio", feature = "cloudflare")))]
pub use _tokio::*;

#[cfg(feature = "cloudflare")]
pub(crate) mod cloudflare;
#[cfg(feature = "cloudflare")]
pub use cloudflare::compat::*;

#[cfg(feature = "glommio")]
pub(crate) mod glommio;
#[cfg(feature = "glommio")]
pub use glommio::compat::*;

#[cfg(any(feature = "glommio", feature = "smol", feature = "monoio", feature = "cloudflare"))]
mod sync;
#[cfg(any(feature = "glommio", feature = "smol", feature = "monoio", feature = "cloudflare"))]
pub use sync::*;

#[cfg(not(any(feature = "glommio", feature = "cloudflare")))]
pub use _tokio::ClientLike;
#[cfg(feature = "cloudflare")]
pub use cloudflare::interfaces::ClientLike;
#[cfg(feature = "glommio")]
pub use glommio::interfaces::ClientLike;

#[cfg(not(any(feature = "glommio", feature = "cloudflare")))]
pub(crate) use _tokio::spawn_event_listener;
#[cfg(feature = "cloudflare")]
pub(crate) use cloudflare::interfaces::spawn_event_listener;
#[cfg(feature = "glommio")]
#[doc(hidden)]
pub(crate) use glommio::interfaces::spawn_event_listener;
