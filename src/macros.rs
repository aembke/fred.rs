#![allow(unused_macros)]

macro_rules! to(
  ($val:ident) => {
    crate::utils::try_into($val)
  }
);

macro_rules! _trace(
  ($inner:tt, $($arg:tt)*) => { {
    $inner.log_client_name_fn(log::Level::Trace, |name| {
      log::trace!("{}: {}", name, format!($($arg)*));
    })
   } }
);

macro_rules! _debug(
  ($inner:tt, $($arg:tt)*) => { {
    $inner.log_client_name_fn(log::Level::Debug, |name| {
      log::debug!("{}: {}", name, format!($($arg)*));
    })
   } }
);

macro_rules! _error(
  ($inner:tt, $($arg:tt)*) => { {
    $inner.log_client_name_fn(log::Level::Error, |name| {
      log::error!("{}: {}", name, format!($($arg)*));
    })
   } }
);

macro_rules! _warn(
  ($inner:tt, $($arg:tt)*) => { {
    $inner.log_client_name_fn(log::Level::Warn, |name| {
      log::warn!("{}: {}", name, format!($($arg)*));
    })
   } }
);

macro_rules! _info(
  ($inner:tt, $($arg:tt)*) => { {
    $inner.log_client_name_fn(log::Level::Info, |name| {
      log::info!("{}: {}", name, format!($($arg)*));
    })
   } }
);

/// Span used within the client that uses the command's span ID as the parent.
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
macro_rules! fspan (
  ($cmd:ident, $($arg:tt)*) => {
    tracing::span!(parent: $cmd.traces.cmd_id.clone(), tracing::Level::DEBUG, $($arg)*)
  }
);

/// Fake span used within the client that uses the command's span ID as the parent.
#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
macro_rules! fspan (
  ($cmd:ident, $($arg:tt)*) => {
    crate::trace::Span {}
  }
);

/// Async try! for `AsyncResult`. This is rarely used on its own, but rather as a part of try_into!.
macro_rules! atry (
  ($expr:expr) => {
    match $expr {
      Ok(val) => val,
      Err(e) => return crate::interfaces::AsyncResult::from(Err(e))
    }
  }
);

macro_rules! static_str(
  ($name:ident, $val:expr) => {
    lazy_static::lazy_static! {
      pub(crate) static ref $name: Str = {
        crate::utils::static_str($val)
      };
    }
  }
);

macro_rules! into (
  ($val:ident) => (let $val = $val.into(););
  ($v1:ident, $v2:ident) => (
    let ($v1, $v2) = ($v1.into(), $v2.into());
  );
  ($v1:ident, $v2:ident, $v3:ident) => (
    let ($v1, $v2, $v3) = ($v1.into(), $v2.into(), $v3.into());
  );
  ($v1:ident, $v2:ident, $v3:ident, $v4:ident) => (
    let ($v1, $v2, $v3, $v4) = ($v1.into(), $v2.into(), $v3.into(), $v4.into());
  );
  ($v1:ident, $v2:ident, $v3:ident, $v4:ident, $v5:ident) => (
    let ($v1, $v2, $v3, $v4, $v5) = ($v1.into(), $v2.into(), $v3.into(), $v4.into(), $v5.into());
  );
  // add to this as needed
);

macro_rules! try_into (
  ($val:ident) => (let $val = atry!(to!($val)););
  ($v1:ident, $v2:ident) => (
    let ($v1, $v2) = (atry!(to!($v1)), atry!(to!($v2)));
  );
  ($v1:ident, $v2:ident, $v3:ident) => (
    let ($v1, $v2, $v3) = (atry!(to!($v1)), atry!(to!($v2)), atry!(to!($v3)));
  );
  ($v1:ident, $v2:ident, $v3:ident, $v4:ident) => (
    let ($v1, $v2, $v3, $v4) = (atry!(to!($v1)), atry!(to!($v2)), atry!(to!($v3)), atry!(to!($v4)));
  );
  ($v1:ident, $v2:ident, $v3:ident, $v4:ident, $v5:ident) => (
    let ($v1, $v2, $v3, $v4, $v5) = (atry!(to!($v1)), atry!(to!($v2)), atry!(to!($v3)), atry!(to!($v4)), atry!(to!($v5)));
  );
  // add to this as needed
);
