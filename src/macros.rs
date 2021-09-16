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

macro_rules! to_signed_number(
  ($t:ty, $v:ident) => {
    match $v {
      RedisValue::Integer(i) => Ok(i as $t),
      RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
      _ => Err(RedisError::new(RedisErrorKind::Parse, "Cannot convert to number.")),
    }
  }
);

macro_rules! to_unsigned_number(
  ($t:ty, $v:ident) => {
    match $v {
      RedisValue::Integer(i) => if i < 0 {
        Err(RedisError::new(RedisErrorKind::Parse, "Cannot convert from negative number."))
      }else{
        Ok(i as $t)
      },
      RedisValue::String(s) => s.parse::<$t>().map_err(|e| e.into()),
      _ => Err(RedisError::new(RedisErrorKind::Parse, "Cannot convert to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl RedisResponse for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        to_signed_number!($t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl RedisResponse for $t {
      fn from_value(value: RedisValue) -> Result<$t, RedisError> {
        to_unsigned_number!($t, value)
      }
    }
  }
);

macro_rules! convert (
  ($t:ty, $value:expr) => {
    <$t>::from_value($value?)
  }
);
