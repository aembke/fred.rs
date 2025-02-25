#![allow(unused_macros)]

macro_rules! to(
  ($val:ident) => {
    crate::utils::try_into($val)
  }
);

/// Span used within the client that uses the command's span ID as the parent.
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
macro_rules! fspan (
  ($cmd:ident, $lvl:expr, $($arg:tt)*) => { {
    let _id = $cmd.traces.cmd.as_ref().and_then(|c| c.id());
    span_lvl!($lvl, parent: _id, $($arg)*)
  } }
);

macro_rules! span_lvl {
    ($lvl:expr, $($args:tt)*) => {{
        match $lvl {
            tracing::Level::ERROR => tracing::error_span!($($args)*),
            tracing::Level::WARN => tracing::warn_span!($($args)*),
            tracing::Level::INFO => tracing::info_span!($($args)*),
            tracing::Level::DEBUG => tracing::debug_span!($($args)*),
            tracing::Level::TRACE => tracing::trace_span!($($args)*),
        }
    }};
}

/// Fake span used within the client that uses the command's span ID as the parent.
#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
macro_rules! fspan (
  ($cmd:ident, $($arg:tt)*) => {
    crate::trace::Span {}
  }
);

macro_rules! static_val(
  ($val:expr) => {
    Value::from_static_str($val)
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
  ($val:ident) => (let $val = to!($val)?;);
  ($v1:ident, $v2:ident) => (
    let ($v1, $v2) = (to!($v1)?, to!($v2)?);
  );
  ($v1:ident, $v2:ident, $v3:ident) => (
    let ($v1, $v2, $v3) = (to!($v1)?, to!($v2)?, to!($v3)?);
  );
  ($v1:ident, $v2:ident, $v3:ident, $v4:ident) => (
    let ($v1, $v2, $v3, $v4) = (to!($v1)?, to!($v2)?, to!($v3)?, to!($v4)?);
  );
  ($v1:ident, $v2:ident, $v3:ident, $v4:ident, $v5:ident) => (
    let ($v1, $v2, $v3, $v4, $v5) = (to!($v1)?, to!($v2)?, to!($v3)?, to!($v4)?, to!($v5)?);
  );
  // add to this as needed
);
