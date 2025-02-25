use crate::{
  error::{Error, ErrorKind},
  runtime::{broadcast_channel, AtomicBool, AtomicUsize, BroadcastSender, Mutex, RefCount, RefSwap, RwLock},
  types::{
    values::{Key, Map, Value},
    Server,
  },
};
use bytes::Bytes;
use bytes_utils::Str;
use float_cmp::approx_eq;
use rand::{distributions::Alphanumeric, Rng};
use std::{
  collections::HashMap,
  path::{Path, PathBuf},
  sync::atomic::Ordering,
};
use url::Url;
use urlencoding::decode as percent_decode;

#[cfg(any(
  feature = "enable-native-tls",
  feature = "enable-rustls",
  feature = "enable-rustls-ring"
))]
use crate::types::config::{TlsConfig, TlsConnector};

const REDIS_TLS_SCHEME: &str = "rediss";
const VALKEY_TLS_SCHEME: &str = "valkeys";
const CLUSTER_SCHEME_SUFFIX: &str = "-cluster";
const SENTINEL_SCHEME_SUFFIX: &str = "-sentinel";
const UNIX_SCHEME_SUFFIX: &str = "+unix";
const SENTINEL_NAME_QUERY: &str = "sentinelServiceName";
const CLUSTER_NODE_QUERY: &str = "node";
#[cfg(feature = "sentinel-auth")]
const SENTINEL_USERNAME_QUERY: &str = "sentinelUsername";
#[cfg(feature = "sentinel-auth")]
const SENTINEL_PASSWORD_QUERY: &str = "sentinelPassword";

/// Create a `Str` from a static str slice without copying.
pub const fn static_str(s: &'static str) -> Str {
  // it's already parsed as a string
  unsafe { Str::from_inner_unchecked(Bytes::from_static(s.as_bytes())) }
}

/// Create a `Bytes` from static bytes without copying.
pub fn static_bytes(b: &'static [u8]) -> Bytes {
  Bytes::from_static(b)
}

pub fn f64_eq(lhs: f64, rhs: f64) -> bool {
  approx_eq!(f64, lhs, rhs, ulps = 2)
}

/// Convert a string to an `f64`, supporting "+inf" and "-inf".
pub fn string_to_f64(s: &str) -> Result<f64, Error> {
  // this is changing in newer versions of redis to lose the "+" prefix
  if s == "+inf" || s == "inf" {
    Ok(f64::INFINITY)
  } else if s == "-inf" {
    Ok(f64::NEG_INFINITY)
  } else {
    s.parse::<f64>().map_err(|_| {
      Error::new(
        ErrorKind::Unknown,
        format!("Could not convert {} to floating point value.", s),
      )
    })
  }
}

/// Convert an `f64` to a string, supporting "+inf" and "-inf".
pub fn f64_to_string(d: f64) -> Result<Value, Error> {
  if d.is_infinite() && d.is_sign_negative() {
    Ok(Value::from_static_str("-inf"))
  } else if d.is_infinite() {
    Ok(Value::from_static_str("+inf"))
  } else if d.is_nan() {
    Err(Error::new(
      ErrorKind::InvalidArgument,
      "Cannot convert NaN to redis value.",
    ))
  } else {
    Ok(d.to_string().into())
  }
}

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(len)
    .map(char::from)
    .collect()
}

pub fn read_bool_atomic(val: &AtomicBool) -> bool {
  val.load(Ordering::Acquire)
}

pub fn set_bool_atomic(val: &AtomicBool, new: bool) -> bool {
  val.swap(new, Ordering::SeqCst)
}

pub fn decr_atomic(size: &AtomicUsize) -> usize {
  size.fetch_sub(1, Ordering::AcqRel).saturating_sub(1)
}

pub fn incr_atomic(size: &AtomicUsize) -> usize {
  size.fetch_add(1, Ordering::AcqRel).saturating_add(1)
}

pub fn read_atomic(size: &AtomicUsize) -> usize {
  size.load(Ordering::Acquire)
}

pub fn set_atomic(size: &AtomicUsize, val: usize) -> usize {
  size.swap(val, Ordering::SeqCst)
}

pub fn read_locked<T: Clone>(locked: &RwLock<T>) -> T {
  locked.read().clone()
}

#[cfg(feature = "transactions")]
pub fn read_mutex<T: Clone>(locked: &Mutex<T>) -> T {
  locked.lock().clone()
}

pub fn swap_new_broadcast_channel<T: Clone>(old: &RefSwap<RefCount<BroadcastSender<T>>>, capacity: usize) {
  let new = broadcast_channel(capacity).0;
  old.swap(RefCount::new(new));
}

pub fn url_uses_tls(url: &Url) -> bool {
  let scheme = url.scheme();
  scheme.starts_with(REDIS_TLS_SCHEME) || scheme.starts_with(VALKEY_TLS_SCHEME)
}

pub fn url_is_clustered(url: &Url) -> bool {
  url.scheme().ends_with(CLUSTER_SCHEME_SUFFIX)
}

pub fn url_is_sentinel(url: &Url) -> bool {
  url.scheme().ends_with(SENTINEL_SCHEME_SUFFIX)
}

pub fn url_is_unix_socket(url: &Url) -> bool {
  url.scheme().ends_with(UNIX_SCHEME_SUFFIX)
}

pub fn parse_url(url: &str, default_port: Option<u16>) -> Result<(Url, String, u16, bool), Error> {
  let url = Url::parse(url)?;
  let host = if let Some(host) = url.host_str() {
    host.to_owned()
  } else {
    return Err(Error::new(ErrorKind::Config, "Invalid or missing host."));
  };
  let port = if let Some(port) = url.port().or(default_port) {
    port
  } else {
    return Err(Error::new(ErrorKind::Config, "Invalid or missing port."));
  };

  let tls = url_uses_tls(&url);
  if tls {
    check_tls_features();
  }

  Ok((url, host, port, tls))
}

#[cfg(any(
  feature = "enable-native-tls",
  feature = "enable-rustls",
  feature = "enable-rustls-ring"
))]
pub fn check_tls_features() {}

#[cfg(not(any(
  feature = "enable-native-tls",
  feature = "enable-rustls",
  feature = "enable-rustls-ring"
)))]
pub fn check_tls_features() {
  warn!("TLS features are not enabled, but a TLS feature may have been used.");
}

#[cfg(feature = "unix-sockets")]
pub fn parse_unix_url(url: &str) -> Result<(Url, PathBuf), Error> {
  let url = Url::parse(url)?;
  let path: PathBuf = url.path().into();
  Ok((url, path))
}

pub fn parse_url_db(url: &Url) -> Result<Option<u8>, Error> {
  let parts: Vec<&str> = if let Some(parts) = url.path_segments() {
    parts.collect()
  } else {
    return Ok(None);
  };

  if parts.len() > 1 {
    return Err(Error::new(ErrorKind::Config, "Invalid database path."));
  } else if parts.is_empty() {
    return Ok(None);
  }
  // handle empty paths with a / prefix
  if parts[0].trim() == "" {
    return Ok(None);
  }

  Ok(Some(parts[0].parse()?))
}

pub fn parse_url_credentials(url: &Url) -> Result<(Option<String>, Option<String>), Error> {
  let username = if url.username().is_empty() {
    None
  } else {
    let username = percent_decode(url.username())?;
    Some(username.into_owned())
  };
  let password = percent_decode(url.password().unwrap_or_default())?;
  let password = if password.is_empty() {
    None
  } else {
    Some(password.into_owned())
  };

  Ok((username, password))
}

pub fn parse_url_other_nodes(url: &Url) -> Result<Vec<Server>, Error> {
  let mut out = Vec::new();

  for (key, value) in url.query_pairs().into_iter() {
    if key == CLUSTER_NODE_QUERY {
      let parts: Vec<&str> = value.split(':').collect();
      if parts.len() != 2 {
        return Err(Error::new(
          ErrorKind::Config,
          format!("Invalid host:port for cluster node: {}", value),
        ));
      }

      let host = parts[0].to_owned();
      let port = parts[1].parse::<u16>()?;
      out.push(Server::new(host, port));
    }
  }

  Ok(out)
}

pub fn parse_url_sentinel_service_name(url: &Url) -> Result<String, Error> {
  for (key, value) in url.query_pairs().into_iter() {
    if key == SENTINEL_NAME_QUERY {
      return Ok(value.to_string());
    }
  }

  Err(Error::new(
    ErrorKind::Config,
    "Invalid or missing sentinel service name query parameter.",
  ))
}

#[cfg(feature = "sentinel-auth")]
pub fn parse_url_sentinel_username(url: &Url) -> Option<String> {
  url.query_pairs().find_map(|(key, value)| {
    if key == SENTINEL_USERNAME_QUERY {
      Some(value.to_string())
    } else {
      None
    }
  })
}

#[cfg(feature = "sentinel-auth")]
pub fn parse_url_sentinel_password(url: &Url) -> Option<String> {
  url.query_pairs().find_map(|(key, value)| {
    if key == SENTINEL_PASSWORD_QUERY {
      Some(value.to_string())
    } else {
      None
    }
  })
}

#[cfg(feature = "unix-sockets")]
pub fn path_to_string(path: &Path) -> String {
  path.as_os_str().to_string_lossy().to_string()
}

pub fn server_to_parts(server: &str) -> Result<(&str, u16), Error> {
  let parts: Vec<&str> = server.split(':').collect();
  if parts.len() < 2 {
    return Err(Error::new(ErrorKind::IO, "Invalid server."));
  }
  Ok((parts[0], parts[1].parse::<u16>()?))
}

pub fn incr_with_max(curr: u32, max: u32) -> Option<u32> {
  if max != 0 && curr >= max {
    None
  } else {
    Some(curr.saturating_add(1))
  }
}

pub fn add_jitter(delay: u64, jitter: u32) -> u64 {
  if jitter == 0 {
    delay
  } else {
    delay.saturating_add(rand::thread_rng().gen_range(0 .. jitter as u64))
  }
}

/// A generic TryInto wrapper to work with the Infallible error type in the blanket From implementation.
pub fn try_into<S, D>(val: S) -> Result<D, Error>
where
  S: TryInto<D>,
  S::Error: Into<Error>,
{
  val.try_into().map_err(|e| e.into())
}

/// Attempt to convert an iterator of values into an array.
pub fn try_into_vec<S>(values: Vec<S>) -> Result<Vec<Value>, Error>
where
  S: TryInto<Value>,
  S::Error: Into<Error>,
{
  let mut out = Vec::with_capacity(values.len());
  for value in values.into_iter() {
    out.push(try_into(value)?);
  }

  Ok(out)
}

/// Attempt to convert an iterator of key/value pairs to a map.
pub fn into_map<I, K, V>(mut iter: I) -> Result<HashMap<Key, Value>, Error>
where
  I: Iterator<Item = (K, V)>,
  K: TryInto<Key>,
  K::Error: Into<Error>,
  V: TryInto<Value>,
  V::Error: Into<Error>,
{
  let (lower, upper) = iter.size_hint();
  let capacity = if let Some(upper) = upper { upper } else { lower };
  let mut out = HashMap::with_capacity(capacity);

  while let Some((key, value)) = iter.next() {
    out.insert(to!(key)?, to!(value)?);
  }
  Ok(out)
}

/// Whether the values may be a map encoded as key/value pairs.
pub fn is_maybe_array_map(arr: &[Value]) -> bool {
  if !arr.is_empty() && arr.len() % 2 == 0 {
    arr.chunks(2).all(|chunk| !chunk[0].is_aggregate_type())
  } else {
    false
  }
}

/// Flatten a nested array of values into one array.
pub fn flatten_value(value: Value) -> Value {
  if let Value::Array(values) = value {
    let mut out = Vec::with_capacity(values.len());
    for value in values.into_iter() {
      let flattened = flatten_value(value);
      if let Value::Array(flattened) = flattened {
        out.extend(flattened);
      } else {
        out.push(flattened);
      }
    }

    Value::Array(out)
  } else {
    value
  }
}

/// Attempt to flatten nested arrays to the provided depth.
pub fn flatten_nested_array_values(value: Value, depth: usize) -> Value {
  if depth == 0 {
    return value;
  }

  match value {
    Value::Array(values) => {
      let inner_size = values.iter().fold(0, |s, v| s + v.array_len().unwrap_or(1));
      let mut out = Vec::with_capacity(inner_size);

      for value in values.into_iter() {
        match value {
          Value::Array(inner) => {
            for value in inner.into_iter() {
              out.push(flatten_nested_array_values(value, depth - 1));
            }
          },
          _ => out.push(value),
        }
      }
      Value::Array(out)
    },
    Value::Map(values) => {
      let mut out = HashMap::with_capacity(values.len());

      for (key, value) in values.inner().into_iter() {
        let value = if value.is_array() {
          flatten_nested_array_values(value, depth - 1)
        } else {
          value
        };

        out.insert(key, value);
      }
      Value::Map(Map { inner: out })
    },
    _ => value,
  }
}

/// Convert a redis value to an array of (value, score) tuples.
pub fn value_to_zset_result(value: Value) -> Result<Vec<(Value, f64)>, Error> {
  let value = flatten_value(value);

  if let Value::Array(mut values) = value {
    if values.is_empty() {
      return Ok(Vec::new());
    }
    if values.len() % 2 != 0 {
      return Err(Error::new(
        ErrorKind::Unknown,
        "Expected an even number of redis values.",
      ));
    }

    let mut out = Vec::with_capacity(values.len() / 2);
    while values.len() >= 2 {
      let score = match values.pop().unwrap().as_f64() {
        Some(f) => f,
        None => {
          return Err(Error::new(
            ErrorKind::Protocol,
            "Could not convert value to floating point number.",
          ))
        },
      };
      let value = values.pop().unwrap();

      out.push((value, score));
    }

    Ok(out)
  } else {
    Err(Error::new(ErrorKind::Unknown, "Expected array of redis values."))
  }
}

#[cfg(all(
  feature = "enable-native-tls",
  not(any(feature = "enable-rustls", feature = "enable-rustls-ring"))
))]
pub fn tls_config_from_url(tls: bool) -> Result<Option<TlsConfig>, Error> {
  if tls {
    TlsConnector::default_native_tls().map(|c| Some(c.into()))
  } else {
    Ok(None)
  }
}

#[cfg(all(
  any(feature = "enable-rustls", feature = "enable-rustls-ring"),
  not(feature = "enable-native-tls")
))]
pub fn tls_config_from_url(tls: bool) -> Result<Option<TlsConfig>, Error> {
  if tls {
    TlsConnector::default_rustls().map(|c| Some(c.into()))
  } else {
    Ok(None)
  }
}

#[cfg(all(
  feature = "enable-native-tls",
  any(feature = "enable-rustls", feature = "enable-rustls-ring")
))]
pub fn tls_config_from_url(tls: bool) -> Result<Option<TlsConfig>, Error> {
  // default to native-tls when both are enabled
  if tls {
    TlsConnector::default_native_tls().map(|c| Some(c.into()))
  } else {
    Ok(None)
  }
}
