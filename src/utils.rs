use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::ClientLike,
  modules::inner::{CommandSender, RedisClientInner},
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  types::*,
};
use arc_swap::ArcSwap;
use bytes::Bytes;
use bytes_utils::Str;
use float_cmp::approx_eq;
use futures::{
  future::{select, Either},
  pin_mut,
  Future,
  TryFutureExt,
};
use parking_lot::RwLock;
use rand::{self, distributions::Alphanumeric, Rng};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::{
  collections::HashMap,
  convert::TryInto,
  f64,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};
use tokio::{
  sync::{
    broadcast::{channel as broadcast_channel, Sender as BroadcastSender},
    oneshot::channel as oneshot_channel,
  },
  time::sleep,
};
use url::Url;
use urlencoding::decode as percent_decode;

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::protocol::tls::{TlsConfig, TlsConnector};
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace;
#[cfg(feature = "transactions")]
use parking_lot::Mutex;
#[cfg(feature = "transactions")]
use std::mem;
#[cfg(feature = "unix-sockets")]
use std::path::Path;
use tokio::sync::mpsc::unbounded_channel;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use tracing_futures::Instrument;

const REDIS_TLS_SCHEME: &str = "rediss";
const REDIS_CLUSTER_SCHEME_SUFFIX: &str = "-cluster";
const REDIS_SENTINEL_SCHEME_SUFFIX: &str = "-sentinel";
const SENTINEL_NAME_QUERY: &str = "sentinelServiceName";
const CLUSTER_NODE_QUERY: &str = "node";
#[cfg(feature = "sentinel-auth")]
const SENTINEL_USERNAME_QUERY: &'static str = "sentinelUsername";
#[cfg(feature = "sentinel-auth")]
const SENTINEL_PASSWORD_QUERY: &'static str = "sentinelPassword";

/// Create a `Str` from a static str slice without copying.
pub fn static_str(s: &'static str) -> Str {
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

pub fn f64_opt_eq(lhs: &Option<f64>, rhs: &Option<f64>) -> bool {
  match *lhs {
    Some(lhs) => match *rhs {
      Some(rhs) => f64_eq(lhs, rhs),
      None => false,
    },
    None => rhs.is_none(),
  }
}

/// Convert a redis string to an `f64`, supporting "+inf" and "-inf".
pub fn redis_string_to_f64(s: &str) -> Result<f64, RedisError> {
  // this is changing in newer versions of redis to lose the "+" prefix
  if s == "+inf" || s == "inf" {
    Ok(f64::INFINITY)
  } else if s == "-inf" {
    Ok(f64::NEG_INFINITY)
  } else {
    s.parse::<f64>().map_err(|_| {
      RedisError::new(
        RedisErrorKind::Unknown,
        format!("Could not convert {} to floating point value.", s),
      )
    })
  }
}

/// Convert an `f64` to a redis string, supporting "+inf" and "-inf".
pub fn f64_to_redis_string(d: f64) -> Result<RedisValue, RedisError> {
  if d.is_infinite() && d.is_sign_negative() {
    Ok(RedisValue::from_static_str("-inf"))
  } else if d.is_infinite() {
    Ok(RedisValue::from_static_str("+inf"))
  } else if d.is_nan() {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Cannot convert NaN to redis value.",
    ))
  } else {
    Ok(d.to_string().into())
  }
}

pub fn f64_to_zrange_bound(d: f64, kind: &ZRangeKind) -> Result<String, RedisError> {
  if d.is_infinite() && d.is_sign_negative() {
    Ok("-inf".into())
  } else if d.is_infinite() {
    Ok("+inf".into())
  } else if d.is_nan() {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Cannot convert NaN to redis value.",
    ))
  } else {
    Ok(match kind {
      ZRangeKind::Inclusive => d.to_string(),
      ZRangeKind::Exclusive => format!("({}", d),
    })
  }
}

pub fn incr_with_max(curr: u32, max: u32) -> Option<u32> {
  if max != 0 && curr >= max {
    None
  } else {
    Some(curr.saturating_add(1))
  }
}

pub fn random_string(len: usize) -> String {
  rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(len)
    .map(char::from)
    .collect()
}

pub fn convert_or_default<R>(value: RedisValue) -> R
where
  R: FromRedis + Default,
{
  value.convert().ok().unwrap_or_default()
}

#[cfg(feature = "transactions")]
pub fn random_u64(max: u64) -> u64 {
  rand::thread_rng().gen_range(0 .. max)
}

pub fn set_client_state(state: &RwLock<ClientState>, new_state: ClientState) {
  let mut state_guard = state.write();
  *state_guard = new_state;
}

pub fn check_and_set_client_state(
  state: &RwLock<ClientState>,
  expected: ClientState,
  new_state: ClientState,
) -> bool {
  let mut state_guard = state.write();

  if *state_guard != expected {
    false
  } else {
    *state_guard = new_state;
    true
  }
}

pub fn read_bool_atomic(val: &Arc<AtomicBool>) -> bool {
  val.load(Ordering::Acquire)
}

pub fn set_bool_atomic(val: &Arc<AtomicBool>, new: bool) -> bool {
  val.swap(new, Ordering::SeqCst)
}

pub fn decr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_sub(1, Ordering::AcqRel).saturating_sub(1)
}

pub fn incr_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.fetch_add(1, Ordering::AcqRel).saturating_add(1)
}

pub fn read_atomic(size: &Arc<AtomicUsize>) -> usize {
  size.load(Ordering::Acquire)
}

pub fn set_atomic(size: &Arc<AtomicUsize>, val: usize) -> usize {
  size.swap(val, Ordering::SeqCst)
}

pub fn read_locked<T: Clone>(locked: &RwLock<T>) -> T {
  locked.read().clone()
}

#[cfg(feature = "transactions")]
pub fn read_mutex<T: Clone>(locked: &Mutex<T>) -> T {
  locked.lock().clone()
}

#[cfg(feature = "transactions")]
pub fn set_mutex<T>(locked: &Mutex<T>, value: T) -> T {
  mem::replace(&mut *locked.lock(), value)
}

#[cfg(feature = "unix-sockets")]
pub fn path_to_string(path: &Path) -> String {
  path.as_os_str().to_string_lossy().to_string()
}

pub fn check_lex_str(val: String, kind: &ZRangeKind) -> String {
  let formatted = val.starts_with('(') || val.starts_with('[') || val == "+" || val == "-";

  if formatted {
    val
  } else if *kind == ZRangeKind::Exclusive {
    format!("({}", val)
  } else {
    format!("[{}", val)
  }
}

/// Parse the response from `FUNCTION LIST`.
fn parse_functions(value: &RedisValue) -> Result<Vec<Function>, RedisError> {
  if let RedisValue::Array(functions) = value {
    let mut out = Vec::with_capacity(functions.len());
    for function_block in functions.iter() {
      let functions: HashMap<Str, RedisValue> = function_block.clone().convert()?;
      let name = match functions.get("name").and_then(|n| n.as_bytes_str()) {
        Some(name) => name,
        None => return Err(RedisError::new_parse("Missing function name.")),
      };
      let flags: Vec<FunctionFlag> = functions
        .get("flags")
        .and_then(|f| {
          f.clone()
            .into_array()
            .into_iter()
            .map(|v| FunctionFlag::from_str(v.as_str().unwrap_or_default().as_ref()))
            .collect()
        })
        .unwrap_or_default();

      out.push(Function { name, flags })
    }

    Ok(out)
  } else {
    Err(RedisError::new_parse("Invalid functions block."))
  }
}

/// Check and parse the response to `FUNCTION LIST`.
pub fn value_to_functions(value: &RedisValue, name: &str) -> Result<Vec<Function>, RedisError> {
  if let RedisValue::Array(ref libraries) = value {
    for library in libraries.iter() {
      let properties: HashMap<Str, RedisValue> = library.clone().convert()?;
      let should_parse = properties
        .get("library_name")
        .and_then(|v| v.as_str())
        .map(|s| s == name)
        .unwrap_or(false);

      if should_parse {
        if let Some(functions) = properties.get("functions") {
          return parse_functions(functions);
        }
      }
    }

    Err(RedisError::new_parse(format!("Missing library '{}'", name)))
  } else {
    Err(RedisError::new_parse("Expected array."))
  }
}

pub async fn apply_timeout<T, Fut, E>(ft: Fut, timeout: Duration) -> Result<T, RedisError>
where
  E: Into<RedisError>,
  Fut: Future<Output = Result<T, E>>,
{
  if !timeout.is_zero() {
    let sleep_ft = sleep(timeout);
    pin_mut!(sleep_ft);
    pin_mut!(ft);

    trace!("Using timeout: {:?}", timeout);
    match select(ft, sleep_ft).await {
      Either::Left((lhs, _)) => lhs.map_err(|e| e.into()),
      Either::Right((_, _)) => Err(RedisError::new(RedisErrorKind::Timeout, "Request timed out.")),
    }
  } else {
    ft.await.map_err(|e| e.into())
  }
}

/// Disconnect any state shared with the last router task spawned by the client.
pub fn reset_router_task(inner: &Arc<RedisClientInner>) {
  let _guard = inner._lock.lock();

  if !inner.has_command_rx() {
    _trace!(inner, "Resetting command channel before connecting.");
    // another connection task is running. this will let the command channel drain, then it'll drop everything on
    // the old connection/router interface.
    let (tx, rx) = unbounded_channel();
    let old_command_tx = inner.swap_command_tx(tx);
    inner.store_command_rx(rx, true);
    close_router_channel(inner, old_command_tx);
  }
}

/// Whether the router should check and interrupt the blocked command.
async fn should_enforce_blocking_policy(inner: &Arc<RedisClientInner>, command: &RedisCommand) -> bool {
  if command.kind.closes_connection() {
    return false;
  }
  if matches!(inner.config.blocking, Blocking::Error | Blocking::Interrupt) {
    inner.backchannel.read().await.is_blocked()
  } else {
    false
  }
}

/// Interrupt the currently blocked connection (if found) with the provided flag.
pub async fn interrupt_blocked_connection(
  inner: &Arc<RedisClientInner>,
  flag: ClientUnblockFlag,
) -> Result<(), RedisError> {
  let connection_id = {
    let backchannel = inner.backchannel.read().await;
    let server = match backchannel.blocked_server() {
      Some(server) => server,
      None => return Err(RedisError::new(RedisErrorKind::Unknown, "Connection is not blocked.")),
    };
    let id = match backchannel.connection_id(&server) {
      Some(id) => id,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::Unknown,
          "Failed to read connection ID.",
        ))
      },
    };

    _debug!(inner, "Sending CLIENT UNBLOCK to {}, ID: {}", server, id);
    id
  };

  let mut args = Vec::with_capacity(2);
  args.push(connection_id.into());
  args.push(flag.to_str().into());
  let command = RedisCommand::new(RedisCommandKind::ClientUnblock, args);

  let frame = backchannel_request_response(inner, command, true).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
}

/// Check the status of the connection (usually before sending a command) to determine whether the connection should
/// be unblocked automatically.
async fn check_blocking_policy(inner: &Arc<RedisClientInner>, command: &RedisCommand) -> Result<(), RedisError> {
  if should_enforce_blocking_policy(inner, command).await {
    _debug!(
      inner,
      "Checking to enforce blocking policy for {}",
      command.kind.to_str_debug()
    );

    if inner.config.blocking == Blocking::Error {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Error sending command while connection is blocked.",
      ));
    } else if inner.config.blocking == Blocking::Interrupt {
      if let Err(e) = interrupt_blocked_connection(inner, ClientUnblockFlag::Error).await {
        _error!(inner, "Failed to interrupt blocked connection: {:?}", e);
      }
    }
  }

  Ok(())
}

/// Prepare the command options, returning the timeout duration to apply.
pub fn prepare_command<C: ClientLike>(client: &C, command: &mut RedisCommand) -> Duration {
  client.change_command(command);
  command.inherit_options(client.inner());
  command
    .timeout_dur
    .unwrap_or_else(|| client.inner().default_command_timeout())
}

/// Send a command to the server using the default response handler.
pub async fn basic_request_response<C, F, R>(client: &C, func: F) -> Result<Resp3Frame, RedisError>
where
  C: ClientLike,
  R: Into<RedisCommand>,
  F: FnOnce() -> Result<R, RedisError>,
{
  let inner = client.inner();
  let mut command: RedisCommand = func()?.into();
  let (tx, rx) = oneshot_channel();
  command.response = ResponseKind::Respond(Some(tx));

  let timed_out = command.timed_out.clone();
  let timeout_dur = prepare_command(client, &mut command);
  check_blocking_policy(inner, &command).await?;
  client.send_command(command)?;

  apply_timeout(rx, timeout_dur)
    .and_then(|r| async { r })
    .map_err(move |error| {
      set_bool_atomic(&timed_out, true);
      error
    })
    .await
}

/// Send a command to the server, with tracing.
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
pub async fn request_response<C, F, R>(client: &C, func: F) -> Result<Resp3Frame, RedisError>
where
  C: ClientLike,
  R: Into<RedisCommand>,
  F: FnOnce() -> Result<R, RedisError>,
{
  let inner = client.inner();
  if !inner.should_trace() {
    return basic_request_response(client, func).await;
  }

  let cmd_span = trace::create_command_span(inner);
  let end_cmd_span = cmd_span.clone();

  let (mut command, rx, req_size) = {
    let args_span = trace::create_args_span(cmd_span.id(), inner);
    let _enter = args_span.enter();
    let (tx, rx) = oneshot_channel();

    let mut command: RedisCommand = func()?.into();
    command.response = ResponseKind::Respond(Some(tx));

    let req_size = protocol_utils::args_size(&command.args());
    args_span.record("num_args", &command.args().len());
    (command, rx, req_size)
  };
  cmd_span.record("cmd", &command.kind.to_str_debug());
  cmd_span.record("req_size", &req_size);

  let queued_span = trace::create_queued_span(cmd_span.id(), inner);
  let timed_out = command.timed_out.clone();
  _trace!(
    inner,
    "Setting command trace ID: {:?} for {} ({})",
    cmd_span.id(),
    command.kind.to_str_debug(),
    command.debug_id()
  );
  command.traces.cmd = Some(cmd_span.clone());
  command.traces.queued = Some(queued_span);

  let timeout_dur = prepare_command(client, &mut command);
  let _ = check_blocking_policy(inner, &command).await?;
  let _ = client.send_command(command)?;

  apply_timeout(rx, timeout_dur)
    .and_then(|r| async { r })
    .map_err(move |error| {
      set_bool_atomic(&timed_out, true);
      error
    })
    .and_then(|frame| async move {
      trace::record_response_size(&end_cmd_span, &frame);
      Ok::<_, RedisError>(frame)
    })
    .instrument(cmd_span)
    .await
}

#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
pub async fn request_response<C, F, R>(client: &C, func: F) -> Result<Resp3Frame, RedisError>
where
  C: ClientLike,
  R: Into<RedisCommand>,
  F: FnOnce() -> Result<R, RedisError>,
{
  basic_request_response(client, func).await
}

/// Send a command on the backchannel connection.
///
/// A new connection may be created.
pub async fn backchannel_request_response(
  inner: &Arc<RedisClientInner>,
  command: RedisCommand,
  use_blocked: bool,
) -> Result<Resp3Frame, RedisError> {
  let mut backchannel = inner.backchannel.write().await;
  let server = backchannel.find_server(inner, &command, use_blocked)?;
  backchannel.request_response(inner, &server, command).await
}

pub fn check_empty_keys(keys: &MultipleKeys) -> Result<(), RedisError> {
  if keys.len() == 0 {
    Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "At least one key is required.",
    ))
  } else {
    Ok(())
  }
}

/// Check for a scan pattern without a hash tag, or with a wildcard in the hash tag.
///
/// These patterns will result in scanning a random node if used against a clustered redis.
pub fn clustered_scan_pattern_has_hash_tag(inner: &Arc<RedisClientInner>, pattern: &str) -> bool {
  let (mut i, mut j, mut has_wildcard) = (None, None, false);
  for (idx, c) in pattern.chars().enumerate() {
    if c == '{' && i.is_none() {
      i = Some(idx);
    }
    if c == '}' && j.is_none() && i.is_some() {
      j = Some(idx);
      break;
    }
    if c == '*' && i.is_some() {
      has_wildcard = true;
    }
  }

  if i.is_none() || j.is_none() {
    return false;
  }

  if has_wildcard {
    _warn!(
      inner,
      "Found wildcard in scan pattern hash tag. You may not be scanning the correct node."
    );
  }

  true
}

/// A generic TryInto wrapper to work with the Infallible error type in the blanket From implementation.
pub fn try_into<S, D>(val: S) -> Result<D, RedisError>
where
  S: TryInto<D>,
  S::Error: Into<RedisError>,
{
  val.try_into().map_err(|e| e.into())
}

pub fn try_into_vec<S>(values: Vec<S>) -> Result<Vec<RedisValue>, RedisError>
where
  S: TryInto<RedisValue>,
  S::Error: Into<RedisError>,
{
  let mut out = Vec::with_capacity(values.len());
  for value in values.into_iter() {
    out.push(try_into(value)?);
  }

  Ok(out)
}

pub fn add_jitter(delay: u64, jitter: u32) -> u64 {
  if jitter == 0 {
    delay
  } else {
    delay.saturating_add(rand::thread_rng().gen_range(0 .. jitter as u64))
  }
}

pub fn into_redis_map<I, K, V>(mut iter: I) -> Result<HashMap<RedisKey, RedisValue>, RedisError>
where
  I: Iterator<Item = (K, V)>,
  K: TryInto<RedisKey>,
  K::Error: Into<RedisError>,
  V: TryInto<RedisValue>,
  V::Error: Into<RedisError>,
{
  let (lower, upper) = iter.size_hint();
  let capacity = if let Some(upper) = upper { upper } else { lower };
  let mut out = HashMap::with_capacity(capacity);

  while let Some((key, value)) = iter.next() {
    out.insert(to!(key)?, to!(value)?);
  }
  Ok(out)
}

pub fn flatten_nested_array_values(value: RedisValue, depth: usize) -> RedisValue {
  if depth == 0 {
    return value;
  }

  match value {
    RedisValue::Array(values) => {
      let inner_size = values.iter().fold(0, |s, v| s + v.array_len().unwrap_or(1));
      let mut out = Vec::with_capacity(inner_size);

      for value in values.into_iter() {
        match value {
          RedisValue::Array(inner) => {
            for value in inner.into_iter() {
              out.push(flatten_nested_array_values(value, depth - 1));
            }
          },
          _ => out.push(value),
        }
      }
      RedisValue::Array(out)
    },
    RedisValue::Map(values) => {
      let mut out = HashMap::with_capacity(values.len());

      for (key, value) in values.inner().into_iter() {
        let value = if value.is_array() {
          flatten_nested_array_values(value, depth - 1)
        } else {
          value
        };

        out.insert(key, value);
      }
      RedisValue::Map(RedisMap { inner: out })
    },
    _ => value,
  }
}

pub fn is_maybe_array_map(arr: &[RedisValue]) -> bool {
  if !arr.is_empty() && arr.len() % 2 == 0 {
    arr.chunks(2).all(|chunk| !chunk[0].is_aggregate_type())
  } else {
    false
  }
}

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
pub fn check_tls_features() {}

#[cfg(not(any(feature = "enable-native-tls", feature = "enable-rustls")))]
pub fn check_tls_features() {
  warn!("TLS features are not enabled, but a TLS feature may have been used.");
}

#[cfg(all(feature = "enable-native-tls", not(feature = "enable-rustls")))]
pub fn tls_config_from_url(tls: bool) -> Result<Option<TlsConfig>, RedisError> {
  if tls {
    TlsConnector::default_native_tls().map(|c| Some(c.into()))
  } else {
    Ok(None)
  }
}

#[cfg(all(feature = "enable-rustls", not(feature = "enable-native-tls")))]
pub fn tls_config_from_url(tls: bool) -> Result<Option<TlsConfig>, RedisError> {
  if tls {
    TlsConnector::default_rustls().map(|c| Some(c.into()))
  } else {
    Ok(None)
  }
}

#[cfg(all(feature = "enable-rustls", feature = "enable-native-tls"))]
pub fn tls_config_from_url(tls: bool) -> Result<Option<TlsConfig>, RedisError> {
  // default to native-tls when both are enabled
  if tls {
    TlsConnector::default_native_tls().map(|c| Some(c.into()))
  } else {
    Ok(None)
  }
}

pub fn swap_new_broadcast_channel<T: Clone>(old: &ArcSwap<BroadcastSender<T>>, capacity: usize) {
  let new = broadcast_channel(capacity).0;
  old.swap(Arc::new(new));
}

pub fn url_uses_tls(url: &Url) -> bool {
  url.scheme().starts_with(REDIS_TLS_SCHEME)
}

pub fn url_is_clustered(url: &Url) -> bool {
  url.scheme().ends_with(REDIS_CLUSTER_SCHEME_SUFFIX)
}

pub fn url_is_sentinel(url: &Url) -> bool {
  url.scheme().ends_with(REDIS_SENTINEL_SCHEME_SUFFIX)
}

pub fn parse_url(url: &str, default_port: Option<u16>) -> Result<(Url, String, u16, bool), RedisError> {
  let url = Url::parse(url)?;
  let host = if let Some(host) = url.host_str() {
    host.to_owned()
  } else {
    return Err(RedisError::new(RedisErrorKind::Config, "Invalid or missing host."));
  };
  let port = if let Some(port) = url.port().or(default_port) {
    port
  } else {
    return Err(RedisError::new(RedisErrorKind::Config, "Invalid or missing port."));
  };

  let tls = url_uses_tls(&url);
  if tls {
    check_tls_features();
  }

  Ok((url, host, port, tls))
}

pub fn parse_url_db(url: &Url) -> Result<Option<u8>, RedisError> {
  let parts: Vec<&str> = if let Some(parts) = url.path_segments() {
    parts.collect()
  } else {
    return Ok(None);
  };

  if parts.len() > 1 {
    return Err(RedisError::new(RedisErrorKind::Config, "Invalid database path."));
  } else if parts.is_empty() {
    return Ok(None);
  }
  // handle empty paths with a / prefix
  if parts[0].trim() == "" {
    return Ok(None);
  }

  Ok(Some(parts[0].parse()?))
}

pub fn parse_url_credentials(url: &Url) -> Result<(Option<String>, Option<String>), RedisError> {
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

pub fn parse_url_other_nodes(url: &Url) -> Result<Vec<Server>, RedisError> {
  let mut out = Vec::new();

  for (key, value) in url.query_pairs().into_iter() {
    if key == CLUSTER_NODE_QUERY {
      let parts: Vec<&str> = value.split(':').collect();
      if parts.len() != 2 {
        return Err(RedisError::new(
          RedisErrorKind::Config,
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

pub fn parse_url_sentinel_service_name(url: &Url) -> Result<String, RedisError> {
  for (key, value) in url.query_pairs().into_iter() {
    if key == SENTINEL_NAME_QUERY {
      return Ok(value.to_string());
    }
  }

  Err(RedisError::new(
    RedisErrorKind::Config,
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

pub async fn clear_backchannel_state(inner: &Arc<RedisClientInner>) {
  inner.backchannel.write().await.clear_router_state(inner).await;
}

/// Send QUIT to the servers and clean up the old router task's state.
fn close_router_channel(inner: &Arc<RedisClientInner>, command_tx: Arc<CommandSender>) {
  inner.notifications.broadcast_close();
  inner.reset_server_state();

  let command = RedisCommand::new(RedisCommandKind::Quit, vec![]);
  inner.counters.incr_cmd_buffer_len();
  if let Err(_) = command_tx.send(command.into()) {
    inner.counters.decr_cmd_buffer_len();
    _warn!(inner, "Failed to send QUIT when dropping old command channel.");
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{error::RedisError, types::RedisValue};
  use std::{convert::TryInto, fmt::Debug};

  fn m<V>(v: V) -> RedisValue
  where
    V: TryInto<RedisValue> + Debug,
    V::Error: Into<RedisError> + Debug,
  {
    v.try_into().unwrap()
  }

  fn a(v: Vec<RedisValue>) -> RedisValue {
    RedisValue::Array(v)
  }

  #[test]
  fn should_not_panic_with_zero_jitter() {
    assert_eq!(add_jitter(10, 0), 10);
  }

  #[test]
  fn should_flatten_xread_example() {
    // 127.0.0.1:6379> xread count 2 streams foo bar 1643479648480-0 1643479834990-0
    // 1) 1) "foo"
    //    2) 1) 1) "1643479650336-0"
    //          2) 1) "count"
    //             2) "3"
    // 2) 1) "bar"
    //    2) 1) 1) "1643479837746-0"
    //          2) 1) "count"
    //             2) "5"
    //       2) 1) "1643479925582-0"
    //          2) 1) "count"
    //             2) "6"
    let actual: RedisValue = vec![
      a(vec![
        m("foo"),
        a(vec![a(vec![m("1643479650336-0"), a(vec![m("count"), m(3)])])]),
      ]),
      a(vec![
        m("bar"),
        a(vec![
          a(vec![m("1643479837746-0"), a(vec![m("count"), m(5)])]),
          a(vec![m("1643479925582-0"), a(vec![m("count"), m(6)])]),
        ]),
      ]),
    ]
    .into_iter()
    .collect();

    // flatten the top level nested array into something that can be cast to a map
    let expected: RedisValue = vec![
      m("foo"),
      a(vec![a(vec![m("1643479650336-0"), a(vec![m("count"), m(3)])])]),
      m("bar"),
      a(vec![
        a(vec![m("1643479837746-0"), a(vec![m("count"), m(5)])]),
        a(vec![m("1643479925582-0"), a(vec![m("count"), m(6)])]),
      ]),
    ]
    .into_iter()
    .collect();

    assert_eq!(flatten_nested_array_values(actual, 1), expected);
  }

  #[test]
  fn should_parse_url_credentials_no_creds() {
    let url = Url::parse("redis://localhost:6379").unwrap();
    let (username, password) = parse_url_credentials(&url).unwrap();

    assert_eq!(username, None);
    assert_eq!(password, None);
  }

  #[test]
  fn should_parse_url_credentials_with_creds() {
    let url = Url::parse("redis://default:abc123@localhost:6379").unwrap();
    let (username, password) = parse_url_credentials(&url).unwrap();

    assert_eq!(username.unwrap(), "default");
    assert_eq!(password.unwrap(), "abc123");
  }

  #[test]
  fn should_parse_url_credentials_with_percent_encoded_creds() {
    let url = Url::parse("redis://default:abc%2F123@localhost:6379").unwrap();
    let (username, password) = parse_url_credentials(&url).unwrap();

    assert_eq!(username.unwrap(), "default");
    assert_eq!(password.unwrap(), "abc/123");
  }
}
