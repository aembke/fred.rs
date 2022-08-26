use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::utils as multiplexer_utils;
use crate::multiplexer::{sentinel, ConnectionIDs};
use crate::protocol::types::{RedisCommand, RedisCommandKind};
use crate::types::*;
use arcstr::ArcStr;
use bytes::Bytes;
use bytes_utils::Str;
use float_cmp::approx_eq;
use futures::future::{select, Either};
use futures::{pin_mut, Future};
use parking_lot::{Mutex, RwLock};
use rand::distributions::Alphanumeric;
use rand::{self, Rng};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{f64, mem};
use tokio::sync::oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver};
use tokio::sync::RwLock as AsyncRwLock;
use tokio::time::sleep;
use url::Url;

use crate::interfaces::ClientLike;
use crate::protocol::command::RedisCommand;
use crate::protocol::responders::ResponseKind;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::protocol::utils as protocol_utils;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use futures::TryFutureExt;
#[cfg(feature = "serde-json")]
use serde_json::Value;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use tracing_futures::Instrument;

const REDIS_TLS_SCHEME: &'static str = "rediss";
const REDIS_CLUSTER_SCHEME_SUFFIX: &'static str = "-cluster";
const REDIS_SENTINEL_SCHEME_SUFFIX: &'static str = "-sentinel";
const SENTINEL_NAME_QUERY: &'static str = "sentinelServiceName";
const CLUSTER_NODE_QUERY: &'static str = "node";
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

pub fn is_clustered(config: &RwLock<RedisConfig>) -> bool {
  config.read().server.is_clustered()
}

pub fn is_sentinel(config: &RwLock<RedisConfig>) -> bool {
  config.read().server.is_sentinel()
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

pub fn random_u64(max: u64) -> u64 {
  rand::thread_rng().gen_range(0..max)
}

pub fn pattern_pubsub_counts(result: Vec<RedisValue>) -> Result<Vec<usize>, RedisError> {
  let mut out = Vec::with_capacity(result.len() / 3);

  if result.len() > 0 {
    let mut idx = 2;
    while idx < result.len() {
      out.push(match result[idx] {
        RedisValue::Integer(ref i) => {
          if *i < 0 {
            return Err(RedisError::new(
              RedisErrorKind::Unknown,
              "Invalid pattern pubsub channel count response.",
            ));
          } else {
            *i as usize
          }
        },
        _ => {
          return Err(RedisError::new(
            RedisErrorKind::Unknown,
            "Invalid pattern pubsub response.",
          ))
        },
      });

      idx += 3;
    }
  }

  Ok(out)
}

pub fn set_client_state(state: &RwLock<ClientState>, new_state: ClientState) {
  let mut state_guard = state.write();
  *state_guard = new_state;
}

pub fn read_client_state(state: &RwLock<ClientState>) -> ClientState {
  state.read().clone()
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

/// Check and set the inner locked value by updating `locked` with `new_value`, returning the old value.
pub fn check_and_set_bool(locked: &RwLock<bool>, new_value: bool) -> bool {
  let mut guard = locked.write();
  let old_value = *guard;
  *guard = new_value;
  old_value
}

pub fn read_centralized_server(inner: &Arc<RedisClientInner>) -> Option<Arc<String>> {
  match inner.config.read().server {
    ServerConfig::Centralized { ref host, ref port } => Some(Arc::new(format!("{}:{}", host, port))),
    ServerConfig::Sentinel { .. } => inner.sentinel_primary.read().clone(),
    _ => None,
  }
}

pub fn read_bool_atomic(val: &Arc<AtomicBool>) -> bool {
  val.load(Ordering::Acquire)
}

pub fn set_bool_atomic(val: &Arc<AtomicBool>, new: bool) -> bool {
  val.swap(new, Ordering::SeqCst)
}

pub fn cas_bool_atomic(val: &Arc<AtomicBool>, current: bool, new: bool) -> Result<bool, bool> {
  val.compare_exchange(current, new, Ordering::SeqCst, Ordering::Acquire)
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

pub fn set_locked<T>(locked: &RwLock<T>, value: T) -> T {
  mem::replace(&mut *locked.write(), value)
}

pub async fn set_locked_async<T>(locked: &AsyncRwLock<T>, value: T) -> T {
  mem::replace(&mut *locked.write().await, value)
}

pub fn take_locked<T>(locked: &RwLock<Option<T>>) -> Option<T> {
  locked.write().take()
}

pub fn read_locked<T: Clone>(locked: &RwLock<T>) -> T {
  locked.read().clone()
}

pub fn is_locked_some<T>(locked: &RwLock<Option<T>>) -> bool {
  locked.read().is_some()
}

pub fn read_mutex<T: Clone>(locked: &Mutex<T>) -> T {
  locked.lock().clone()
}

pub fn set_mutex<T>(locked: &Mutex<T>, value: T) -> T {
  mem::replace(&mut *locked.lock(), value)
}

pub fn take_mutex<T>(locked: &Mutex<Option<T>>) -> Option<T> {
  locked.lock().take()
}

pub fn is_mutex_some<T>(locked: &Mutex<Option<T>>) -> bool {
  locked.lock().is_some()
}

pub fn check_and_set_none<T>(locked: &RwLock<Option<T>>, value: T) -> bool {
  let mut guard = locked.write();

  if guard.is_some() {
    false
  } else {
    *guard = Some(value);
    true
  }
}

pub fn disallow_during_transaction(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  if is_locked_some(&inner.multi_block) {
    Err(RedisError::new(
      RedisErrorKind::InvalidCommand,
      "Cannot use command within transaction.",
    ))
  } else {
    Ok(())
  }
}

pub fn interrupt_reconnect_sleep(inner: &Arc<RedisClientInner>) {
  if let Some(jh) = inner.reconnect_sleep_jh.write().take() {
    jh.abort();
  }
}

/// Check whether the client has already sent the MULTI portion of a transaction, and if not return the hash slot describing the server to which it should be sent.
///
/// Sending the MULTI portion of a transaction is deferred on clustered clients because we dont know the server to which to send the command until the caller
/// sends the first command with a key. When we finally have a hash slot we send the MULTI portion first, and then we send the actual user command to start
/// the transaction.
pub fn should_send_multi_command(inner: &Arc<RedisClientInner>) -> Option<u16> {
  if is_clustered(&inner.config) {
    inner.multi_block.read().as_ref().and_then(|policy| {
      if !policy.sent_multi {
        policy.hash_slot.clone()
      } else {
        None
      }
    })
  } else {
    None
  }
}

/// Read the MULTI block hash slot, if known.
pub fn read_multi_hash_slot(inner: &Arc<RedisClientInner>) -> Option<u16> {
  if is_clustered(&inner.config) {
    inner
      .multi_block
      .read()
      .as_ref()
      .and_then(|policy| policy.hash_slot.clone())
  } else {
    None
  }
}

pub fn check_lex_str(val: String, kind: &ZRangeKind) -> String {
  let formatted = val.starts_with("(") || val.starts_with("[") || val == "+" || val == "-";

  if formatted {
    val
  } else {
    if *kind == ZRangeKind::Exclusive {
      format!("({}", val)
    } else {
      format!("[{}", val)
    }
  }
}

pub fn value_to_f64(value: &RedisValue) -> Result<f64, RedisError> {
  value.as_f64().ok_or(RedisError::new(
    RedisErrorKind::Unknown,
    "Could not parse value as float.",
  ))
}

pub fn value_to_geo_pos(value: &RedisValue) -> Result<Option<GeoPosition>, RedisError> {
  if let RedisValue::Array(value) = value {
    if value.len() == 2 {
      let longitude = value_to_f64(&value[0])?;
      let latitude = value_to_f64(&value[1])?;

      Ok(Some(GeoPosition { longitude, latitude }))
    } else {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Expected array with 2 elements.",
      ))
    }
  } else {
    Ok(None)
  }
}

pub fn update_multi_sent_flag(inner: &Arc<RedisClientInner>, value: bool) {
  if let Some(ref mut policy) = inner.multi_block.write().deref_mut() {
    policy.sent_multi = value;
  }
}

pub fn read_transaction_hash_slot(inner: &Arc<RedisClientInner>) -> Option<u16> {
  inner.multi_block.read().as_ref().and_then(|p| p.hash_slot.clone())
}

pub async fn wait_for_connect(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  if read_client_state(&inner.state) == ClientState::Connected {
    return Ok(());
  }

  let (tx, rx) = oneshot_channel();
  inner.connect_tx.write().push_back(tx);
  rx.await?
}

pub fn send_command(inner: &Arc<RedisClientInner>, command: RedisCommand) -> Result<(), RedisError> {
  incr_atomic(&inner.cmd_buffer_len);
  if let Err(mut e) = inner.command_tx.send(command) {
    decr_atomic(&inner.cmd_buffer_len);
    if let Some(tx) = e.0.tx.take() {
      if let Err(_) = tx.send(Err(RedisError::new(RedisErrorKind::Unknown, "Failed to send command."))) {
        _error!(inner, "Failed to send command to multiplexer {:?}.", e.0.kind);
      }
    }
  }

  Ok(())
}

pub async fn apply_timeout<T, Fut, E>(ft: Fut, timeout: u64) -> Result<T, RedisError>
where
  E: Into<RedisError>,
  Fut: Future<Output = Result<T, E>>,
{
  if timeout > 0 {
    let sleep_ft = sleep(Duration::from_millis(timeout));
    pin_mut!(sleep_ft);
    pin_mut!(ft);

    match select(ft, sleep_ft).await {
      Either::Left((lhs, _)) => lhs.map_err(|e| e.into()),
      Either::Right((_, _)) => Err(RedisError::new(RedisErrorKind::Timeout, "Request timed out.")),
    }
  } else {
    ft.await.map_err(|e| e.into())
  }
}

pub async fn wait_for_response(
  rx: OneshotReceiver<Result<Resp3Frame, RedisError>>,
  timeout: u64,
) -> Result<Resp3Frame, RedisError> {
  apply_timeout(rx, timeout).await?
}

fn has_blocking_error_policy(inner: &Arc<RedisClientInner>) -> bool {
  inner.config.read().blocking == Blocking::Error
}

fn has_blocking_interrupt_policy(inner: &Arc<RedisClientInner>) -> bool {
  inner.config.read().blocking == Blocking::Interrupt
}

async fn should_enforce_blocking_policy(inner: &Arc<RedisClientInner>, command: &RedisCommand) -> bool {
  !command.kind.closes_connection() && inner.backchannel.read().await.is_blocked()
}

pub async fn interrupt_blocked_connection(
  inner: &Arc<RedisClientInner>,
  flag: ClientUnblockFlag,
) -> Result<(), RedisError> {
  let blocked_server = match inner.backchannel.read().await.blocked.clone() {
    Some(server) => server,
    None => return Err(RedisError::new(RedisErrorKind::Unknown, "No blocked connection found.")),
  };
  let connection_id = match inner.backchannel.read().await.connection_id(&blocked_server) {
    Some(id) => id,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Failed to find blocked connection ID.",
      ))
    },
  };

  backchannel_request_response(inner, true, move || {
    Ok((
      RedisCommandKind::ClientUnblock,
      vec![connection_id.into(), flag.to_str().into()],
    ))
  })
  .await
  .map(|_| ())
}

async fn check_blocking_policy(inner: &Arc<RedisClientInner>, command: &RedisCommand) -> Result<(), RedisError> {
  if should_enforce_blocking_policy(inner, &command).await {
    _debug!(
      inner,
      "Checking to enforce blocking policy for {}",
      command.kind.to_str_debug()
    );

    if has_blocking_error_policy(inner) {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Error sending command while connection is blocked.",
      ));
    } else if has_blocking_interrupt_policy(inner) {
      if let Err(e) = interrupt_blocked_connection(inner, ClientUnblockFlag::Error).await {
        _error!(inner, "Failed to interrupt blocked connection: {:?}", e);
      }
    }
  }

  Ok(())
}

/// Send a command to the server using the default response handler.
pub async fn basic_request_response<C, F, R>(client: C, func: F) -> Result<Resp3Frame, RedisError>
where
  C: ClientLike,
  R: Into<RedisCommand>,
  F: FnOnce() -> Result<R, RedisError>,
{
  let inner = client.inner();
  let mut command: RedisCommand = func()?.into();
  let (tx, rx) = oneshot_channel();
  command.response = ResponseKind::Respond(Some(tx));

  let _ = check_blocking_policy(inner, &command).await?;
  let _ = disallow_nested_values(&command)?;
  let _ = client.send_command(command)?;

  wait_for_response(rx, inner.default_command_timeout()).await
}

/// Send a command to the server, with tracing.
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
pub async fn request_response<C, F, R>(client: C, func: F) -> Result<Resp3Frame, RedisError>
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
    let args_span = trace::create_args_span(cmd_span.id());
    let _enter = args_span.enter();
    let (tx, rx) = oneshot_channel();

    let mut command: RedisCommand = func()?.into();
    command.response = ResponseKind::Respond(Some(tx));

    let req_size = protocol_utils::args_size(&command.args);
    args_span.record("num_args", &command.args.len());
    let _ = disallow_nested_values(&command)?;
    (command, rx, req_size)
  };
  cmd_span.record("cmd", &command.kind.to_str_debug());
  cmd_span.record("req_size", &req_size);

  let queued_span = trace::create_queued_span(cmd_span.id(), inner);
  command.traces.cmd_id = cmd_span.id();
  command.traces.queued = Some(queued_span);

  let _ = check_blocking_policy(inner, &command).await?;
  let _ = client.send_command(command)?;

  wait_for_response(rx, inner.default_command_timeout())
    .and_then(|frame| async move {
      trace::record_response_size(&end_cmd_span, &frame);
      Ok::<_, RedisError>(frame)
    })
    .instrument(cmd_span)
    .await
}

#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
pub async fn request_response<C, F, R>(client: C, func: F) -> Result<Resp3Frame, RedisError>
where
  C: ClientLike,
  R: Into<RedisCommand>,
  F: FnOnce() -> Result<R, RedisError>,
{
  basic_request_response(client, func).await
}

/// Find the server that should receive a command on the `BackChannel` connection.
///
/// * If the client is clustered then attempt to hash the arguments, otherwise pick a random node.
/// * If the client is centralized then use the server specified in the `RedisConfig`.
/// * If the client uses the sentinel interface then use the cached primary  ID.
pub fn route_backchannel_command(
  inner: &Arc<RedisClientInner>,
  command: &RedisCommand,
) -> Result<ArcStr, RedisError> {
  match inner.config.server {
    ServerConfig::Sentinel { .. } => {
      inner
        .sentinel_primary
        .read()
        .as_ref()
        .map(|s| s.clone())
        .ok_or(RedisError::new(
          RedisErrorKind::Sentinel,
          "Failed to read sentinel primary server",
        ))
    },
    ServerConfig::Centralized { ref host, ref port } => Ok(ArcStr::from(format!("{}:{}", host, port))),
    ServerConfig::Clustered { .. } => {
      if let Some(key) = command.extract_key() {
        // hash the key and send the command to that node
        let hash_slot = redis_protocol::redis_keyslot(&key);
        let server = match &*inner.cluster_state.read() {
          Some(ref state) => match state.get_server(hash_slot) {
            Some(slot) => slot.server.clone(),
            None => {
              return Err(RedisError::new(
                RedisErrorKind::Cluster,
                "Failed to find cluster node at hash slot.",
              ))
            },
          },
          None => {
            return Err(RedisError::new(
              RedisErrorKind::Cluster,
              "Failed to find cluster state.",
            ))
          },
        };

        Ok(server)
      } else {
        // read a random node from the cluster
        let server = match &*inner.cluster_state.read() {
          Some(ref state) => match state.random_slot() {
            Some(slot) => slot.server.clone(),
            None => {
              return Err(RedisError::new(
                RedisErrorKind::Cluster,
                "Failed to find read random cluster node.",
              ))
            },
          },
          None => {
            return Err(RedisError::new(
              RedisErrorKind::Cluster,
              "Failed to find cluster state.",
            ))
          },
        };

        Ok(server)
      }
    },
  }
}

pub async fn backchannel_request_response<F>(
  inner: &Arc<RedisClientInner>,
  use_blocked: bool,
  func: F,
) -> Result<Resp3Frame, RedisError>
where
  F: FnOnce() -> Result<(RedisCommandKind, Vec<RedisValue>), RedisError>,
{
  let (kind, args) = func()?;
  let command = RedisCommand::new(kind, args, None);
  let _ = disallow_nested_values(&command)?;

  let blocked_server = inner.backchannel.read().await.blocked.clone();

  if let Some(ref blocked_server) = blocked_server {
    // if we're clustered and only one server is blocked then send the command to the blocked server
    _debug!(
      inner,
      "Backchannel: Using blocked server {} for {}",
      blocked_server,
      command.kind.to_str_debug()
    );

    inner
      .backchannel
      .write()
      .await
      .request_response(inner, blocked_server, command, use_blocked)
      .await
  } else {
    // otherwise no connections are blocked
    let server = find_backchannel_server(inner, &command)?;
    _debug!(
      inner,
      "Backchannel: Sending to backchannel server {}: {}",
      server,
      command.kind.to_str_debug()
    );

    inner
      .backchannel
      .write()
      .await
      .request_response(inner, &server, command, use_blocked)
      .await
  }
}

pub async fn read_connection_ids(inner: &Arc<RedisClientInner>) -> Option<HashMap<Arc<String>, i64>> {
  inner
    .backchannel
    .read()
    .await
    .connection_ids
    .as_ref()
    .and_then(|connection_ids| match connection_ids {
      ConnectionIDs::Clustered(ref connection_ids) => {
        Some(connection_ids.read().iter().map(|(k, v)| (k.clone(), *v)).collect())
      },
      ConnectionIDs::Centralized(connection_id) => {
        if let Some(id) = connection_id.read().as_ref() {
          let mut out = HashMap::with_capacity(1);
          let server = match read_centralized_server(inner) {
            Some(server) => server,
            None => return None,
          };

          out.insert(server, *id);
          Some(out)
        } else {
          None
        }
      },
    })
}

pub fn read_sentinel_host(inner: &Arc<RedisClientInner>) -> Result<(Vec<(String, u16)>, String), RedisError> {
  match inner.config.read().server {
    #[cfg(not(feature = "sentinel-auth"))]
    ServerConfig::Sentinel {
      ref hosts,
      ref service_name,
    } => Ok((hosts.clone(), service_name.to_owned())),
    #[cfg(feature = "sentinel-auth")]
    ServerConfig::Sentinel {
      ref hosts,
      ref service_name,
      ..
    } => Ok((hosts.clone(), service_name.to_owned())),
    _ => Err(RedisError::new(RedisErrorKind::Config, "Expected sentinel config.")),
  }
}

pub async fn update_sentinel_nodes(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  let (hosts, service_name) = read_sentinel_host(inner)?;
  let timeout = globals().sentinel_connection_timeout_ms() as u64;

  for (host, port) in hosts.into_iter() {
    _debug!(inner, "Updating sentinel nodes from {}:{}...", host, port);

    let transport = match sentinel::connect_to_sentinel(inner, &host, port, timeout).await {
      Ok(transport) => transport,
      Err(e) => {
        _warn!(
          inner,
          "Failed to connect to sentinel {}:{} with error: {:?}",
          host,
          port,
          e
        );
        continue;
      },
    };
    if let Err(e) = sentinel::update_sentinel_nodes(inner, transport, &service_name).await {
      _warn!(
        inner,
        "Failed to read sentinel nodes from {}:{} with error: {:?}",
        host,
        port,
        e
      );
    } else {
      return Ok(());
    }
  }

  Err(RedisError::new(
    RedisErrorKind::Sentinel,
    "Failed to read sentinel nodes from any known sentinel.",
  ))
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

pub fn disallow_nested_values(cmd: &RedisCommand) -> Result<(), RedisError> {
  for arg in cmd.args.iter() {
    if arg.is_map() || arg.is_array() {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument,
        format!("Invalid argument type: {:?}", arg.kind()),
      ));
    }
  }

  Ok(())
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
      "Found wildcard in scan pattern hash tag. You're probably not scanning the correct node."
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
  delay.saturating_add(rand::thread_rng().gen_range(0..jitter as u64))
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

#[cfg(feature = "serde-json")]
pub fn parse_nested_json(s: &str) -> Option<Value> {
  let trimmed = s.trim();
  let is_maybe_json =
    (trimmed.starts_with("{") && trimmed.ends_with("}")) || (trimmed.starts_with("[") && trimmed.ends_with("]"));

  if is_maybe_json {
    serde_json::from_str(s).ok()
  } else {
    None
  }
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

pub fn is_maybe_array_map(arr: &Vec<RedisValue>) -> bool {
  if arr.len() > 0 && arr.len() % 2 == 0 {
    arr.chunks(2).fold(true, |b, chunk| b && !chunk[0].is_aggregate_type())
  } else {
    false
  }
}

#[cfg(feature = "enable-native-tls")]
pub fn check_tls_features() {}

#[cfg(not(feature = "enable-native-tls"))]
pub fn check_tls_features() {
  warn!("TLS features are not enabled, but a TLS feature may have been used.");
}

#[cfg(feature = "enable-native-tls")]
pub fn tls_config_from_url(tls: bool) -> Option<TlsConfig> {
  if tls {
    Some(TlsConfig::default())
  } else {
    None
  }
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
  // gracefully handle empty paths with a / prefix
  if parts[0].trim() == "" {
    return Ok(None);
  }

  Ok(Some(parts[0].parse()?))
}

pub fn parse_url_credentials(url: &Url) -> (Option<String>, Option<String>) {
  let username = if url.username().is_empty() {
    None
  } else {
    Some(url.username().to_owned())
  };
  let password = url.password().map(|s| s.to_owned());

  (username, password)
}

pub fn parse_url_other_nodes(url: &Url) -> Result<Vec<(String, u16)>, RedisError> {
  let mut out = Vec::new();

  for (key, value) in url.query_pairs().into_iter() {
    if key == CLUSTER_NODE_QUERY {
      let parts: Vec<&str> = value.split(":").collect();
      if parts.len() != 2 {
        return Err(RedisError::new(
          RedisErrorKind::Config,
          format!("Invalid host:port for cluster node: {}", value),
        ));
      }

      let host = parts[0].to_owned();
      let port = parts[1].parse::<u16>()?;
      out.push((host, port));
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::error::RedisError;
  use crate::types::RedisValue;
  use std::convert::TryInto;
  use std::fmt::Debug;

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
}
