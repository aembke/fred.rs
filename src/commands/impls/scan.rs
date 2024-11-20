use super::*;
use crate::{
  error::*,
  interfaces,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    responders::ResponseKind,
    types::*,
  },
  runtime::{channel, RefCount},
  types::*,
  utils,
};
use bytes_utils::Str;
use futures::stream::{Stream, TryStreamExt};

static STARTING_CURSOR: &str = "0";

fn values_args(key: RedisKey, pattern: Str, count: Option<u32>) -> Vec<RedisValue> {
  let mut args = Vec::with_capacity(6);
  args.push(key.into());
  args.push(static_val!(STARTING_CURSOR));
  args.push(static_val!(MATCH));
  args.push(pattern.into());

  if let Some(count) = count {
    args.push(static_val!(COUNT));
    args.push(count.into());
  }

  args
}

fn create_scan_args(args: &mut Vec<RedisValue>, pattern: Str, count: Option<u32>, r#type: Option<ScanType>) {
  args.push(static_val!(STARTING_CURSOR));
  args.push(static_val!(MATCH));
  args.push(pattern.into());

  if let Some(count) = count {
    args.push(static_val!(COUNT));
    args.push(count.into());
  }
  if let Some(r#type) = r#type {
    args.push(static_val!(TYPE));
    args.push(r#type.to_str().into());
  }
}

// TODO change all of the scanning functions to return a Future<Stream<...>> in the next major version to avoid the
// task spawning here.

pub fn scan_cluster(
  inner: &RefCount<RedisClientInner>,
  pattern: Str,
  count: Option<u32>,
  r#type: Option<ScanType>,
) -> impl Stream<Item = Result<ScanResult, RedisError>> {
  let (tx, rx) = channel(0);

  let hash_slots = inner.with_cluster_state(|state| Ok(state.unique_hash_slots()));
  let hash_slots = match hash_slots {
    Ok(slots) => slots,
    Err(e) => {
      let _ = tx.try_send(Err(e));
      return rx.into_stream();
    },
  };

  let mut args = Vec::with_capacity(7);
  create_scan_args(&mut args, pattern, count, r#type);
  for slot in hash_slots.into_iter() {
    _trace!(inner, "Scan cluster hash slot server: {}", slot);
    let response = ResponseKind::KeyScan(KeyScanInner {
      hash_slot:  Some(slot),
      args:       args.clone(),
      cursor_idx: 0,
      tx:         tx.clone(),
      server:     None,
    });
    let command: RedisCommand = (RedisCommandKind::Scan, Vec::new(), response).into();

    if let Err(e) = interfaces::default_send_command(inner, command) {
      let _ = tx.try_send(Err(e));
      break;
    }
  }

  rx.into_stream()
}

pub fn scan_cluster_buffered(
  inner: &RefCount<RedisClientInner>,
  pattern: Str,
  count: Option<u32>,
  r#type: Option<ScanType>,
) -> impl Stream<Item = Result<RedisKey, RedisError>> {
  let (tx, rx) = channel(0);

  let hash_slots = inner.with_cluster_state(|state| Ok(state.unique_hash_slots()));
  let hash_slots = match hash_slots {
    Ok(slots) => slots,
    Err(e) => {
      let _ = tx.try_send(Err(e));
      return rx.into_stream();
    },
  };

  let mut args = Vec::with_capacity(7);
  create_scan_args(&mut args, pattern, count, r#type);
  for slot in hash_slots.into_iter() {
    _trace!(inner, "Scan cluster buffered hash slot server: {}", slot);
    let response = ResponseKind::KeyScanBuffered(KeyScanBufferedInner {
      hash_slot:  Some(slot),
      args:       args.clone(),
      cursor_idx: 0,
      tx:         tx.clone(),
      server:     None,
    });
    let command: RedisCommand = (RedisCommandKind::Scan, Vec::new(), response).into();

    if let Err(e) = interfaces::default_send_command(inner, command) {
      let _ = tx.try_send(Err(e));
      break;
    }
  }

  rx.into_stream()
}

pub fn scan(
  inner: &RefCount<RedisClientInner>,
  pattern: Str,
  count: Option<u32>,
  r#type: Option<ScanType>,
  server: Option<Server>,
) -> impl Stream<Item = Result<ScanResult, RedisError>> {
  let (tx, rx) = channel(0);

  let hash_slot = if inner.config.server.is_clustered() {
    if utils::clustered_scan_pattern_has_hash_tag(inner, &pattern) {
      Some(redis_protocol::redis_keyslot(pattern.as_bytes()))
    } else {
      None
    }
  } else {
    None
  };

  let mut args = Vec::with_capacity(7);
  create_scan_args(&mut args, pattern, count, r#type);
  let response = ResponseKind::KeyScan(KeyScanInner {
    hash_slot,
    args,
    server,
    cursor_idx: 0,
    tx: tx.clone(),
  });
  let command: RedisCommand = (RedisCommandKind::Scan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    let _ = tx.try_send(Err(e));
  }

  rx.into_stream()
}

pub fn scan_buffered(
  inner: &RefCount<RedisClientInner>,
  pattern: Str,
  count: Option<u32>,
  r#type: Option<ScanType>,
  server: Option<Server>,
) -> impl Stream<Item = Result<RedisKey, RedisError>> {
  let (tx, rx) = channel(0);

  let hash_slot = if inner.config.server.is_clustered() {
    if utils::clustered_scan_pattern_has_hash_tag(inner, &pattern) {
      Some(redis_protocol::redis_keyslot(pattern.as_bytes()))
    } else {
      None
    }
  } else {
    None
  };

  let mut args = Vec::with_capacity(7);
  create_scan_args(&mut args, pattern, count, r#type);
  let response = ResponseKind::KeyScanBuffered(KeyScanBufferedInner {
    hash_slot,
    args,
    server,
    cursor_idx: 0,
    tx: tx.clone(),
  });
  let command: RedisCommand = (RedisCommandKind::Scan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    let _ = tx.try_send(Err(e));
  }

  rx.into_stream()
}

pub fn hscan(
  inner: &RefCount<RedisClientInner>,
  key: RedisKey,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<HScanResult, RedisError>> {
  let (tx, rx) = channel(0);
  let args = values_args(key, pattern, count);

  let response = ResponseKind::ValueScan(ValueScanInner {
    tx: tx.clone(),
    cursor_idx: 1,
    args,
  });
  let command: RedisCommand = (RedisCommandKind::Hscan, Vec::new(), response).into();
  if let Err(e) = interfaces::default_send_command(inner, command) {
    let _ = tx.try_send(Err(e));
  }

  rx.into_stream().try_filter_map(|result| async move {
    match result {
      ValueScanResult::HScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected HSCAN result.")),
    }
  })
}

pub fn sscan(
  inner: &RefCount<RedisClientInner>,
  key: RedisKey,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<SScanResult, RedisError>> {
  let (tx, rx) = channel(0);
  let args = values_args(key, pattern, count);

  let response = ResponseKind::ValueScan(ValueScanInner {
    tx: tx.clone(),
    cursor_idx: 1,
    args,
  });
  let command: RedisCommand = (RedisCommandKind::Sscan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    let _ = tx.try_send(Err(e));
  }

  rx.into_stream().try_filter_map(|result| async move {
    match result {
      ValueScanResult::SScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected SSCAN result.")),
    }
  })
}

pub fn zscan(
  inner: &RefCount<RedisClientInner>,
  key: RedisKey,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<ZScanResult, RedisError>> {
  let inner = inner.clone();
  let (tx, rx) = channel(0);
  let args = values_args(key, pattern, count);

  let response = ResponseKind::ValueScan(ValueScanInner {
    tx: tx.clone(),
    cursor_idx: 1,
    args,
  });
  let command: RedisCommand = (RedisCommandKind::Zscan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(&inner, command) {
    let _ = tx.try_send(Err(e));
  }

  rx.into_stream().try_filter_map(|result| async move {
    match result {
      ValueScanResult::ZScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected ZSCAN result.")),
    }
  })
}
