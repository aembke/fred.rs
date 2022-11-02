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
  types::*,
  utils,
};
use bytes_utils::Str;
use futures::stream::{Stream, TryStreamExt};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;

static STARTING_CURSOR: &'static str = "0";

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

fn early_error<T: Send + 'static>(tx: &UnboundedSender<Result<T, RedisError>>, error: RedisError) {
  let tx = tx.clone();
  let _ = tokio::spawn(async move {
    let _ = tx.send(Err(error));
  });
}

pub fn scan_cluster(
  inner: &Arc<RedisClientInner>,
  pattern: Str,
  count: Option<u32>,
  r#type: Option<ScanType>,
) -> impl Stream<Item = Result<ScanResult, RedisError>> {
  let (tx, rx) = unbounded_channel();

  let hash_slots = inner.with_cluster_state(|state| Ok(state.unique_hash_slots()));
  let hash_slots = match hash_slots {
    Ok(slots) => slots,
    Err(e) => {
      early_error(&tx, e);
      return UnboundedReceiverStream::new(rx);
    },
  };

  let mut args = Vec::with_capacity(7);
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

  for slot in hash_slots.into_iter() {
    _trace!(inner, "Scan cluster hash slot server: {}", slot);
    let response = ResponseKind::KeyScan(KeyScanInner {
      hash_slot:  Some(slot),
      args:       args.clone(),
      cursor_idx: 0,
      tx:         tx.clone(),
    });
    let command: RedisCommand = (RedisCommandKind::Scan, Vec::new(), response).into();

    if let Err(e) = interfaces::default_send_command(inner, command) {
      early_error(&tx, e);
      break;
    }
  }

  UnboundedReceiverStream::new(rx)
}

pub fn scan(
  inner: &Arc<RedisClientInner>,
  pattern: Str,
  count: Option<u32>,
  r#type: Option<ScanType>,
) -> impl Stream<Item = Result<ScanResult, RedisError>> {
  let (tx, rx) = unbounded_channel();

  let hash_slot = if inner.config.server.is_clustered() {
    let result = inner.with_cluster_state(|state| {
      Ok(if utils::clustered_scan_pattern_has_hash_tag(inner, &pattern) {
        Some(redis_keyslot(pattern.as_bytes()))
      } else {
        None
      })
    });

    match result {
      Ok(slot) => slot,
      Err(e) => {
        early_error(&tx, e);
        return UnboundedReceiverStream::new(rx);
      },
    }
  } else {
    None
  };

  let mut args = Vec::with_capacity(7);
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

  let response = ResponseKind::KeyScan(KeyScanInner {
    hash_slot,
    args,
    cursor_idx: 0,
    tx: tx.clone(),
  });
  let command: RedisCommand = (RedisCommandKind::Scan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    early_error(&tx, e);
  }

  UnboundedReceiverStream::new(rx)
}

pub fn hscan(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<HScanResult, RedisError>> {
  let (tx, rx) = unbounded_channel();
  let args = values_args(key, pattern, count);

  let response = ResponseKind::ValueScan(ValueScanInner {
    tx: tx.clone(),
    cursor_idx: 1,
    args,
  });
  let command: RedisCommand = (RedisCommandKind::Hscan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    early_error(&tx, e);
  }

  UnboundedReceiverStream::new(rx).try_filter_map(|result| async move {
    match result {
      ValueScanResult::HScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected HSCAN result.")),
    }
  })
}

pub fn sscan(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<SScanResult, RedisError>> {
  let (tx, rx) = unbounded_channel();
  let args = values_args(key, pattern, count);

  let response = ResponseKind::ValueScan(ValueScanInner {
    tx: tx.clone(),
    cursor_idx: 1,
    args,
  });
  let command: RedisCommand = (RedisCommandKind::Sscan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    early_error(&tx, e);
  }

  UnboundedReceiverStream::new(rx).try_filter_map(|result| async move {
    match result {
      ValueScanResult::SScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected SSCAN result.")),
    }
  })
}

pub fn zscan(
  inner: &Arc<RedisClientInner>,
  key: RedisKey,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<ZScanResult, RedisError>> {
  let (tx, rx) = unbounded_channel();
  let args = values_args(key.into(), pattern, count);

  let response = ResponseKind::ValueScan(ValueScanInner {
    tx: tx.clone(),
    cursor_idx: 1,
    args,
  });
  let command: RedisCommand = (RedisCommandKind::Zscan, Vec::new(), response).into();

  if let Err(e) = interfaces::default_send_command(inner, command) {
    early_error(&tx, e);
  }

  UnboundedReceiverStream::new(rx).try_filter_map(|result| async move {
    match result {
      ValueScanResult::ZScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::Protocol, "Expected ZSCAN result.")),
    }
  })
}
