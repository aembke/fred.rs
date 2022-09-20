use super::*;
use crate::{error::*, modules::inner::RedisClientInner, protocol::types::*, types::*, utils};
use bytes_utils::Str;
use futures::stream::{Stream, TryStreamExt};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;

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
  let err_tx = tx.clone();
  if let Err(e) = utils::disallow_during_transaction(inner) {
    let _ = tokio::spawn(async move {
      let _ = err_tx.send(Err(e));
    });
    return UnboundedReceiverStream::new(rx);
  }

  let hash_slots: Vec<u16> = if let Some(ref state) = *inner.cluster_state.read() {
    state.unique_hash_slots()
  } else {
    early_error(
      &tx,
      RedisError::new(RedisErrorKind::Cluster, "Invalid or missing cluster state."),
    );
    return UnboundedReceiverStream::new(rx);
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
    let scan_inner = KeyScanInner {
      key_slot: Some(slot),
      tx:       tx.clone(),
      cursor:   utils::static_str(STARTING_CURSOR),
    };
    let cmd = RedisCommand::new(RedisCommandKind::Scan(scan_inner), args.clone(), None);

    if let Err(e) = utils::send_command(inner, cmd) {
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
  let err_tx = tx.clone();
  if let Err(e) = utils::disallow_during_transaction(inner) {
    let _ = tokio::spawn(async move {
      let _ = err_tx.send(Err(e));
    });
    return UnboundedReceiverStream::new(rx);
  }

  let key_slot = if utils::is_clustered(&inner.config) {
    if utils::clustered_scan_pattern_has_hash_tag(inner, &pattern) {
      Some(redis_keyslot(pattern.as_bytes()))
    } else {
      None
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

  let err_tx = tx.clone();
  let scan = KeyScanInner {
    key_slot,
    tx,
    cursor: utils::static_str(STARTING_CURSOR),
  };

  let cmd = RedisCommand::new(RedisCommandKind::Scan(scan), args, None);
  if let Err(e) = utils::send_command(inner, cmd) {
    let _ = tokio::spawn(async move {
      let _ = err_tx.send(Err(e));
    });
  }

  UnboundedReceiverStream::new(rx)
}

pub fn hscan<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<HScanResult, RedisError>>
where
  K: Into<RedisKey>, {
  let (tx, rx) = unbounded_channel();
  let should_send = if let Err(e) = utils::disallow_during_transaction(inner) {
    early_error(&tx, e);
    false
  } else {
    true
  };

  if should_send {
    let args = values_args(key.into(), pattern, count);
    let err_tx = tx.clone();
    let scan = ValueScanInner {
      tx,
      cursor: utils::static_str(STARTING_CURSOR),
    };

    let cmd = RedisCommand::new(RedisCommandKind::Hscan(scan), args, None);
    if let Err(e) = utils::send_command(inner, cmd) {
      let _ = tokio::spawn(async move {
        let _ = err_tx.send(Err(e));
      });
    }
  }

  UnboundedReceiverStream::new(rx).try_filter_map(|result| async move {
    match result {
      ValueScanResult::HScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected HSCAN result.")),
    }
  })
}

pub fn sscan<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<SScanResult, RedisError>>
where
  K: Into<RedisKey>, {
  let (tx, rx) = unbounded_channel();
  let should_send = if let Err(e) = utils::disallow_during_transaction(inner) {
    early_error(&tx, e);
    false
  } else {
    true
  };

  if should_send {
    let args = values_args(key.into(), pattern, count);
    let err_tx = tx.clone();
    let scan = ValueScanInner {
      tx,
      cursor: utils::static_str(STARTING_CURSOR),
    };

    let cmd = RedisCommand::new(RedisCommandKind::Sscan(scan), args, None);
    if let Err(e) = utils::send_command(inner, cmd) {
      let _ = tokio::spawn(async move {
        let _ = err_tx.send(Err(e));
      });
    }
  }

  UnboundedReceiverStream::new(rx).try_filter_map(|result| async move {
    match result {
      ValueScanResult::SScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected SSCAN result.")),
    }
  })
}

pub fn zscan<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  pattern: Str,
  count: Option<u32>,
) -> impl Stream<Item = Result<ZScanResult, RedisError>>
where
  K: Into<RedisKey>, {
  let (tx, rx) = unbounded_channel();
  let should_send = if let Err(e) = utils::disallow_during_transaction(inner) {
    early_error(&tx, e);
    false
  } else {
    true
  };

  if should_send {
    let args = values_args(key.into(), pattern, count);
    let err_tx = tx.clone();
    let scan = ValueScanInner {
      tx,
      cursor: utils::static_str(STARTING_CURSOR),
    };

    let cmd = RedisCommand::new(RedisCommandKind::Zscan(scan), args, None);
    if let Err(e) = utils::send_command(inner, cmd) {
      let _ = tokio::spawn(async move {
        let _ = err_tx.send(Err(e));
      });
    }
  }

  UnboundedReceiverStream::new(rx).try_filter_map(|result| async move {
    match result {
      ValueScanResult::ZScan(res) => Ok(Some(res)),
      _ => Err(RedisError::new(RedisErrorKind::ProtocolError, "Expected ZSCAN result.")),
    }
  })
}
