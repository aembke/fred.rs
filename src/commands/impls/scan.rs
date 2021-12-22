use super::*;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::types::*;
use crate::utils;
use futures::stream::{Stream, TryStreamExt};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;

static STARTING_CURSOR: &'static str = "0";

fn values_args(key: RedisKey, pattern: String, count: Option<u32>) -> Vec<RedisValue> {
  let mut args = Vec::with_capacity(6);
  args.push(key.into());
  args.push(STARTING_CURSOR.into());
  args.push(MATCH.into());
  args.push(pattern.into());

  if let Some(count) = count {
    args.push(COUNT.into());
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

pub fn scan<S>(
  inner: &Arc<RedisClientInner>,
  pattern: S,
  count: Option<u32>,
  r#type: Option<ScanType>,
) -> impl Stream<Item = Result<ScanResult, RedisError>>
where
  S: Into<String>,
{
  let (tx, rx) = unbounded_channel();
  let err_tx = tx.clone();
  if let Err(e) = utils::disallow_during_transaction(inner) {
    let _ = tokio::spawn(async move {
      let _ = err_tx.send(Err(e));
    });
    return UnboundedReceiverStream::new(rx);
  }

  let pattern = pattern.into();
  let key_slot = if utils::is_clustered(&inner.config) {
    if utils::clustered_scan_pattern_has_hash_tag(inner, &pattern) {
      Some(redis_keyslot(&pattern))
    } else {
      None
    }
  } else {
    None
  };

  let mut args = Vec::with_capacity(7);
  args.push(STARTING_CURSOR.into());
  args.push(MATCH.into());
  args.push(pattern.into());

  if let Some(count) = count {
    args.push(COUNT.into());
    args.push(count.into());
  }
  if let Some(r#type) = r#type {
    args.push(TYPE.into());
    args.push(r#type.to_str().into());
  }

  let err_tx = tx.clone();
  let scan = KeyScanInner {
    key_slot,
    tx,
    cursor: STARTING_CURSOR.into(),
  };

  let cmd = RedisCommand::new(RedisCommandKind::Scan(scan), args, None);
  if let Err(e) = utils::send_command(inner, cmd) {
    let _ = tokio::spawn(async move {
      let _ = err_tx.send(Err(e));
    });
  }

  UnboundedReceiverStream::new(rx)
}

pub fn hscan<K, P>(
  inner: &Arc<RedisClientInner>,
  key: K,
  pattern: P,
  count: Option<u32>,
) -> impl Stream<Item = Result<HScanResult, RedisError>>
where
  K: Into<RedisKey>,
  P: Into<String>,
{
  let (tx, rx) = unbounded_channel();
  let should_send = if let Err(e) = utils::disallow_during_transaction(inner) {
    early_error(&tx, e);
    false
  } else {
    true
  };

  if should_send {
    let (key, pattern) = (key.into(), pattern.into());
    let args = values_args(key, pattern, count);
    let err_tx = tx.clone();
    let scan = ValueScanInner {
      tx,
      cursor: STARTING_CURSOR.into(),
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

pub fn sscan<K, P>(
  inner: &Arc<RedisClientInner>,
  key: K,
  pattern: P,
  count: Option<u32>,
) -> impl Stream<Item = Result<SScanResult, RedisError>>
where
  K: Into<RedisKey>,
  P: Into<String>,
{
  let (tx, rx) = unbounded_channel();
  let should_send = if let Err(e) = utils::disallow_during_transaction(inner) {
    early_error(&tx, e);
    false
  } else {
    true
  };

  if should_send {
    let (key, pattern) = (key.into(), pattern.into());
    let args = values_args(key, pattern, count);
    let err_tx = tx.clone();
    let scan = ValueScanInner {
      tx,
      cursor: STARTING_CURSOR.into(),
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

pub fn zscan<K, P>(
  inner: &Arc<RedisClientInner>,
  key: K,
  pattern: P,
  count: Option<u32>,
) -> impl Stream<Item = Result<ZScanResult, RedisError>>
where
  K: Into<RedisKey>,
  P: Into<String>,
{
  let (tx, rx) = unbounded_channel();
  let should_send = if let Err(e) = utils::disallow_during_transaction(inner) {
    early_error(&tx, e);
    false
  } else {
    true
  };

  if should_send {
    let (key, pattern) = (key.into(), pattern.into());
    let args = values_args(key, pattern, count);
    let err_tx = tx.clone();
    let scan = ValueScanInner {
      tx,
      cursor: STARTING_CURSOR.into(),
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
