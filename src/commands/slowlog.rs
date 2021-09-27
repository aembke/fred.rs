use super::*;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::sync::Arc;

pub async fn slowlog_get(inner: &Arc<RedisClientInner>, count: Option<i64>) -> Result<Vec<SlowlogEntry>, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(GET.into());

    if let Some(count) = count {
      args.push(count.into());
    }

    Ok((RedisCommandKind::Slowlog, args))
  })
  .await?;

  if let Frame::Array(frames) = frame {
    protocol_utils::parse_slowlog_entries(frames)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array response.",
    ))
  }
}

pub async fn slowlog_length(inner: &Arc<RedisClientInner>) -> Result<u64, RedisError> {
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::Slowlog, vec![LEN.into()]))).await?;
  let response = protocol_utils::frame_to_single_result(frame)?;

  if let RedisValue::Integer(len) = response {
    Ok(len as u64)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected integer response.",
    ))
  }
}

pub async fn slowlog_reset(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  args_ok_cmd(inner, RedisCommandKind::Slowlog, vec![RESET.into()]).await
}
