use super::*;
use crate::{
  error::*,
  modules::inner::RedisClientInner,
  protocol::{types::*, utils as protocol_utils},
  types::*,
  utils,
};
use bytes_utils::Str;
use std::{collections::VecDeque, sync::Arc};

pub async fn subscribe(inner: &Arc<RedisClientInner>, channel: Str) -> Result<usize, RedisError> {
  // note: if this ever changes to take in more than one channel then some additional work must be done
  // in the multiplexer to associate multiple responses with a single request
  let results = one_arg_values_cmd(inner, RedisCommandKind::Subscribe, channel.into()).await?;

  // last value in the array is number of channels
  if let RedisValue::Array(mut values) = results {
    values
      .pop()
      .and_then(|c| c.as_u64())
      .map(|c| c as usize)
      .ok_or(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid SUBSCRIBE response.",
      ))
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid SUBSCRIBE response.",
    ))
  }
}

pub async fn unsubscribe(inner: &Arc<RedisClientInner>, channel: Str) -> Result<usize, RedisError> {
  // note: if this ever changes to take in more than one channel then some additional work must be done
  // in the multiplexer to associate multiple responses with a single request
  let results = one_arg_values_cmd(inner, RedisCommandKind::Unsubscribe, channel.into()).await?;

  // last value in the array is number of channels
  if let RedisValue::Array(mut values) = results {
    values.pop().and_then(|c| c.as_usize()).ok_or(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid UNSUBSCRIBE response.",
    ))
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid UNSUBSCRIBE response.",
    ))
  }
}

pub async fn publish(
  inner: &Arc<RedisClientInner>,
  channel: Str,
  message: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn psubscribe<S>(inner: &Arc<RedisClientInner>, patterns: S) -> Result<Vec<usize>, RedisError>
where
  S: Into<MultipleStrings>, {
  let patterns = patterns.into();
  let frame = utils::request_response(inner, move || {
    let kind = RedisCommandKind::Psubscribe(ResponseKind::Multiple {
      count:  patterns.len(),
      buffer: VecDeque::with_capacity(patterns.len()),
    });
    let mut args = Vec::with_capacity(patterns.len());

    for pattern in patterns.inner().into_iter() {
      args.push(pattern.into());
    }

    Ok((kind, args))
  })
  .await?;

  let result = protocol_utils::frame_to_results(frame)?;
  if let RedisValue::Array(values) = result {
    utils::pattern_pubsub_counts(values)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array of results.",
    ))
  }
}

pub async fn punsubscribe<S>(inner: &Arc<RedisClientInner>, patterns: S) -> Result<Vec<usize>, RedisError>
where
  S: Into<MultipleStrings>, {
  let patterns = patterns.into();
  let frame = utils::request_response(inner, move || {
    let kind = RedisCommandKind::Punsubscribe(ResponseKind::Multiple {
      count:  patterns.len(),
      buffer: VecDeque::with_capacity(patterns.len()),
    });
    let mut args = Vec::with_capacity(patterns.len());

    for pattern in patterns.inner().into_iter() {
      args.push(pattern.into());
    }

    Ok((kind, args))
  })
  .await?;

  let result = protocol_utils::frame_to_results(frame)?;
  if let RedisValue::Array(values) = result {
    utils::pattern_pubsub_counts(values)
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array of results.",
    ))
  }
}
