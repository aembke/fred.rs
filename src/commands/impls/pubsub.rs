use super::*;
use crate::{
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use tokio::sync::oneshot::channel as oneshot_channel;

pub async fn subscribe<C: ClientLike>(client: &C, channel: Str) -> Result<RedisValue, RedisError> {
  // note: if this ever changes to take in more than one channel then the response kind must change
  one_arg_values_cmd(client, RedisCommandKind::Subscribe, channel.into()).await
}

pub async fn unsubscribe<C: ClientLike>(client: &C, channel: Str) -> Result<RedisValue, RedisError> {
  // note: if this ever changes to take in more than one channel then the response kind must change
  one_arg_values_cmd(client, RedisCommandKind::Unsubscribe, channel.into()).await
}

pub async fn publish<C: ClientLike>(client: &C, channel: Str, message: RedisValue) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn psubscribe<C: ClientLike>(client: &C, patterns: MultipleStrings) -> Result<RedisValue, RedisError> {
  if patterns.len() == 0 {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(patterns.len(), tx);
  let args = patterns.inner().into_iter().map(|p| p.into()).collect();
  let command: RedisCommand = (RedisCommandKind::Psubscribe, args, response).into();
  let _ = client.send_command(command)?;

  let frame = rx.await??;
  protocol_utils::frame_to_results(frame)
}

pub async fn punsubscribe<C: ClientLike>(client: &C, patterns: MultipleStrings) -> Result<RedisValue, RedisError> {
  let (tx, rx) = oneshot_channel();
  let response = if patterns.len() == 0 {
    ResponseKind::Respond(Some(tx))
  } else {
    ResponseKind::new_multiple(patterns.len(), tx)
  };
  let args = patterns.inner().into_iter().map(|p| p.into()).collect();
  let command: RedisCommand = (RedisCommandKind::Punsubscribe, args, response).into();
  let _ = client.send_command(command)?;

  let frame = rx.await??;
  protocol_utils::frame_to_results(frame)
}

pub async fn spublish<C: ClientLike>(
  client: &C,
  channel: Str,
  message: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut command: RedisCommand = (RedisCommandKind::Spublish, vec![channel.into(), message]).into();
    command.hasher = ClusterHash::FirstKey;

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn ssubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  if channels.len() == 0 {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(channels.len(), tx);
  let args = channels.inner().into_iter().map(|p| p.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Ssubscribe, args, response).into();
  command.hasher = ClusterHash::FirstKey;

  let _ = client.send_command(command)?;
  let frame = rx.await??;
  protocol_utils::frame_to_results(frame)
}

pub async fn sunsubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  let (tx, rx) = oneshot_channel();
  let (response, hasher) = if channels.len() == 0 {
    (ResponseKind::Respond(Some(tx)), ClusterHash::Random)
  } else {
    (ResponseKind::new_multiple(channels.len(), tx), ClusterHash::FirstKey)
  };
  let args = channels.inner().into_iter().map(|p| p.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Sunsubscribe, args, response).into();
  command.hasher = hasher;

  let _ = client.send_command(command)?;
  let frame = rx.await??;
  protocol_utils::frame_to_results(frame)
}
