use super::*;
use crate::{
  error::*,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    responders::ResponseKind,
    types::*,
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use std::{collections::VecDeque, sync::Arc};
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
  let response = ResponseKind::new_multiple(patterns.len(), tx);
  let args = patterns.inner().into_iter().map(|p| p.into()).collect();
  let command: RedisCommand = (RedisCommandKind::Punsubscribe, args, response).into();
  let _ = client.send_command(command)?;

  let frame = rx.await??;
  protocol_utils::frame_to_results(frame)
}
