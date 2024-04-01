use super::*;
use crate::{
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  utils,
};

pub async fn slowlog_get<C: ClientLike>(client: &C, count: Option<i64>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    args.push(static_val!(GET));

    if let Some(count) = count {
      args.push(count.into());
    }

    Ok((RedisCommandKind::Slowlog, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn slowlog_length<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || Ok((RedisCommandKind::Slowlog, vec![LEN.into()]))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn slowlog_reset<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  args_ok_cmd(client, RedisCommandKind::Slowlog, vec![static_val!(RESET)]).await
}
