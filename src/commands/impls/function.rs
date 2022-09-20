use super::*;
use crate::{
  modules::inner::RedisClientInner,
  protocol::{types::*, utils as protocol_utils},
  types::*,
  utils,
};
use bytes_utils::Str;
use std::sync::Arc;

pub async fn function_load(
  inner: &Arc<RedisClientInner>,
  replace: bool,
  script: Str,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    if replace {
      args.push(static_val!(REPLACE));
    }
    args.push(script.into());
    Ok((RedisCommandKind::FunctionLoad, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

macro_rules! fcall {
  ($name:ident, $cmd:ident) => {
    pub async fn $name(
      inner: &Arc<RedisClientInner>,
      function: Str,
      keys: MultipleKeys,
      cmd_args: MultipleValues,
    ) -> Result<RedisValue, RedisError> {
      let keys = keys.inner();
      let frame = utils::request_response(inner, move || {
        let mut args = Vec::with_capacity(2 + keys.len() + cmd_args.len());
        args.push(function.into());
        args.push(keys.len().try_into()?);

        for key in keys.into_iter() {
          args.push(key.into());
        }
        for arg in cmd_args.inner().into_iter() {
          args.push(arg);
        }

        Ok((RedisCommandKind::$cmd, args))
      })
      .await?;

      protocol_utils::frame_to_results(frame)
    }
  };
}

fcall!(fcall, Fcall);
fcall!(fcall_ro, FcallRo);
