use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::types::*;
use bytes_utils::Str;
use std::sync::Arc;

ok_cmd!(config_resetstat, ConfigResetStat);
ok_cmd!(config_rewrite, ConfigRewrite);

pub async fn config_get(inner: &Arc<RedisClientInner>, parameter: Str) -> Result<RedisValue, RedisError> {
  one_arg_values_cmd(inner, RedisCommandKind::ConfigGet, parameter.into()).await
}

pub async fn config_set(inner: &Arc<RedisClientInner>, parameter: Str, value: RedisValue) -> Result<(), RedisError> {
  args_ok_cmd(inner, RedisCommandKind::ConfigSet, vec![parameter.into(), value]).await
}
