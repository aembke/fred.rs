use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::types::*;
use std::sync::Arc;

ok_cmd!(config_resetstat, ConfigResetStat);
ok_cmd!(config_rewrite, ConfigRewrite);

pub async fn config_get<S>(inner: &Arc<RedisClientInner>, parameter: S) -> Result<RedisValue, RedisError>
where
  S: Into<String>,
{
  one_arg_values_cmd(inner, RedisCommandKind::ConfigGet, parameter.into().into()).await
}

pub async fn config_set<P>(inner: &Arc<RedisClientInner>, parameter: P, value: RedisValue) -> Result<(), RedisError>
where
  P: Into<String>,
{
  args_ok_cmd(inner, RedisCommandKind::ConfigSet, vec![parameter.into().into(), value]).await
}
