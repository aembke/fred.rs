use crate::error::RedisErrorKind;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::utils;
use crate::multiplexer::Written;
use crate::prelude::RedisError;
use crate::protocol::command::RedisCommand;
use crate::protocol::connection::RedisWriter;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub async fn send_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut Option<RedisWriter<T>>,
  mut command: RedisCommand,
) -> Result<Written, (RedisError, RedisCommand)>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  if let Some(writer) = writer.as_mut() {
    Ok(utils::write_command(inner, writer, command).await)
  } else {
    _debug!(
      inner,
      "Failed to read connection {} for {}",
      server,
      command.kind.to_str_debug()
    );
    Err((RedisError::new(RedisErrorKind::IO, "Missing connection."), command))
  }
}
