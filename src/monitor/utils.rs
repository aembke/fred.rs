use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  monitor::{parser, Command},
  protocol::{
    codec::RedisCodec,
    command::{RedisCommand, RedisCommandKind},
    connection::{self, ConnectionKind, RedisTransport},
    types::ProtocolFrame,
    utils as protocol_utils,
  },
  runtime::{spawn, unbounded_channel, RefCount, UnboundedSender},
  types::{ConnectionConfig, PerformanceConfig, RedisConfig, ServerConfig},
};
use futures::stream::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

#[cfg(all(feature = "blocking-encoding", not(feature = "glommio")))]
use redis_protocol::resp3::types::Resp3Frame;
#[cfg(not(feature = "glommio"))]
use tokio_stream::wrappers::UnboundedReceiverStream;

#[cfg(all(feature = "blocking-encoding", not(feature = "glommio")))]
async fn handle_monitor_frame(
  inner: &RefCount<RedisClientInner>,
  frame: Result<ProtocolFrame, RedisError>,
) -> Option<Command> {
  let frame = match frame {
    Ok(frame) => frame.into_resp3(),
    Err(e) => {
      _error!(inner, "Error on monitor stream: {:?}", e);
      return None;
    },
  };
  let frame_size = frame.encode_len();

  if frame_size >= inner.with_perf_config(|c| c.blocking_encode_threshold) {
    // since this isn't called from the Encoder/Decoder trait we can use spawn_blocking here
    _trace!(
      inner,
      "Parsing monitor frame with blocking task with size {}",
      frame_size
    );

    let inner = inner.clone();
    tokio::task::spawn_blocking(move || parser::parse(&inner, frame))
      .await
      .ok()
      .flatten()
  } else {
    parser::parse(inner, frame)
  }
}

#[cfg(any(not(feature = "blocking-encoding"), feature = "glommio"))]
async fn handle_monitor_frame(
  inner: &RefCount<RedisClientInner>,
  frame: Result<ProtocolFrame, RedisError>,
) -> Option<Command> {
  let frame = match frame {
    Ok(frame) => frame.into_resp3(),
    Err(e) => {
      _error!(inner, "Error on monitor stream: {:?}", e);
      return None;
    },
  };

  parser::parse(inner, frame)
}

async fn send_monitor_command(
  inner: &RefCount<RedisClientInner>,
  mut connection: RedisTransport,
) -> Result<RedisTransport, RedisError> {
  _debug!(inner, "Sending MONITOR command.");

  let command = RedisCommand::new(RedisCommandKind::Monitor, vec![]);
  let frame = connection.request_response(command, inner.is_resp3()).await?;

  _trace!(inner, "Recv MONITOR response: {:?}", frame);
  let response = protocol_utils::frame_to_results(frame)?;
  protocol_utils::expect_ok(&response)?;
  Ok(connection)
}

async fn forward_results<T>(
  inner: &RefCount<RedisClientInner>,
  tx: UnboundedSender<Command>,
  mut framed: Framed<T, RedisCodec>,
) where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  while let Some(frame) = framed.next().await {
    if let Some(command) = handle_monitor_frame(inner, frame).await {
      if let Err(_) = tx.send(command) {
        _warn!(inner, "Stopping monitor stream.");
        return;
      }
    } else {
      _debug!(inner, "Skipping invalid monitor frame.");
    }
  }
}

async fn process_stream(
  inner: &RefCount<RedisClientInner>,
  tx: UnboundedSender<Command>,
  connection: RedisTransport,
) {
  _debug!(inner, "Starting monitor stream processing...");

  match connection.transport {
    ConnectionKind::Tcp(framed) => forward_results(inner, tx, framed).await,
    #[cfg(feature = "enable-rustls")]
    ConnectionKind::Rustls(framed) => forward_results(inner, tx, framed).await,
    #[cfg(feature = "enable-native-tls")]
    ConnectionKind::NativeTls(framed) => forward_results(inner, tx, framed).await,
    #[cfg(feature = "unix-sockets")]
    ConnectionKind::Unix(framed) => forward_results(inner, tx, framed).await,
  };

  _warn!(inner, "Stopping monitor stream.");
}

pub async fn start(config: RedisConfig) -> Result<impl Stream<Item = Command>, RedisError> {
  let perf = PerformanceConfig {
    auto_pipeline: false,
    ..Default::default()
  };
  let connection = ConnectionConfig::default();
  let server = match config.server {
    ServerConfig::Centralized { ref server } => server.clone(),
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Config,
        "Expected centralized server config.",
      ))
    },
  };

  let inner = RedisClientInner::new(config, perf, connection, None);
  let mut connection = connection::create(&inner, &server, None).await?;
  connection.setup(&inner, None).await?;
  let connection = send_monitor_command(&inner, connection).await?;

  // there isn't really a mechanism to surface backpressure to the server for the MONITOR stream, so we use a
  // background task with a channel to process the frames so that the server can keep sending data even if the
  // stream consumer slows down processing the frames.
  let (tx, rx) = unbounded_channel();
  #[cfg(feature = "glommio")]
  let tx = tx.into();
  spawn(async move {
    process_stream(&inner, tx, connection).await;
  });

  #[cfg(feature = "glommio")]
  return Ok(crate::runtime::rx_stream(rx));
  #[cfg(not(feature = "glommio"))]
  return Ok(UnboundedReceiverStream::new(rx));
}
