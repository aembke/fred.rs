use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::monitor::parser;
use crate::monitor::{Command, Config};
use crate::protocol::codec::RedisCodec;
use crate::protocol::connection::{self, RedisTransport};
use crate::protocol::types::{RedisCommand, RedisCommandKind};
use crate::protocol::utils as protocol_utils;
use crate::types::{RedisConfig, ServerConfig};
use futures::stream::{Stream, StreamExt};
use redis_protocol::resp2::types::Frame as ProtocolFrame;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec::Framed;

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;

#[cfg(feature = "blocking-encoding")]
async fn handle_monitor_frame(
  inner: &Arc<RedisClientInner>,
  frame: Result<ProtocolFrame, RedisError>,
) -> Option<Command> {
  let frame = match frame {
    Ok(frame) => frame,
    Err(e) => {
      _error!(inner, "Error on monitor stream: {:?}", e);
      return None;
    }
  };
  let frame_size = protocol_utils::frame_size(&frame);

  if frame_size >= globals().blocking_encode_threshold() {
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

#[cfg(not(feature = "blocking-encoding"))]
async fn handle_monitor_frame(
  inner: &Arc<RedisClientInner>,
  frame: Result<ProtocolFrame, RedisError>,
) -> Option<Command> {
  let frame = match frame {
    Ok(frame) => frame,
    Err(e) => {
      _error!(inner, "Error on monitor stream: {:?}", e);
      return None;
    }
  };

  parser::parse(inner, frame)
}

#[cfg(feature = "enable-tls")]
fn create_client_inner(config: Config) -> Arc<RedisClientInner> {
  let config = RedisConfig {
    username: config.username,
    password: config.password,
    server: ServerConfig::Centralized {
      host: config.host,
      port: config.port,
    },
    tls: config.tls,
    ..Default::default()
  };

  RedisClientInner::new(config)
}

#[cfg(not(feature = "enable-tls"))]
fn create_client_inner(config: Config) -> Arc<RedisClientInner> {
  let config = RedisConfig {
    username: config.username,
    password: config.password,
    server: ServerConfig::Centralized {
      host: config.host,
      port: config.port,
    },
    ..Default::default()
  };

  RedisClientInner::new(config)
}

async fn send_monitor_command(
  inner: &Arc<RedisClientInner>,
  connection: RedisTransport,
) -> Result<RedisTransport, RedisError> {
  _debug!(inner, "Sending MONITOR command.");

  let command = RedisCommand::new(RedisCommandKind::Monitor, vec![], None);
  let (frame, connection) = match connection {
    RedisTransport::Tcp(framed) => {
      let (frame, framed) = connection::request_response(framed, &command).await?;
      (frame, RedisTransport::Tcp(framed))
    }
    RedisTransport::Tls(framed) => {
      let (frame, framed) = connection::request_response(framed, &command).await?;
      (frame, RedisTransport::Tls(framed))
    }
  };

  _trace!(inner, "Recv MONITOR response: {:?}", frame);
  let response = protocol_utils::frame_to_single_result(frame)?;
  let _ = protocol_utils::expect_ok(&response)?;
  Ok(connection)
}

async fn forward_results<T>(
  inner: &Arc<RedisClientInner>,
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

async fn process_stream(inner: &Arc<RedisClientInner>, tx: UnboundedSender<Command>, connection: RedisTransport) {
  _debug!(inner, "Starting monitor stream processing...");

  match connection {
    RedisTransport::Tcp(framed) => forward_results(inner, tx, framed).await,
    RedisTransport::Tls(framed) => forward_results(inner, tx, framed).await,
  };

  _warn!(inner, "Stopping monitor stream.");
}

pub async fn start(config: Config) -> Result<impl Stream<Item = Command>, RedisError> {
  let inner = create_client_inner(config);
  let connection = connection::create_centralized_connection(&inner).await?;
  let connection = send_monitor_command(&inner, connection).await?;

  // there isn't really a mechanism to surface backpressure to the server for the MONITOR stream, so we use a
  // background task with a channel to process the frames so that the server can keep sending data even if the
  // stream consumer slows down processing the frames.
  let (tx, rx) = unbounded_channel();
  let _ = tokio::spawn(async move {
    process_stream(&inner, tx, connection).await;
  });

  Ok(UnboundedReceiverStream::new(rx))
}
