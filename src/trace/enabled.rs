use crate::modules::inner::RedisClientInner;
use crate::protocol::types::RedisCommand;
use crate::protocol::utils as protocol_utils;
use crate::utils;
use redis_protocol::resp2::types::Frame;
use std::fmt;
use std::sync::Arc;
use tracing::event;
use tracing::field::Empty;
pub use tracing::span::Span;
use tracing::{span, Id as TraceId, Level};

#[cfg(not(feature = "full-tracing"))]
use crate::trace::disabled::Span as FakeSpan;

/// Struct for storing spans used by the client when sending a command.
pub struct CommandTraces {
  pub cmd_id: Option<TraceId>,
  pub network: Option<Span>,
  #[cfg(feature = "full-tracing")]
  pub queued: Option<Span>,
  #[cfg(not(feature = "full-tracing"))]
  pub queued: Option<FakeSpan>,
}

/// Enter the network span when the command is dropped after receiving a response.
impl Drop for CommandTraces {
  fn drop(&mut self) {
    if let Some(span) = self.network.take() {
      let _enter = span.enter();
    }
  }
}

impl Default for CommandTraces {
  fn default() -> Self {
    CommandTraces {
      cmd_id: None,
      queued: None,
      network: None,
    }
  }
}

impl fmt::Debug for CommandTraces {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Command Traces]")
  }
}

pub fn set_network_span(command: &mut RedisCommand, flush: bool) {
  let span = fspan!(command, "wait_for_response", flush);
  let _ = span.in_scope(|| {});
  command.traces.network = Some(span);
}

pub fn record_response_size(span: &Span, frame: &Frame) {
  span.record("res_size", &protocol_utils::frame_size(frame));
}

pub fn create_command_span(inner: &Arc<RedisClientInner>) -> Span {
  span!(
    Level::INFO,
    "redis_command",
    module = "fred",
    client_id = inner.id.as_str(),
    cmd = Empty,
    key = Empty,
    req_size = Empty,
    res_size = Empty
  )
}

#[cfg(feature = "full-tracing")]
pub fn create_args_span(parent: Option<TraceId>) -> Span {
  span!(parent: parent, Level::DEBUG, "prepare_args", num_args = Empty)
}

#[cfg(not(feature = "full-tracing"))]
pub fn create_args_span(_parent: Option<TraceId>) -> FakeSpan {
  FakeSpan {}
}

#[cfg(feature = "full-tracing")]
pub fn create_queued_span(parent: Option<TraceId>, inner: &Arc<RedisClientInner>) -> Span {
  let buf_len = utils::read_atomic(&inner.cmd_buffer_len);
  span!(parent: parent, Level::DEBUG, "queued", buf_len)
}

#[cfg(not(feature = "full-tracing"))]
pub fn create_queued_span(_parent: Option<TraceId>, _inner: &Arc<RedisClientInner>) -> FakeSpan {
  FakeSpan {}
}

#[cfg(feature = "full-tracing")]
pub fn create_pubsub_span(inner: &Arc<RedisClientInner>, frame: &Frame) -> Span {
  span!(
    parent: None,
    Level::INFO,
    "parse_pubsub",
    module = "fred",
    client_id = &inner.id.as_str(),
    res_size = &protocol_utils::frame_size(frame),
    channel = Empty
  )
}

#[cfg(not(feature = "full-tracing"))]
pub fn create_pubsub_span(_inner: &Arc<RedisClientInner>, _frame: &Frame) -> FakeSpan {
  FakeSpan {}
}

pub fn backpressure_event(cmd: &RedisCommand, duration: u128) {
  event!(parent: cmd.traces.cmd_id.clone(), Level::INFO, "backpressure duration_ms={}", duration);
}
