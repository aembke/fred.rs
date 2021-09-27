#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
use crate::modules::inner::RedisClientInner;
#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
use crate::protocol::types::RedisCommand;
#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
use redis_protocol::resp2::types::Frame;
#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
use std::sync::Arc;

/// Fake span for mocking tracing functions.
#[cfg(not(feature = "full-tracing"))]
pub struct Span {}

#[cfg(not(feature = "full-tracing"))]
impl Span {
  pub fn enter(&self) -> () {
    ()
  }

  pub fn record<Q: ?Sized, V: ?Sized>(&self, _field: &Q, _value: &V) -> &Self {
    &self
  }
}

#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
pub fn set_network_span(_command: &mut RedisCommand, _flush: bool) {}

#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
pub fn create_pubsub_span(_inner: &Arc<RedisClientInner>, _frame: &Frame) -> Span {
  Span {}
}

#[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
pub fn backpressure_event(_cmd: &RedisCommand, _duration: u128) {}
