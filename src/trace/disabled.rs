use crate::client::RedisClientInner;
use crate::protocol::types::RedisCommand;
use redis_protocol::types::Frame;
use std::sync::Arc;

/// Fake span for mocking tracing functions.
pub struct Span {}

impl Span {
  pub fn enter(&self) -> () {
    ()
  }

  pub fn record<Q: ?Sized, V: ?Sized>(&self, _field: &Q, _value: &V) -> &Self {
    &self
  }
}

pub fn set_network_span(_command: &mut RedisCommand, _flush: bool) {}

pub fn create_pubsub_span(_inner: &Arc<RedisClientInner>, _frame: &Frame) -> Span {
  Span {}
}

pub fn backpressure_event(_cmd: &RedisCommand, _duration: u128) {}
