use crate::error::RedisError;
use crate::interfaces::Resp3Frame;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::RedisCommand;
use crate::protocol::command::ResponseSender;
use crate::utils as client_utils;
use parking_lot::Mutex;
use std::collections::vec_deque::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

// TODO change ArcSwap<Arc<T>> to ArcSwap<T>
// TODO make sure should_pipeline flushes the socket on EXEC/DISCARD or if multiplexer is blocked

// TODO move should_pipeline to RedisCommandKind with override arguments
// TODO create a new backpressure policy to block the connection until all responses are received.

// TODO
// impl the transaction interface the same way as a pipeline, but with added checks to store the hash slot
// update docs to make it clear that transactions are pipelined unless abort_on_error is passed
// in which case don't pipeline it, and use the Multiple policy and override pipeline to be false
// this greatly simplifies the cluster slot checks in transactions

// TODO put an atomicbool on the multiplexer, shared with each connection to indicate reader state.
// before writing a command check that value and queue commands instead if needed.
// assumes a reconnect or close command will arrive later
// if the writer was dropped first then the reader should detect this.
// do not send sync or reconnect if the multiplixer rx is on inner (indicates quit was called)

// TODO
// make a ReplicaClient struct that has an added field for replica state
// override send_command on this struct to add a replica flag on the command before sending it
// put all this behind a feature flag
// add cluster support behind the ff that looks up replicas on reads if possible
// defer to the caller to only use read commands
// put a note in the docs about consistency checks with WAIT, etc
// put a `replicas(&self)` function on RedisClient that shares the underlying connections
// put checks in here so it only works on clustered clients with the FF set

// TODO
// multiplexer stream logic needs to change:
// * handle _Reconnect and _Sync
// * reorder functions to reader can detect that the connection was intentionally closed via the inner.multiplexer_rx field
// * implement cluster sync logic
// * implement shared cluster socket flushing
// * change reconnect logic on write failure
//   * dont emit message, drop the writer and queue the command
//   * handle messages in the queue recv after the writer is dropped but before _Reconnect arrives

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResponseKind {
  /// Throw away the response frame without modifying the command buffer.
  Requeue,
  /// Throw away the response frame and last command in the command buffer.
  ///
  /// Equivalent to `Respond(None)`.
  Skip,
  /// Respond to the caller of the last command with the response frame.
  Respond(Option<ResponseSender>),
  /// Associates multiple responses with one command, throwing away all but the last response.
  ///
  /// Typically used by commands sent to all nodes in a cluster where responses arrive on different connections concurrently.
  Multiple {
    /// The number of expected response frames.
    expected: usize,
    /// The number of response frames received.
    received: Arc<AtomicUsize>,
    /// A shared oneshot sender to the caller.
    tx: Arc<Mutex<Option<ResponseSender>>>,
  },
  /// Buffer multiple response frames until the expected number of frames are received, then respond with an array to the caller.
  Buffer {
    /// A shared buffer for response frames.
    frames: Arc<Mutex<VecDeque<Resp3Frame>>>,
    /// The expected number of response frames.
    expected: usize,
    /// A shared oneshot channel to the caller.
    tx: Arc<Mutex<Option<ResponseSender>>>,
  },
}

impl ResponseKind {
  pub fn new_buffer(expected: usize, tx: ResponseSender) -> Self {
    ResponseKind::Buffer {
      frames: Arc::new(Mutex::new(VecDeque::new())),
      tx: Arc::new(Mutex::new(Some(tx))),
      expected,
    }
  }

  pub fn new_multiple(expected: usize, tx: ResponseSender) -> Self {
    ResponseKind::Multiple {
      received: Arc::new(AtomicUsize::new(0)),
      tx: Arc::new(Mutex::new(Some(tx))),
      expected,
    }
  }

  /// The expected number of remaining frames to be read from the server.
  pub fn remaining(&self) -> usize {
    match self {
      ResponseKind::Buffer { expected, frames, .. } => expected - frames.lock().len(),
      ResponseKind::Multiple { expected, received, .. } => expected - client_utils::read_atomic(received),
      ResponseKind::Requeue | ResponseKind::Skip => 0,
      ResponseKind::Respond(_) => 1,
    }
  }

  /// Errors returned here close the connection.
  pub fn handle_response(
    &mut self,
    inner: &Arc<RedisClientInner>,
    buffer: &Arc<Mutex<VecDeque<RedisCommand>>>,
  ) -> Result<(), RedisError> {
    match self {
      ResponseKind::Respond(tx) => process_respond(inner, buffer, tx),
      ResponseKind::Multiple { tx, expected, received } => process_multiple(inner, buffer, *expected, received, tx),
      ResponseKind::Buffer { tx, expected, frames } => process_buffer(inner, buffer, *expected, frames, tx),
      ResponseKind::Skip => process_skip(inner, buffer),
      ResponseKind::Requeue => {
        _debug!(inner, "Skip response without modifying command queue.");
      },
    }
  }

  /// Take the oneshot response sender.
  pub fn response_tx(&mut self) -> Option<ResponseSender> {
    match self {
      ResponseKind::Respond(tx) => tx.take(),
      ResponseKind::Buffer { tx, .. } => tx.lock().take(),
      ResponseKind::Multiple { tx, .. } => tx.lock().take(),
      _ => None,
    }
  }

  /// Respond with an error to the caller.
  pub fn respond_with_error(&mut self, error: RedisError) {
    if let Some(tx) = self.response_tx() {
      let _ = tx.send(Err(error));
    }
  }
}

fn process_multiple(
  inner: &Arc<RedisClientInner>,
  buffer: &Arc<Mutex<VecDeque<RedisCommand>>>,
  expected: usize,
  received: &Arc<AtomicUsize>,
  tx: &Arc<Mutex<Option<ResponseSender>>>,
) -> Result<(), RedisError> {
  unimplemented!()
}

fn process_buffer(
  inner: &Arc<RedisClientInner>,
  buffer: &Arc<Mutex<VecDeque<RedisCommand>>>,
  expected: usize,
  frames: &Arc<Mutex<VecDeque<Resp3Frame>>>,
  tx: &Arc<Mutex<Option<ResponseSender>>>,
) -> Result<(), RedisError> {
  unimplemented!()
}

fn process_respond(
  inner: &Arc<RedisClientInner>,
  buffer: &Arc<Mutex<VecDeque<RedisCommand>>>,
  tx: &mut Option<ResponseSender>,
) -> Result<(), RedisError> {
  unimplemented!()
}

fn process_skip(
  inner: &Arc<RedisClientInner>,
  buffer: &Arc<Mutex<VecDeque<RedisCommand>>>,
) -> Result<(), RedisError> {
  unimplemented!()
}
