use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::Resp3Frame;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::ResponseKind::Respond;
use crate::protocol::connection::{SentCommand, SharedBuffer};
use crate::protocol::hashers::ClusterHash;
use crate::protocol::types::ProtocolFrame;
use crate::trace::CommandTraces;
use crate::types::RedisValue;
use crate::utils as client_utils;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use semver::Op;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot::Sender as OneshotSender;

type ResponseSender = OneshotSender<Result<Resp3Frame, RedisError>>;
type MultiplexerSender = OneshotSender<()>;
// TODO change ArcSwap<Arc<T>> to ArcSwap<T>

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
  /// Typically used by commands sent to all nodes in a cluster and can be shared across tasks or threads.
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

// TODO
// impl the transaction interface the same way as a pipeline, but with added checks to store the hash slot
// update docs to make it clear that transactions are pipelined unless abort_on_error is passed
// in which case don't pipeline it, and use the Multiple policy and override pipeline to be false
// this greatly simplifies the cluster slot checks in transactions

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
    buffer: &Arc<Mutex<VecDeque<SentCommand>>>,
  ) -> Result<(), RedisError> {
    unimplemented!()
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

// TODO make sure should_pipeline flushes the socket on EXEC/DISCARD or if multiplexer is blocked

pub enum RedisCommandKind {
  Ping,
  Get,
}

// TODO move should_pipeline to RedisCommandKind with override arguments

pub struct RedisCommand {
  /// The command and optional subcommand name.
  pub kind: RedisCommandKind,
  /// The policy to apply when handling the response.
  pub response: ResponseKind,
  /// The policy to use when hashing the arguments for cluster routing.
  pub hasher: ClusterHash,
  /// The provided arguments.
  pub args: Vec<RedisValue>,
  /// A oneshot sender used to communicate with the multiplexer.
  pub multiplexer_tx: Option<MultiplexerSender>,
  /// The number of times the command was sent to the server.
  pub attempted: usize,
  /// Whether or not the command can be pipelined.
  pub can_pipeline: bool,
  #[cfg(feature = "metrics")]
  pub created: Instant,
  #[cfg(feature = "partial-tracing")]
  pub traces: CommandTraces,
}

impl From<(RedisCommandKind, Vec<RedisValue>)> for RedisCommand {
  fn from((kind, args): (RedisCommandKind, Vec<RedisValue>)) -> Self {
    RedisCommand {
      kind,
      args,
      response: ResponseKind::Respond(None),
      hasher: ClusterHash::FirstKey,
      multiplexer_tx: None,
      attempted: 0,
      can_pipeline: true,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
    }
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>, ResponseSender)> for RedisCommand {
  fn from((kind, args, tx): (RedisCommandKind, Vec<RedisValue>, ResponseSender)) -> Self {
    RedisCommand {
      kind,
      args,
      response: ResponseKind::Respond(Some(tx)),
      hasher: ClusterHash::FirstKey,
      multiplexer_tx: None,
      attempted: 0,
      can_pipeline: true,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
    }
  }
}

impl RedisCommand {
  /// Increment and check the number of write attempts.
  pub fn incr_check_attempted(&mut self, max: usize) -> Result<(), RedisError> {
    self.attempted += 1;
    if self.attempted > max {
      Err(RedisError::new(RedisErrorKind::Unknown, "Max write attempts reached."))
    } else {
      Ok(())
    }
  }

  /// Unblock the multiplexer loop, if necessary.
  pub fn unblock_multiplexer(&mut self, name: &str) {
    if let Some(tx) = self.multiplexer_tx.take() {
      if tx.send(()).is_err() {
        warn!("{}: Failed to unblock multiplexer loop.", name);
      }
    }
  }
}

/// A message or pipeline queued in memory before being sent to the server.
#[derive(Debug)]
pub enum QueuedCommand {
  Command(RedisCommand),
  Pipeline(Vec<RedisCommand>),
}
