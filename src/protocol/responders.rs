use crate::error::RedisError;
use crate::interfaces::Resp3Frame;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::RedisCommand;
use crate::protocol::command::ResponseSender;
use crate::protocol::connection::SharedBuffer;
use crate::protocol::types::{KeyScanInner, ValueScanInner};
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

// TODO reconnect logic needs to select() on a second ft from quit(), shutdown(), etc via the close notifications
// the command function needs to emit this before sending the command in the queue
// need to decide how to handle situation where the connection dies, reconnect policy calls sleep(), some
// commands are queued, then quit is called. should the client wait to reconnect, then try those commands,
// then quit on the new connection? or should quit() cancel reconnection attempts and all queued messages?

// TODO change on_message to return BroadcastReceiver, move to pubsub interface trait
// TODO change on_error/reconnect/connect to return BroadcastReceivers, move to ClientLike trait

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
  /// Handle the response as a page of key/value pairs from a HSCAN, SSCAN, ZSCAN command.
  // TODO add args and any other shared state (like args) to this
  ValueScan(ValueScanInner),
  /// Handle the response as a page of keys from a SCAN command.
  KeyScan(KeyScanInner),
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

  /// Take the oneshot response sender.
  pub fn take_response_tx(&mut self) -> Option<ResponseSender> {
    match self {
      ResponseKind::Respond(tx) => tx.take(),
      ResponseKind::Buffer { tx, .. } => tx.lock().take(),
      ResponseKind::Multiple { tx, .. } => tx.lock().take(),
      _ => None,
    }
  }

  /// Respond with an error to the caller.
  pub fn respond_with_error(&mut self, error: RedisError) {
    if let Some(tx) = self.take_response_tx() {
      let _ = tx.send(Err(error));
    }
  }
}

// TODO change these to take last_command
// the outer handle_command function should pop off the front
// these should return Option<RedisCommand> and the outer fn should put it back if needed

// when processing an all_nodes response the command should not be put back in the buffer

// TODO how to figure out which of these to call without taking a lock on the command buffer?

fn process_multiple(
  inner: &Arc<RedisClientInner>,
  last_command: RedisCommand,
  expected: usize,
  received: &Arc<AtomicUsize>,
  tx: &Arc<Mutex<Option<ResponseSender>>>,
) -> Result<Option<RedisCommand>, RedisError> {
  unimplemented!()
}

fn process_buffer(
  inner: &Arc<RedisClientInner>,
  last_command: Option<RedisCommand>,
  expected: usize,
  frames: &Arc<Mutex<VecDeque<Resp3Frame>>>,
  tx: &Arc<Mutex<Option<ResponseSender>>>,
) -> Result<Option<RedisCommand>, RedisError> {
  unimplemented!()
}

fn process_respond(
  inner: &Arc<RedisClientInner>,
  last_command: RedisCommand,
  tx: &mut Option<ResponseSender>,
) -> Result<Option<RedisCommand>, RedisError> {
  unimplemented!()
}

fn process_skip(
  inner: &Arc<RedisClientInner>,
  buffer: &Arc<Mutex<VecDeque<RedisCommand>>>,
) -> Result<(), RedisError> {
  unimplemented!()
}

pub mod utils {
  use super::*;
  use crate::prelude::Resp3Frame;

  pub fn should_reconnect(inner: &Arc<RedisClientInner>, error: &RedisError) -> bool {
    inner.policy.read().is_some() && !error.is_canceled()
  }

  pub fn should_sync(inner: &Arc<RedisClientInner>, error: &RedisError) -> bool {
    inner.config.server.is_clustered() && error.is_cluster_error()
  }

  pub async fn take_last_command(buffer: &SharedBuffer) -> Option<RedisCommand> {
    buffer.lock().await.pop_front()
  }

  pub async fn replace_last_command(buffer: &SharedBuffer, command: RedisCommand) {
    buffer.lock().await.push_front(command);
  }

  pub fn is_pubsub(frame: &Resp3Frame) -> bool {
    unimplemented!()
  }
}

pub mod default {
  use super::*;
  use crate::protocol::connection::{Counters, SharedBuffer, SplitRedisStream};
  use arcstr::ArcStr;
  use futures::stream::{Stream, StreamExt};
  use std::sync::atomic::AtomicBool;

  /// Process the frame in the context of the command at the front of the queue.
  ///
  /// Returns the command if it should be put back at the front of the queue.
  async fn process_command(
    inner: &Arc<RedisClientInner>,
    server: &ArcStr,
    counters: &Counters,
    command: RedisCommand,
    frame: Resp3Frame,
  ) -> Result<Option<RedisCommand>, RedisError> {
    unimplemented!()
  }

  async fn process_pubsub(
    inner: &Arc<RedisClientInner>,
    server: &ArcStr,
    counters: &Counters,
    frame: Resp3Frame,
  ) -> Result<(), RedisError> {
    unimplemented!()
  }

  pub async fn run<T>(
    inner: Arc<RedisClientInner>,
    mut stream: SplitRedisStream<T>,
    server: ArcStr,
    buffer: SharedBuffer,
    counters: Counters,
    all_connected: Arc<AtomicBool>,
  ) -> Result<(), RedisError> {
    loop {
      let frame = match stream.next().await {
        Ok(Some(frame)) => frame.into_resp3(),
        Ok(None) => {
          // TODO stream closed
          unimplemented!()
        },
        Err(e) => {
          // TODO IO error
          unimplemented!()
        },
      };

      // TODO think through how multiple consecutive MOVED/ASK errors should be handled

      if utils::is_pubsub(frame) {
        if let Err(e) = process_pubsub(&inner, &server, &counters, frame).await {
          _warn!(inner, "{}: Failed processing pubsub message: {:?}", server, e);
        }
      } else {
        let command = match utils::take_last_command(&buffer).await {
          Some(command) => command,
          None => {
            // TODO need a better error message here
            unimplemented!()
          },
        };

        if let Some(command) = process_command(&inner, &server, &counters, command, frame).await? {
          utils::replace_last_command(&buffer, command).await;
        }
      }
    }

    // TODO:
    // set some state on the writer to start queueing messages, if not already
    // decide whether to reconnect
    // check error to see if it's canceled, return Ok(())

    unimplemented!()
  }
}
