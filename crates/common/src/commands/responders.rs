use crate::{
  commands::{
    scan::{KeyScanBufferedInner, KeyScanInner, ValueScanInner},
    ResponseSender,
  },
  error::Error,
  runtime::{AtomicUsize, Mutex, RefCount},
  types::Resp3Frame,
};
use std::{fmt, fmt::Formatter};

/// An enum describing the policies by which callers can receive responses from a server.
pub enum ResponseKind {
  /// Throw away the response frame and last command in the command buffer.
  ///
  /// Equivalent to `Respond(None)`.
  Skip,
  /// Respond to the caller of the last command with the response frame.
  Respond(Option<ResponseSender>),
  /// Buffer multiple response frames until the expected number of frames are received, then respond with an array to
  /// the caller.
  ///
  /// Typically used in `*_cluster` commands or to handle concurrent responses in a `Pipeline` that may span multiple
  /// cluster connections.
  Buffer {
    /// A shared buffer for response frames.
    frames:      RefCount<Mutex<Vec<Resp3Frame>>>,
    /// The expected number of response frames.
    expected:    usize,
    /// The number of response frames received.
    received:    RefCount<AtomicUsize>,
    /// A shared oneshot channel to the caller.
    tx:          RefCount<Mutex<Option<ResponseSender>>>,
    /// A local field for tracking the expected index of the response in the `frames` array.
    ///
    /// This field is not ref counted so clones can track different expected response indexes.
    index:       usize,
    /// Whether errors should be returned early to the caller.
    error_early: bool,
  },
  /// Handle the response as a page of key/value pairs from a HSCAN, SSCAN, ZSCAN command.
  ValueScan(ValueScanInner),
  /// Handle the response as a page of keys from a SCAN command.
  KeyScan(KeyScanInner),
  /// Handle the response as a buffered key SCAN command.
  KeyScanBuffered(KeyScanBufferedInner),
}

impl fmt::Debug for ResponseKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", match self {
      ResponseKind::Skip => "Skip",
      ResponseKind::Buffer { .. } => "Buffer",
      ResponseKind::Respond(_) => "Respond",
      ResponseKind::KeyScan(_) => "KeyScan",
      ResponseKind::ValueScan(_) => "ValueScan",
      ResponseKind::KeyScanBuffered(_) => "KeyScanBuffered",
    })
  }
}

impl ResponseKind {
  /// Attempt to clone the response channel.
  ///
  /// If the channel cannot be shared or cloned (since it contains a oneshot channel) this will fall back to a `Skip`
  /// policy.
  pub fn duplicate(&self) -> Option<Self> {
    Some(match self {
      ResponseKind::Skip => ResponseKind::Skip,
      ResponseKind::Respond(_) => ResponseKind::Respond(None),
      ResponseKind::Buffer {
        frames,
        tx,
        received,
        index,
        expected,
        error_early,
      } => ResponseKind::Buffer {
        frames:      frames.clone(),
        tx:          tx.clone(),
        received:    received.clone(),
        index:       *index,
        expected:    *expected,
        error_early: *error_early,
      },
      ResponseKind::KeyScan(_) | ResponseKind::ValueScan(_) | ResponseKind::KeyScanBuffered(_) => return None,
    })
  }

  /// Set the expected index of the next frame received on the socket.
  pub fn set_expected_index(&mut self, idx: usize) {
    if let ResponseKind::Buffer { ref mut index, .. } = self {
      *index = idx;
    }
  }

  /// Set the error early flag.
  pub fn set_error_early(&mut self, _error_early: bool) {
    if let ResponseKind::Buffer {
      ref mut error_early, ..
    } = self
    {
      *error_early = _error_early;
    }
  }

  /// Create a new empty response buffer.
  pub fn new_buffer(tx: ResponseSender) -> Self {
    ResponseKind::Buffer {
      frames:      RefCount::new(Mutex::new(vec![])),
      tx:          RefCount::new(Mutex::new(Some(tx))),
      received:    RefCount::new(AtomicUsize::new(0)),
      index:       0,
      expected:    0,
      error_early: true,
    }
  }

  /// Create a new response buffer with the provided size.
  pub fn new_buffer_with_size(expected: usize, tx: ResponseSender) -> Self {
    ResponseKind::Buffer {
      frames: RefCount::new(Mutex::new(vec![Resp3Frame::Null; expected])),
      tx: RefCount::new(Mutex::new(Some(tx))),
      received: RefCount::new(AtomicUsize::new(0)),
      index: 0,
      error_early: true,
      expected,
    }
  }

  /// Take the oneshot response sender.
  pub fn take_response_tx(&mut self) -> Option<ResponseSender> {
    match self {
      ResponseKind::Respond(tx) => tx.take(),
      ResponseKind::Buffer { tx, .. } => tx.lock().take(),
      _ => None,
    }
  }

  /// Clone the shared response sender for `Buffer` or `Multiple` variants.
  pub fn clone_shared_response_tx(&self) -> Option<RefCount<Mutex<Option<ResponseSender>>>> {
    match self {
      ResponseKind::Buffer { tx, .. } => Some(tx.clone()),
      _ => None,
    }
  }

  /// Respond with an error to the caller.
  pub fn respond_with_error(&mut self, error: Error) {
    if let Some(tx) = self.take_response_tx() {
      let _ = tx.send(Err(error));
    }
  }

  /// Read the number of expected response frames.
  pub fn expected_response_frames(&self) -> usize {
    match self {
      ResponseKind::Skip | ResponseKind::Respond(_) => 1,
      ResponseKind::Buffer { ref expected, .. } => *expected,
      ResponseKind::ValueScan(_) | ResponseKind::KeyScan(_) | ResponseKind::KeyScanBuffered(_) => 1,
    }
  }

  /// Whether the responder is a `ResponseKind::Buffer`.
  pub fn is_buffer(&self) -> bool {
    matches!(self, ResponseKind::Buffer { .. })
  }
}
