use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind, ResponseSender, RouterResponse},
    types::{KeyScanInner, Server, ValueScanInner, ValueScanResult},
    utils as protocol_utils,
  },
  types::{HScanResult, RedisKey, RedisValue, SScanResult, ScanResult, ZScanResult},
  utils as client_utils,
};
use bytes_utils::Str;
use parking_lot::Mutex;
use std::{
  fmt,
  fmt::Formatter,
  iter::repeat,
  ops::DerefMut,
  sync::{atomic::AtomicUsize, Arc},
};

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
#[cfg(feature = "metrics")]
use parking_lot::RwLock;
#[cfg(feature = "metrics")]
use std::{cmp, time::Instant};

const LAST_CURSOR: &'static str = "0";

pub enum ResponseKind {
  /// Throw away the response frame and last command in the command buffer.
  ///
  /// Note: The reader task will still unblock the router, if specified.
  ///
  /// Equivalent to `Respond(None)`.
  Skip,
  /// Respond to the caller of the last command with the response frame.
  Respond(Option<ResponseSender>),
  /// Associates multiple responses with one command, throwing away all but the last response.
  ///
  /// Note: This must not be shared across multiple commands. Use `Buffer` instead.
  ///
  /// `PSUBSCRIBE` and `PUNSUBSCRIBE` return multiple top level response frames on the same connection to the
  /// command. This requires unique response handling logic to re-queue the command at the front of the shared
  /// command buffer until the expected number of frames are received.
  // FIXME change this interface so it wont compile if this is shared across commands
  Multiple {
    /// The number of expected response frames.
    expected: usize,
    /// The number of response frames received.
    received: Arc<AtomicUsize>,
    /// A shared oneshot sender to the caller.
    tx:       Arc<Mutex<Option<ResponseSender>>>,
  },
  /// Buffer multiple response frames until the expected number of frames are received, then respond with an array to
  /// the caller.
  ///
  /// Typically used in `*_cluster` commands or to handle concurrent responses in a `Pipeline` that may span multiple
  /// cluster connections.
  Buffer {
    /// A shared buffer for response frames.
    frames:      Arc<Mutex<Vec<Resp3Frame>>>,
    /// The expected number of response frames.
    expected:    usize,
    /// The number of response frames received.
    received:    Arc<AtomicUsize>,
    /// A shared oneshot channel to the caller.
    tx:          Arc<Mutex<Option<ResponseSender>>>,
    /// A local field for tracking the expected index of the response in the `frames` array.
    index:       usize,
    /// Whether errors should be returned early to the caller.
    error_early: bool,
  },
  /// Handle the response as a page of key/value pairs from a HSCAN, SSCAN, ZSCAN command.
  ValueScan(ValueScanInner),
  /// Handle the response as a page of keys from a SCAN command.
  KeyScan(KeyScanInner),
}

impl fmt::Debug for ResponseKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", match self {
      ResponseKind::Skip => "Skip",
      ResponseKind::Buffer { .. } => "Buffer",
      ResponseKind::Multiple { .. } => "Multiple",
      ResponseKind::Respond(_) => "Respond",
      ResponseKind::KeyScan(_) => "KeyScan",
      ResponseKind::ValueScan(_) => "ValueScan",
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
        index:       index.clone(),
        expected:    expected.clone(),
        error_early: error_early.clone(),
      },
      ResponseKind::Multiple { received, tx, expected } => ResponseKind::Multiple {
        received: received.clone(),
        tx:       tx.clone(),
        expected: expected.clone(),
      },
      ResponseKind::KeyScan(_) | ResponseKind::ValueScan(_) => return None,
    })
  }

  pub fn set_expected_index(&mut self, idx: usize) {
    if let ResponseKind::Buffer { ref mut index, .. } = self {
      *index = idx;
    }
  }

  pub fn set_error_early(&mut self, _error_early: bool) {
    if let ResponseKind::Buffer {
      ref mut error_early, ..
    } = self
    {
      *error_early = _error_early;
    }
  }

  pub fn new_buffer(tx: ResponseSender) -> Self {
    ResponseKind::Buffer {
      frames:      Arc::new(Mutex::new(vec![])),
      tx:          Arc::new(Mutex::new(Some(tx))),
      received:    Arc::new(AtomicUsize::new(0)),
      index:       0,
      expected:    0,
      error_early: true,
    }
  }

  pub fn new_buffer_with_size(expected: usize, tx: ResponseSender) -> Self {
    let frames = repeat(Resp3Frame::Null).take(expected).collect();
    ResponseKind::Buffer {
      frames: Arc::new(Mutex::new(frames)),
      tx: Arc::new(Mutex::new(Some(tx))),
      received: Arc::new(AtomicUsize::new(0)),
      index: 0,
      error_early: true,
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

  /// Clone the shared response sender for `Buffer` or `Multiple` variants.
  pub fn clone_shared_response_tx(&self) -> Option<Arc<Mutex<Option<ResponseSender>>>> {
    match self {
      ResponseKind::Buffer { tx, .. } => Some(tx.clone()),
      ResponseKind::Multiple { tx, .. } => Some(tx.clone()),
      _ => None,
    }
  }

  /// Respond with an error to the caller.
  pub fn respond_with_error(&mut self, error: RedisError) {
    if let Some(tx) = self.take_response_tx() {
      let _ = tx.send(Err(error));
    }
  }

  /// Read the number of expected response frames.
  pub fn expected_response_frames(&self) -> usize {
    match self {
      ResponseKind::Skip | ResponseKind::Respond(_) => 1,
      ResponseKind::Multiple { ref expected, .. } => *expected,
      ResponseKind::Buffer { ref expected, .. } => *expected,
      ResponseKind::ValueScan(_) | ResponseKind::KeyScan(_) => 1,
    }
  }
}

#[cfg(feature = "metrics")]
fn sample_latency(latency_stats: &RwLock<MovingStats>, sent: Instant) {
  let dur = Instant::now().duration_since(sent);
  let dur_ms = cmp::max(0, (dur.as_secs() * 1000) + dur.subsec_millis() as u64) as i64;
  latency_stats.write().sample(dur_ms);
}

/// Sample overall and network latency values for a command.
#[cfg(feature = "metrics")]
fn sample_command_latencies(inner: &Arc<RedisClientInner>, command: &mut RedisCommand) {
  if let Some(sent) = command.network_start.take() {
    sample_latency(&inner.network_latency_stats, sent);
  }
  sample_latency(&inner.latency_stats, command.created.clone());
}

#[cfg(not(feature = "metrics"))]
fn sample_command_latencies(_: &Arc<RedisClientInner>, _: &mut RedisCommand) {}

/// Update the client's protocol version codec version after receiving a non-error response to HELLO.
fn update_protocol_version(inner: &Arc<RedisClientInner>, command: &RedisCommand, frame: &Resp3Frame) {
  if !frame.is_error() {
    let version = match command.kind {
      RedisCommandKind::_Hello(ref version) => version,
      RedisCommandKind::_HelloAllCluster(ref version) => version,
      _ => return,
    };

    _debug!(inner, "Changing RESP version to {:?}", version);
    // HELLO cannot be pipelined so this is safe
    inner.switch_protocol_versions(version.clone());
  }
}

fn respond_locked(
  inner: &Arc<RedisClientInner>,
  tx: &Arc<Mutex<Option<ResponseSender>>>,
  result: Result<Resp3Frame, RedisError>,
) {
  if let Some(tx) = tx.lock().take() {
    if let Err(_) = tx.send(result) {
      _debug!(inner, "Error responding to caller.");
    }
  }
}

fn add_buffered_frame(
  server: &Server,
  buffer: &Arc<Mutex<Vec<Resp3Frame>>>,
  index: usize,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  let mut guard = buffer.lock();
  let buffer_ref = guard.deref_mut();

  if index >= buffer_ref.len() {
    debug!(
      "({}) Unexpected buffer response array index: {}, len: {}",
      server,
      index,
      buffer_ref.len()
    );
    return Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Invalid buffer response index.",
    ));
  }

  trace!(
    "({}) Add buffered frame {:?} at index {} with length {}",
    server,
    frame.kind(),
    index,
    buffer_ref.len()
  );
  buffer_ref[index] = frame;
  Ok(())
}

/// Merge multiple potentially nested frames into one flat array of frames.
fn merge_multiple_frames(frames: &mut Vec<Resp3Frame>, error_early: bool) -> Resp3Frame {
  let inner_len = frames.iter().fold(0, |count, frame| {
    count
      + match frame {
        Resp3Frame::Array { ref data, .. } => data.len(),
        Resp3Frame::Push { ref data, .. } => data.len(),
        _ => 1,
      }
  });

  let mut out = Vec::with_capacity(inner_len);
  for frame in frames.drain(..) {
    // unwrap and return errors early
    if error_early && frame.is_error() {
      return frame;
    }

    match frame {
      Resp3Frame::Array { data, .. } | Resp3Frame::Push { data, .. } => {
        for inner_frame in data.into_iter() {
          out.push(inner_frame);
        }
      },
      _ => out.push(frame),
    };
  }

  Resp3Frame::Array {
    data:       out,
    attributes: None,
  }
}

/// Parse the output of a command that scans keys.
fn parse_key_scan_frame(frame: Resp3Frame) -> Result<(Str, Vec<RedisKey>), RedisError> {
  if let Resp3Frame::Array { mut data, .. } = frame {
    if data.len() == 2 {
      let cursor = match protocol_utils::frame_to_str(&data[0]) {
        Some(s) => s,
        None => {
          return Err(RedisError::new(
            RedisErrorKind::Protocol,
            "Expected first SCAN result element to be a bulk string.",
          ))
        },
      };

      if let Some(Resp3Frame::Array { data, .. }) = data.pop() {
        let mut keys = Vec::with_capacity(data.len());

        for frame in data.into_iter() {
          let key = match protocol_utils::frame_to_bytes(&frame) {
            Some(s) => s,
            None => {
              return Err(RedisError::new(
                RedisErrorKind::Protocol,
                "Expected an array of strings from second SCAN result.",
              ))
            },
          };

          keys.push(key.into());
        }

        Ok((cursor, keys))
      } else {
        Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Expected second SCAN result element to be an array.",
        ))
      }
    } else {
      Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Expected two-element bulk string array from SCAN.",
      ))
    }
  } else {
    Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Expected bulk string array from SCAN.",
    ))
  }
}

/// Parse the output of a command that scans values.
fn parse_value_scan_frame(frame: Resp3Frame) -> Result<(Str, Vec<RedisValue>), RedisError> {
  if let Resp3Frame::Array { mut data, .. } = frame {
    if data.len() == 2 {
      let cursor = match protocol_utils::frame_to_str(&data[0]) {
        Some(s) => s,
        None => {
          return Err(RedisError::new(
            RedisErrorKind::Protocol,
            "Expected first result element to be a bulk string.",
          ))
        },
      };

      if let Some(Resp3Frame::Array { data, .. }) = data.pop() {
        let mut values = Vec::with_capacity(data.len());

        for frame in data.into_iter() {
          values.push(protocol_utils::frame_to_single_result(frame)?);
        }

        Ok((cursor, values))
      } else {
        Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Expected second result element to be an array.",
        ))
      }
    } else {
      Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Expected two-element bulk string array.",
      ))
    }
  } else {
    Err(RedisError::new(RedisErrorKind::Protocol, "Expected bulk string array."))
  }
}

/// Send the output to the caller of a command that scans values.
fn send_value_scan_result(
  inner: &Arc<RedisClientInner>,
  scanner: ValueScanInner,
  command: &RedisCommand,
  result: Vec<RedisValue>,
  can_continue: bool,
) -> Result<(), RedisError> {
  match command.kind {
    RedisCommandKind::Zscan => {
      let tx = scanner.tx.clone();
      let results = ValueScanInner::transform_zscan_result(result)?;

      let state = ValueScanResult::ZScan(ZScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state: scanner,
        results: Some(results),
      });

      if let Err(_) = tx.send(Ok(state)) {
        _warn!(inner, "Failed to send ZSCAN result to caller");
      }
    },
    RedisCommandKind::Sscan => {
      let tx = scanner.tx.clone();

      let state = ValueScanResult::SScan(SScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state: scanner,
        results: Some(result),
      });

      if let Err(_) = tx.send(Ok(state)) {
        _warn!(inner, "Failed to send SSCAN result to caller");
      }
    },
    RedisCommandKind::Hscan => {
      let tx = scanner.tx.clone();
      let results = ValueScanInner::transform_hscan_result(result)?;

      let state = ValueScanResult::HScan(HScanResult {
        can_continue,
        inner: inner.clone(),
        scan_state: scanner,
        results: Some(results),
      });

      if let Err(_) = tx.send(Ok(state)) {
        _warn!(inner, "Failed to send HSCAN result to caller");
      }
    },
    _ => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Invalid redis command. Expected HSCAN, SSCAN, or ZSCAN.",
      ))
    },
  };

  Ok(())
}

/// Respond to the caller with the default response policy.
pub fn respond_to_caller(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  mut command: RedisCommand,
  tx: ResponseSender,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  sample_command_latencies(inner, &mut command);
  _trace!(
    inner,
    "Respond to caller from {} for {} with {:?}",
    server,
    command.kind.to_str_debug(),
    frame.kind()
  );
  if command.kind.is_hello() {
    update_protocol_version(inner, &command, &frame);
  }

  let _ = tx.send(Ok(frame));
  command.respond_to_router(inner, RouterResponse::Continue);
  Ok(())
}

/// Respond to the caller, assuming multiple response frames from the last command.
///
/// This interface may return the last command to be put back at the front of the shared buffer if more responses are
/// expected.
pub fn respond_multiple(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  mut command: RedisCommand,
  received: Arc<AtomicUsize>,
  expected: usize,
  tx: Arc<Mutex<Option<ResponseSender>>>,
  frame: Resp3Frame,
) -> Result<Option<RedisCommand>, RedisError> {
  _trace!(
    inner,
    "Handling `multiple` response from {} for {}",
    server,
    command.kind.to_str_debug()
  );
  if frame.is_error() {
    // respond early to callers if an error is received from any of the commands
    command.respond_to_router(inner, RouterResponse::Continue);

    let result = Err(protocol_utils::frame_to_error(&frame).unwrap_or(RedisError::new_canceled()));
    respond_locked(inner, &tx, result);
  } else {
    let recv = client_utils::incr_atomic(&received);

    if recv == expected {
      command.respond_to_router(inner, RouterResponse::Continue);

      if command.kind.is_hello() {
        update_protocol_version(inner, &command, &frame);
      }

      _trace!(
        inner,
        "Finishing `multiple` response from {} for {}",
        server,
        command.kind.to_str_debug()
      );
      respond_locked(inner, &tx, Ok(frame));
    } else {
      // more responses are expected
      _trace!(
        inner,
        "Waiting on {} more responses to `multiple` command",
        expected - recv
      );
      // do not unblock the router here

      // need to reconstruct the responder state
      command.response = ResponseKind::Multiple { tx, received, expected };
      return Ok(Some(command));
    }
  }

  Ok(None)
}

/// Respond to the caller, assuming multiple response frames from the last command, storing intermediate responses in
/// the shared buffer.
pub fn respond_buffer(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  command: RedisCommand,
  received: Arc<AtomicUsize>,
  expected: usize,
  error_early: bool,
  frames: Arc<Mutex<Vec<Resp3Frame>>>,
  index: usize,
  tx: Arc<Mutex<Option<ResponseSender>>>,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(
    inner,
    "Handling `buffer` response from {} for {}. Is error: {}, Index: {}, ID: {}",
    server,
    command.kind.to_str_debug(),
    frame.is_error(),
    index,
    command.debug_id()
  );

  // errors are buffered like normal frames and are not returned early
  if let Err(e) = add_buffered_frame(&server, &frames, index, frame) {
    respond_locked(inner, &tx, Err(e));
    command.respond_to_router(inner, RouterResponse::Continue);
    _error!(
      inner,
      "Exiting early after unexpected buffer response index from {} with command {}, ID {}",
      server,
      command.kind.to_str_debug(),
      command.debug_id()
    );
    return Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Invalid buffer response index.",
    ));
  }

  // this must come after adding the buffered frame. there's a potential race condition if this task is interrupted
  // due to contention on the frame lock and another parallel task moves past the `received==expected` check before
  // this task can add the frame to the buffer.
  let received = client_utils::incr_atomic(&received);
  if received == expected {
    _trace!(
      inner,
      "Responding to caller after last buffered response from {}, ID: {}",
      server,
      command.debug_id()
    );

    let frame = merge_multiple_frames(frames.lock().deref_mut(), error_early);
    if frame.is_error() {
      let err = match frame.as_str() {
        Some(s) => protocol_utils::pretty_error(s),
        None => RedisError::new(
          RedisErrorKind::Unknown,
          "Unknown or invalid error from buffered frames.",
        ),
      };

      respond_locked(inner, &tx, Err(err));
    } else {
      respond_locked(inner, &tx, Ok(frame));
    }
    command.respond_to_router(inner, RouterResponse::Continue);
  } else {
    // more responses are expected
    _trace!(
      inner,
      "Waiting on {} more responses to all nodes command, ID: {}",
      expected - received,
      command.debug_id()
    );
    // this response type is shared across connections so we do not return the command to be re-queued
  }

  Ok(())
}

/// Respond to the caller of a key scanning operation.
pub fn respond_key_scan(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  command: RedisCommand,
  mut scanner: KeyScanInner,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(
    inner,
    "Handling `KeyScan` response from {} for {}",
    server,
    command.kind.to_str_debug()
  );
  let (next_cursor, keys) = match parse_key_scan_frame(frame) {
    Ok(result) => result,
    Err(e) => {
      scanner.send_error(e);
      command.respond_to_router(inner, RouterResponse::Continue);
      return Ok(());
    },
  };
  let scan_stream = scanner.tx.clone();
  let can_continue = next_cursor != LAST_CURSOR;
  scanner.update_cursor(next_cursor);
  command.respond_to_router(inner, RouterResponse::Continue);

  let scan_result = ScanResult {
    scan_state: scanner,
    inner: inner.clone(),
    results: Some(keys),
    can_continue,
  };
  if let Err(_) = scan_stream.send(Ok(scan_result)) {
    _debug!(inner, "Error sending SCAN page.");
  }

  Ok(())
}

/// Respond to the caller of a value scanning operation.
pub fn respond_value_scan(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  command: RedisCommand,
  mut scanner: ValueScanInner,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(
    inner,
    "Handling `ValueScan` response from {} for {}",
    server,
    command.kind.to_str_debug()
  );

  let (next_cursor, values) = match parse_value_scan_frame(frame) {
    Ok(result) => result,
    Err(e) => {
      scanner.send_error(e);
      command.respond_to_router(inner, RouterResponse::Continue);
      return Ok(());
    },
  };
  let scan_stream = scanner.tx.clone();
  let can_continue = next_cursor != LAST_CURSOR;
  scanner.update_cursor(next_cursor);
  command.respond_to_router(inner, RouterResponse::Continue);

  _trace!(inner, "Sending value scan result with {} values", values.len());
  if let Err(e) = send_value_scan_result(inner, scanner, &command, values, can_continue) {
    if let Err(_) = scan_stream.send(Err(e)) {
      _warn!(inner, "Error sending scan result.");
    }
  }

  Ok(())
}
