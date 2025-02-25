use crate::{
  error::{Error, ErrorKind},
  runtime::Sender,
  types::{
    scan::{HScanResult, SScanResult, ScanResult, ZScanResult},
    values::{Key, Map, Value},
    Server,
  },
  utils,
};
use bytes_utils::Str;
use std::collections::HashMap;

pub struct KeyScanInner {
  /// The hash slot for the command.
  pub hash_slot:  Option<u16>,
  /// An optional server override.
  pub server:     Option<Server>,
  /// The index of the cursor in `args`.
  pub cursor_idx: usize,
  /// The arguments sent in each scan command.
  pub args:       Vec<Value>,
  /// The sender half of the results channel.
  pub tx:         Sender<Result<ScanResult, Error>>,
}

pub struct KeyScanBufferedInner {
  /// The hash slot for the command.
  pub hash_slot:  Option<u16>,
  /// An optional server override.
  pub server:     Option<Server>,
  /// The index of the cursor in `args`.
  pub cursor_idx: usize,
  /// The arguments sent in each scan command.
  pub args:       Vec<Value>,
  /// The sender half of the results channel.
  pub tx:         Sender<Result<Key, Error>>,
}

impl KeyScanInner {
  /// Update the cursor in place in the arguments.
  pub fn update_cursor(&mut self, cursor: Str) {
    self.args[self.cursor_idx] = cursor.into();
  }

  /// Send an error on the response stream.
  pub fn send_error(&self, error: Error) {
    let _ = self.tx.try_send(Err(error));
  }
}

impl KeyScanBufferedInner {
  /// Update the cursor in place in the arguments.
  pub fn update_cursor(&mut self, cursor: Str) {
    self.args[self.cursor_idx] = cursor.into();
  }

  /// Send an error on the response stream.
  pub fn send_error(&self, error: Error) {
    let _ = self.tx.try_send(Err(error));
  }
}

pub enum ValueScanResult {
  SScan(SScanResult),
  HScan(HScanResult),
  ZScan(ZScanResult),
}

pub struct ValueScanInner {
  /// The index of the cursor argument in `args`.
  pub cursor_idx: usize,
  /// The arguments sent in each scan command.
  pub args:       Vec<Value>,
  /// The sender half of the results channel.
  pub tx:         Sender<Result<ValueScanResult, Error>>,
}

impl ValueScanInner {
  /// Update the cursor in place in the arguments.
  pub fn update_cursor(&mut self, cursor: Str) {
    self.args[self.cursor_idx] = cursor.into();
  }

  /// Send an error on the response stream.
  pub fn send_error(&self, error: Error) {
    let _ = self.tx.try_send(Err(error));
  }

  pub fn transform_hscan_result(mut data: Vec<Value>) -> Result<Map, Error> {
    if data.is_empty() {
      return Ok(Map::new());
    }
    if data.len() % 2 != 0 {
      return Err(Error::new(
        ErrorKind::Protocol,
        "Invalid HSCAN result. Expected array with an even number of elements.",
      ));
    }

    let mut out = HashMap::with_capacity(data.len() / 2);
    while data.len() >= 2 {
      let value = data.pop().unwrap();
      let key: Key = match data.pop().unwrap() {
        Value::String(s) => s.into(),
        Value::Bytes(b) => b.into(),
        _ => {
          return Err(Error::new(
            ErrorKind::Protocol,
            "Invalid HSCAN result. Expected string.",
          ))
        },
      };

      out.insert(key, value);
    }

    out.try_into()
  }

  pub fn transform_zscan_result(mut data: Vec<Value>) -> Result<Vec<(Value, f64)>, Error> {
    if data.is_empty() {
      return Ok(Vec::new());
    }
    if data.len() % 2 != 0 {
      return Err(Error::new(
        ErrorKind::Protocol,
        "Invalid ZSCAN result. Expected array with an even number of elements.",
      ));
    }

    let mut out = Vec::with_capacity(data.len() / 2);

    for chunk in data.chunks_exact_mut(2) {
      let value = chunk[0].take();
      let score = match chunk[1].take() {
        Value::String(s) => utils::string_to_f64(&s)?,
        Value::Integer(i) => i as f64,
        Value::Double(f) => f,
        _ => {
          return Err(Error::new(
            ErrorKind::Protocol,
            "Invalid HSCAN result. Expected a string or number score.",
          ))
        },
      };

      out.push((value, score));
    }

    Ok(out)
  }
}
