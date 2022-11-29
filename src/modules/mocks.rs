//! An interface for mocking Redis commands.
//!
//! There are several patterns for utilizing a mocking layer in tests. In some cases a simple "echo" interface is
//! enough, or in others callers may need to buffer a series of commands before performing any assertions, etc. More
//! complicated test scenarios may require storing and operating on real values.
//!
//! This interface exposes several interfaces and structs for supporting the above use cases:
//! * `Echo` - A simple mocking struct that returns the provided arguments back to the caller.
//! * `SimpleMap` - A mocking struct that implements the basic `GET`, `SET`, and `DEL` commands.
//! * `Buffer` - A mocking struct that buffers commands internally, returning `QUEUED` to each command. Callers can
//!   then drain or inspect the buffer later.
//!
//! The base `Mocks` trait is directly exposed so callers can implement their own mocking layer as well.

use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::command::{MultiplexerCommand, RedisCommand},
  types::{RedisKey, RedisValue},
  utils as client_utils,
};
use bytes_utils::Str;
use parking_lot::Mutex;
use std::{
  collections::{BTreeMap, HashMap, VecDeque},
  fmt::Debug,
  sync::Arc,
};
use tokio::sync::mpsc::UnboundedReceiver;

/// A wrapper type for the parts of an internal Redis command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockCommand {
  pub cmd:        Str,
  pub subcommand: Option<Str>,
  pub args:       Vec<RedisValue>,
}

/// An interface for intercepting and processing Redis commands in a mocking layer.
#[allow(unused_variables)]
pub trait Mocks: Debug + Send + Sync + 'static {
  /// Intercept and process a Redis command, returning any `RedisValue`.
  ///
  /// # Important
  ///
  /// The caller must ensure the response value makes sense in the context of the specific command(s) being mocked.
  /// The parsing logic following each command on the public interface will still be applied. __Most__ commands
  /// perform minimal parsing on the response, but some may require specific response formats to function correctly.
  ///
  /// `RedisValue::Queued` can be used to return a value that will work almost anywhere.
  fn process_command(&self, command: MockCommand) -> Result<RedisValue, RedisError>;

  /// Intercept and process an entire transaction. The provided commands will **not** include `MULTI` or `EXEC`.
  ///
  /// Note: The default implementation redirects each command to the [process_command](Self::process_command)
  /// function. The results of each call are buffered and returned as an array.
  fn process_transaction(&self, commands: Vec<MockCommand>) -> Result<RedisValue, RedisError> {
    let mut out = Vec::with_capacity(commands.len());

    for command in commands.into_iter() {
      out.push(self.process_command(command)?);
    }
    Ok(RedisValue::Array(out))
  }
}

/// An implementation of a mocking layer that returns the provided arguments to the caller.
///
/// ```rust no_run
/// #[tokio::test]
/// async fn should_use_echo_mock() {
///   let config = RedisConfig {
///     mocks: Arc::new(Echo),
///     ..Default::default()
///   };
///   let client = RedisClient::new(config, None, None);
///   let _ = client.connect();
///   let _ = client.wait_for_connect().await.expect("Failed to connect");
///
///   let actual: Vec<RedisValue> = client
///     .set(
///       "foo",
///       "bar",
///       Some(Expiration::EX(100)),
///       Some(SetOptions::NX),
///       false,
///     )
///     .await
///     .expect("Failed to call SET");
///
///   let expected: Vec<RedisValue> = vec![
///     "foo".into(),
///     "bar".into(),
///     "EX".into(),
///     "100".into(),
///     "NX".into(),
///   ];
///   assert_eq!(actual, expected);
/// }
/// ```
#[derive(Debug)]
pub struct Echo;

impl Mocks for Echo {
  fn process_command(&self, command: MockCommand) -> Result<RedisValue, RedisError> {
    Ok(RedisValue::Array(command.args))
  }
}

/// A struct that implements some of the basic mapping functions.
#[derive(Debug)]
pub struct SimpleMap {
  values: Mutex<HashMap<RedisKey, RedisValue>>,
}

impl SimpleMap {
  /// Create a new empty `SimpleMap`.
  pub fn new() -> Self {
    SimpleMap {
      values: Mutex::new(HashMap::new()),
    }
  }

  /// Clear the inner map.
  pub fn clear(&self) {
    self.values.lock().clear();
  }

  /// Take the inner map.
  pub fn take(&self) -> HashMap<RedisKey, RedisValue> {
    self.values.lock().drain().collect()
  }

  /// Read a copy of the inner map.
  pub fn inner(&self) -> HashMap<RedisKey, RedisValue> {
    self.values.lock().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
  }

  /// Perform a `GET` operation.
  pub fn get(&self, args: Vec<RedisValue>) -> Result<RedisValue, RedisError> {
    unimplemented!()
  }

  /// Perform a `SET` operation.
  pub fn set(&self, args: Vec<RedisValue>) -> Result<RedisValue, RedisError> {
    unimplemented!()
  }

  /// Perform a `DEL` operation.
  pub fn del(&self, args: Vec<RedisValue>) -> Result<RedisValue, RedisError> {
    unimplemented!()
  }
}

impl Mocks for SimpleMap {
  fn process_command(&self, command: MockCommand) -> Result<RedisValue, RedisError> {
    match &*command.cmd {
      "GET" => self.get(command.args),
      "SET" => self.set(command.args),
      "DEL" => self.del(command.args),
      _ => Err(RedisError::new(RedisErrorKind::Unknown, "Unimplemented.")),
    }
  }
}

/// A mocking layer that buffers the commands internally and returns `QUEUED` to the caller.
#[derive(Debug)]
pub struct Buffer {
  commands: Mutex<VecDeque<MockCommand>>,
}

impl Buffer {
  /// Create a new empty `Buffer`.
  pub fn new() -> Self {
    Buffer {
      commands: Mutex::new(VecDeque::new()),
    }
  }

  /// Read the length of the internal buffer.
  pub fn len(&self) -> usize {
    self.commands.lock().len()
  }

  /// Clear the inner buffer.
  pub fn clear(&self) {
    self.commands.lock().clear();
  }

  /// Drain and return the internal command buffer.
  pub fn take(&self) -> Vec<MockCommand> {
    self.commands.lock().drain(..).collect()
  }

  /// Read a copy of the internal command buffer without modifying the contents.
  pub fn inner(&self) -> Vec<MockCommand> {
    self.commands.lock().iter().map(|c| c.clone()).collect()
  }

  /// Push a new command onto the back of the internal buffer.
  pub fn push_back(&self, command: MockCommand) {
    self.commands.lock().push_back(command);
  }

  /// Pop a command from the back of the internal buffer.
  pub fn pop_back(&self) -> Option<MockCommand> {
    self.commands.lock().pop_back()
  }

  /// Push a new command onto the front of the internal buffer.
  pub fn push_front(&self, command: MockCommand) {
    self.commands.lock().push_front(command);
  }

  /// Pop a command from the front of the internal buffer.
  pub fn pop_front(&self) -> Option<MockCommand> {
    self.commands.lock().pop_front()
  }
}

impl Mocks for Buffer {
  fn process_command(&self, command: MockCommand) -> Result<RedisValue, RedisError> {
    self.push_back(command);
    Ok(RedisValue::Queued)
  }
}

#[cfg(test)]
#[cfg(feature = "mocks")]
mod tests {
  use super::*;
  use crate::{
    clients::RedisClient,
    error::RedisError,
    interfaces::{ClientLike, KeysInterface},
    mocks::{Buffer, Echo, Mocks, SimpleMap},
    prelude::Expiration,
    types::{RedisConfig, RedisKey, RedisValue, SetOptions},
  };
  use tokio::task::JoinHandle;

  async fn create_mock_client<M: Mocks>(mock: M) -> (RedisClient, JoinHandle<Result<(), RedisError>>) {
    let config = RedisConfig {
      mocks: Arc::new(mock),
      ..Default::default()
    };
    let client = RedisClient::new(config, None, None);
    let jh = client.connect();
    let _ = client.wait_for_connect().await.expect("Failed to connect");

    (client, jh)
  }

  #[tokio::test]
  async fn should_create_mock_config_and_client() {
    let _ = create_mock_client(Echo).await;
  }

  #[tokio::test]
  async fn should_use_echo_mock() {
    let (client, _) = create_mock_client(Echo).await;

    let actual: Vec<RedisValue> = client
      .set("foo", "bar", Some(Expiration::EX(100)), Some(SetOptions::NX), false)
      .await
      .expect("Failed to call SET");

    let expected: Vec<RedisValue> = vec!["foo".into(), "bar".into(), "EX".into(), "100".into(), "NX".into()];
    assert_eq!(actual, expected);
  }
}
