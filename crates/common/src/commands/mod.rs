pub mod command;
pub mod kind;
pub mod responders;
pub mod scan;

use crate::{
  commands::command::CommandKind,
  error::Error,
  inner::ClientInner,
  runtime::{OneshotSender, RefCount},
  types::{Resp3Frame, Server},
};
use command::Command;
use log::warn;
use std::{fmt, fmt::Formatter, time::Duration};

/// A channel for communication between connection reader tasks and futures returned to the caller.
pub type ResponseSender = OneshotSender<Result<Resp3Frame, Error>>;

/// A message sent from the front-end client to the router.
pub enum RouterCommand {
  /// Send a command to the server.
  Command(Command),
  /// Send a pipelined series of commands to the server.
  Pipeline { commands: Vec<Command> },
  /// Send a transaction to the server.
  // The inner command buffer will not contain the trailing `EXEC` command.
  #[cfg(feature = "transactions")]
  Transaction {
    id:             u64,
    commands:       Vec<Command>,
    abort_on_error: bool,
    tx:             ResponseSender,
  },
  /// Initiate a reconnection to the provided server, or all servers.
  Reconnect {
    server:  Option<Server>,
    force:   bool,
    tx:      Option<ResponseSender>,
    #[cfg(feature = "replicas")]
    replica: bool,
  },
  /// Retry a command after a `MOVED` error.
  Moved {
    slot:    u16,
    server:  Server,
    command: Command,
  },
  /// Retry a command after an `ASK` error.
  Ask {
    slot:    u16,
    server:  Server,
    command: Command,
  },
  /// Sync the cached cluster state with the server via `CLUSTER SLOTS`.
  SyncCluster { tx: OneshotSender<Result<(), Error>> },
  /// Force sync the replica routing table with the server(s).
  #[cfg(feature = "replicas")]
  SyncReplicas {
    tx:    OneshotSender<Result<(), Error>>,
    reset: bool,
  },
}

impl RouterCommand {
  /// Whether the command should check the health of the backing connections before being used.
  pub fn should_check_fail_fast(&self) -> bool {
    match self {
      RouterCommand::Command(command) => command.fail_fast,
      RouterCommand::Pipeline { commands, .. } => commands.first().map(|c| c.fail_fast).unwrap_or(false),
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { commands, .. } => commands.first().map(|c| c.fail_fast).unwrap_or(false),
      _ => false,
    }
  }

  /// Finish the command early with the provided error.
  #[allow(unused_mut)]
  pub fn finish_with_error(self, error: Error) {
    match self {
      RouterCommand::Command(mut command) => {
        command.respond_to_caller(Err(error));
      },
      RouterCommand::Pipeline { commands } => {
        for mut command in commands.into_iter() {
          command.respond_to_caller(Err(error.clone()));
        }
      },
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { mut tx, .. } => {
        if let Err(_) = tx.send(Err(error)) {
          warn!("Error responding early to transaction.");
        }
      },
      RouterCommand::Reconnect { tx: Some(mut tx), .. } => {
        if let Err(_) = tx.send(Err(error)) {
          warn!("Error responding early to reconnect command.");
        }
      },
      _ => {},
    }
  }

  /// Inherit settings from the configuration structs on `inner`.
  pub fn inherit_options(&mut self, inner: &RefCount<ClientInner>) {
    match self {
      RouterCommand::Command(ref mut cmd) => {
        cmd.inherit_options(inner);
      },
      RouterCommand::Pipeline { ref mut commands, .. } => {
        for cmd in commands.iter_mut() {
          cmd.inherit_options(inner);
        }
      },
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { ref mut commands, .. } => {
        for cmd in commands.iter_mut() {
          cmd.inherit_options(inner);
        }
      },
      _ => {},
    };
  }

  /// Apply a timeout to the response channel receiver based on the command and `inner` context.
  pub fn timeout_dur(&self) -> Option<Duration> {
    match self {
      RouterCommand::Command(ref command) => command.timeout_dur,
      RouterCommand::Pipeline { ref commands, .. } => commands.first().and_then(|c| c.timeout_dur),
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { ref commands, .. } => commands.first().and_then(|c| c.timeout_dur),
      _ => None,
    }
  }

  /// Cancel the underlying command and respond to the caller, if possible.
  pub fn cancel(self) {
    match self {
      RouterCommand::Command(mut command) => {
        let result = if command.kind == CommandKind::Quit {
          Ok(Resp3Frame::Null)
        } else {
          Err(Error::new_canceled())
        };

        command.respond_to_caller(result);
      },
      RouterCommand::Pipeline { mut commands } => {
        if let Some(mut command) = commands.pop() {
          command.respond_to_caller(Err(Error::new_canceled()));
        }
      },
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { tx, .. } => {
        let _ = tx.send(Err(Error::new_canceled()));
      },
      _ => {},
    }
  }
}

impl fmt::Debug for RouterCommand {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let mut formatter = f.debug_struct("RouterCommand");

    match self {
      #[cfg(not(feature = "replicas"))]
      RouterCommand::Reconnect { server, force, .. } => {
        formatter
          .field("kind", &"Reconnect")
          .field("server", &server)
          .field("force", &force);
      },
      #[cfg(feature = "replicas")]
      RouterCommand::Reconnect {
        server, force, replica, ..
      } => {
        formatter
          .field("kind", &"Reconnect")
          .field("server", &server)
          .field("replica", &replica)
          .field("force", &force);
      },
      RouterCommand::SyncCluster { .. } => {
        formatter.field("kind", &"Sync Cluster");
      },
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { .. } => {
        formatter.field("kind", &"Transaction");
      },
      RouterCommand::Pipeline { .. } => {
        formatter.field("kind", &"Pipeline");
      },
      RouterCommand::Ask { .. } => {
        formatter.field("kind", &"Ask");
      },
      RouterCommand::Moved { .. } => {
        formatter.field("kind", &"Moved");
      },
      RouterCommand::Command(command) => {
        formatter
          .field("kind", &"Command")
          .field("command", &command.kind.to_str_debug());
      },
      #[cfg(feature = "replicas")]
      RouterCommand::SyncReplicas { reset, .. } => {
        formatter.field("kind", &"Sync Replicas");
        formatter.field("reset", &reset);
      },
    };

    formatter.finish()
  }
}

impl From<Command> for RouterCommand {
  fn from(cmd: Command) -> Self {
    RouterCommand::Command(cmd)
  }
}

// TODO put default_send_command here. combine with router.rs file
