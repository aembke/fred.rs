#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use fred::clients::RedisClient;
use fred::error::{RedisError, RedisErrorKind};
use fred::globals;
use fred::interfaces::*;
use fred::types::{RedisConfig, RedisKey, ServerConfig};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::Duration;
use subprocess::{Popen, PopenConfig, Redirection};
use tokio::runtime::Builder;

#[cfg(feature = "chaos-monkey")]
use fred::globals::ReconnectError;

lazy_static! {
  static ref TEST_KIND: TestKind = TestKind::default();
}

pub struct TestKind {
  is_clustered: Arc<RwLock<bool>>,
}

impl TestKind {
  pub fn set_clustered(&self, is_clustered: bool) {
    let mut guard = self.is_clustered.write();
    *guard = is_clustered
  }

  pub fn is_clustered(&self) -> bool {
    *self.is_clustered.read()
  }
}

impl Default for TestKind {
  fn default() -> Self {
    TestKind {
      is_clustered: Arc::new(RwLock::new(false)),
    }
  }
}

pub fn set_test_kind(clustered: bool) {
  TEST_KIND.set_clustered(clustered);
}

struct MoveArgs {
  pub src: u16,
  pub dest: u16,
}

fn read_path_from_env(env_path: &str) -> String {
  match env::var_os(env_path) {
    Some(s) => match s.into_string() {
      Ok(s) => {
        if Path::new(&s).exists() {
          s.to_owned()
        } else {
          panic!("Invalid {}: {} not found.", env_path, s);
        }
      }
      Err(_) => panic!("Invalid {} env variable.", env_path),
    },
    None => panic!("Missing {} env variable.", env_path),
  }
}

fn env_vars() -> Vec<(OsString, OsString)> {
  env::vars_os().collect()
}

async fn read_foo_src_and_dest() -> Result<(u16, u16), RedisError> {
  let config = RedisConfig {
    server: ServerConfig::default_clustered(),
    pipeline: false,
    ..Default::default()
  };
  let client = RedisClient::new(config);
  let _ = client.connect(None);
  let _ = client.wait_for_connect().await?;

  let foo = RedisKey::new("foo");
  let owner = match foo.cluster_owner(&client) {
    Some(server) => server.split(":").skip(1).next().unwrap().parse::<u16>()?,
    None => return Err(RedisError::new(RedisErrorKind::Unknown, "Failed to find owner")),
  };
  // find dest node from the other nodes
  let destination = client.cached_cluster_state().and_then(|state| loop {
    let server = state.random_slot().unwrap().server.clone();

    let port = server
      .split(":")
      .skip(1)
      .next()
      .unwrap()
      .parse::<u16>()
      .ok()
      .and_then(|port| if port == owner { None } else { Some(port) });

    if let Some(port) = port {
      return Some(port);
    }
  });
  let destination = match destination {
    Some(d) => d,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Failed to find a destination node.",
      ))
    }
  };

  let _ = client.quit().await;
  Ok((owner, destination))
}

fn create_move_env(args: MoveArgs) -> Vec<(OsString, OsString)> {
  vec![
    ("SRC_SERVER_PORT".into(), args.src.to_string().into()),
    ("DEST_SERVER_PORT".into(), args.dest.to_string().into()),
  ]
}

fn run_command(path: &str, cwd: &str, env: Vec<(OsString, OsString)>) {
  let mut cmd_env = env_vars();
  cmd_env.extend(env);
  trace!("Running {} in {} with env {:?}", path, cwd, cmd_env);

  let config = PopenConfig {
    env: Some(cmd_env),
    cwd: Some(cwd.into()),
    stdout: Redirection::Pipe,
    stderr: Redirection::Pipe,
    ..Default::default()
  };
  let mut p = Popen::create(&[path], config).unwrap();
  sleep(Duration::from_millis(1500));

  let (out, err) = p.communicate(None).unwrap();
  trace!("Stdout: {:?}", out);
  trace!("Stderr: {:?}", err);

  for _ in 0..15 {
    if let Some(exit_status) = p.poll() {
      trace!("Finished running with {:?}", exit_status);
      break;
    } else {
      sleep(Duration::from_millis(500));
    }
  }
  let _ = p.terminate();
}

fn move_foo(root_path: &str) {
  let mut cmd_path = Path::new(root_path).to_path_buf();
  cmd_path.push("tests");
  cmd_path.push("chaos_monkey");
  cmd_path.push("move_foo.sh");
  let path = cmd_path.to_str().unwrap();

  let runtime = Builder::new_current_thread().enable_all().build().unwrap();
  let result = runtime.block_on(async { read_foo_src_and_dest().await });
  let args = match result {
    Ok((src, dest)) => MoveArgs { src, dest },
    Err(e) => {
      warn!("Error reading src/dest ports {:?}", e);
      return;
    }
  };
  debug!("Moving foo from {} -> {}", args.src, args.dest);

  run_command(path, root_path, create_move_env(args));
}

fn restart_cluster(root_path: &str, create_cluster_path: &str) {
  let mut cmd_path = Path::new(root_path).to_path_buf();
  cmd_path.push("tests");
  cmd_path.push("chaos_monkey");
  cmd_path.push("restart_clustered.sh");
  let path = cmd_path.to_str().unwrap();
  let mut cwd_path = Path::new(create_cluster_path).to_path_buf();
  cwd_path.pop();
  let cwd_path = cwd_path.to_str().unwrap();

  run_command(path, cwd_path, vec![("WAIT".into(), "1".into())]);
}

fn restart_centralized(root_path: &str) {
  let mut cmd_path = Path::new(root_path).to_path_buf();
  cmd_path.push("tests");
  cmd_path.push("chaos_monkey");
  cmd_path.push("restart_centralized.sh");
  let path = cmd_path.to_str().unwrap();

  run_command(
    path,
    root_path,
    vec![("WAIT".into(), "1".into()), ("PORT".into(), "6379".into())],
  );
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum Operation {
  MoveFoo,
  RestartCluster,
  RestartCentralized,
}

impl Operation {
  pub fn delay(&self) -> u64 {
    match *self {
      Operation::MoveFoo => 7,
      // restarting a cluster can take a while. it may boot quickly but it takes several seconds to load the aof
      Operation::RestartCluster => 9,
      Operation::RestartCentralized => 4,
    }
  }
}

fn run(root_path: String, cli_path: String, server_path: String, create_cluster_path: String) {
  let mut count = 0;
  debug!("Starting chaos monkey...");
  sleep(Duration::from_secs(1));

  loop {
    let operation = if TEST_KIND.is_clustered() {
      if count % 2 == 0 {
        //Operation::RestartCluster
        Operation::MoveFoo
      } else {
        Operation::RestartCluster
      }
    } else {
      Operation::RestartCentralized
    };
    let sleep_dur = operation.delay();
    debug!("Next operation: {:?}", operation);

    match operation {
      Operation::MoveFoo => move_foo(&root_path),
      Operation::RestartCentralized => restart_centralized(&root_path),
      Operation::RestartCluster => restart_cluster(&root_path, &create_cluster_path),
    };

    sleep(Duration::from_secs(sleep_dur));
    count += 1;
  }
}

#[test]
#[cfg(feature = "chaos-monkey")]
fn start() {
  let _ = pretty_env_logger::try_init_timed();

  globals::set_max_command_attempts(50);
  globals::set_default_command_timeout(30_000);
  globals::set_custom_reconnect_errors(vec![
    ReconnectError::Loading,
    ReconnectError::ClusterDown,
    ReconnectError::ReadOnly,
  ]);

  thread::spawn(move || {
    let root_path = read_path_from_env("ROOT");
    let redis_cli_path = read_path_from_env("REDIS_CLI_PATH");
    let redis_server_path = read_path_from_env("REDIS_SERVER_PATH");
    let create_cluster_path = read_path_from_env("CREATE_CLUSTER_PATH");
    run(root_path, redis_cli_path, redis_server_path, create_cluster_path);
  });
}
