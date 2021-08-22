use std::env;
use std::path::Path;
use std::thread::{self, sleep};
use std::time::Duration;

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

fn run(cli_path: String, server_path: String, create_cluster_path: String) {
  debug!("Starting chaos monkey...");
}

#[test]
fn start() {
  pretty_env_logger::try_init();
  let redis_cli_path = read_path_from_env("REDIS_CLI_PATH");
  let redis_server_path = read_path_from_env("REDIS_SERVER_PATH");
  let create_cluster_path = read_path_from_env("CREATE_CLUSTER_PATH");
  run(redis_cli_path, redis_server_path, create_cluster_path);
}
