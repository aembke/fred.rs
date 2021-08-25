#!/bin/bash

ROOT=$PWD

function check_root_dir {
  if [ ! -d "./tests/tmp" ]; then
    echo "Must be in application root for chaos monkey to work."
    exit 1
  fi
}

# Returns 0 if not installed, 1 otherwise.
function check_redis {
  if [ ! -d "$ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION" ]; then
    echo "Redis install not found."
    exit 1
  fi
}

function set_env_flags {
  export ROOT="$ROOT"
  export FRED_FAIL_FAST=false
  export REDIS_ROOT_DIR="$ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION"
  export REDIS_CLI_PATH="$REDIS_ROOT_DIR/src/redis-cli"
  export REDIS_SERVER_PATH="$REDIS_ROOT_DIR/src/redis-server"
  export CREATE_CLUSTER_PATH="$REDIS_ROOT_DIR/utils/create-cluster/create-cluster"

  if [ ! -f "$REDIS_CLI_PATH" ]; then
    echo "Missing redis-cli at $REDIS_CLI_PATH"
    exit 1
  fi
  if [ ! -f "$REDIS_SERVER_PATH" ]; then
    echo "Missing redis-server at $REDIS_SERVER_PATH"
    exit 1
  fi
  if [ ! -f "$CREATE_CLUSTER_PATH" ]; then
    echo "Missing create-cluster at $CREATE_CLUSTER_PATH"
    exit 1
  fi
}

if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi

check_root_dir
check_redis
set_env_flags

cargo test --release --features chaos-monkey --lib --tests -- --test-threads=1 -- "$@"

unset FRED_FAIL_FAST
unset REDIS_ROOT_DIR
unset REDIS_CLI_PATH
unset REDIS_SERVER_PATH
unset CREATE_CLUSTER_PATH