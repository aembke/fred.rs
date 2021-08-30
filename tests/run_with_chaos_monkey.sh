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

if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi

check_root_dir
check_redis

ROOT="$ROOT" \
 FRED_FAIL_FAST=false \
 REDIS_ROOT_DIR="$ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION" \
 REDIS_CLI_PATH="$REDIS_ROOT_DIR/src/redis-cli" \
 REDIS_SERVER_PATH="$REDIS_ROOT_DIR/src/redis-server" \
 CREATE_CLUSTER_PATH="$REDIS_ROOT_DIR/utils/create-cluster/create-cluster" \
 cargo test --release --features "chaos-monkey custom-reconnect-errors network-logs" \
 --lib --tests -- --test-threads=1 -- "$@"