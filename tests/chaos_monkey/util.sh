#!/bin/bash

function check_env {
  declare -a arr=("REDIS_CLI_PATH" "REDIS_SERVER_PATH" "ROOT" "CREATE_CLUSTER_PATH" "REDIS_ROOT_DIR" "FRED_FAIL_FAST")

  for env in "${arr[@]}"
  do
    if [ -z "$env" ]; then
      echo "$env must be set."
      exit 1
    fi
  done
}

function check_move_env {
  if [ -z "$SRC_SERVER_PORT" ]; then
    echo "SRC_SERVER_PORT must be set."
    exit 1
  fi

  if [ -z "$DEST_SERVER_PORT" ]; then
    echo "DEST_SERVER_PORT must be set."
    exit 1
  fi
}

function check_root_dir {
  if [ ! -d "./tests/tmp" ]; then
    # there's a gitignore for the files that this can create in the root dir
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