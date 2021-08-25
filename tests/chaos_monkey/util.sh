#!/bin/bash

function check_env {
  declare -a arr=("REDIS_CLI_PATH" "REDIS_SERVER_PATH" "ROOT" "CREATE_CLUSTER_PATH" "REDIS_ROOT_DIR")

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