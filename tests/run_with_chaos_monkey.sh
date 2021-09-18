#!/bin/bash

ROOT=$PWD
. $ROOT/tests/chaos_monkey/util.sh

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