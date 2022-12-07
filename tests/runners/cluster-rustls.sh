#!/bin/bash

declare -a arr=("REDIS_VERSION" "REDIS_USERNAME" "REDIS_PASSWORD" "REDIS_SENTINEL_PASSWORD", "FRED_TEST_TLS_CREDS")

for env in "${arr[@]}"
do
  if [ -z "$env" ]; then
    echo "$env must be set. Run `source tests/environ` if needed."
    exit 1
  fi
done

FEATURES="enable-rustls ignore-auth-error"

export FRED_REDIS_CENTRALIZED_HOST="example.com"
export FRED_REDIS_CENTRALIZED_PORT="6379"
export FRED_REDIS_CLUSTER_HOST="node-30001.example.com"
export FRED_REDIS_CLUSTER_PORT="30001"
export FRED_TEST_TLS_CREDS=$PWD/tests/tmp/creds
export FRED_CI_TLS="true"

if [ -z "$FRED_CI_NEXTEST" ]; then
  cargo test --release --lib --tests --features "$FEATURES" -- --test-threads=1 "$@"
else
  cargo nextest run --release --lib --tests --features "$FEATURES" --test-threads=1 "$@"
fi