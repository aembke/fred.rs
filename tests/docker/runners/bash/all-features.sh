#!/bin/bash

declare -a arr=("REDIS_VERSION" "REDIS_USERNAME" "REDIS_PASSWORD" "REDIS_SENTINEL_PASSWORD")

for env in "${arr[@]}"
do
  if [ -z "$env" ]; then
    echo "$env must be set. Run `source tests/environ` if needed."
    exit 1
  fi
done

FEATURES="network-logs pool-prefer-active custom-reconnect-errors ignore-auth-error serde-json blocking-encoding
          full-tracing reconnect-on-auth-error monitor metrics sentinel-client subscriber-client dns debug-ids
          check-unresponsive replicas client-tracking codec sha-1 auto-client-setname"

if [ -z "$FRED_CI_NEXTEST" ]; then
  cargo test --release --lib --tests --features "$FEATURES" -- --test-threads=1 "$@"
else
  cargo nextest run --release --lib --tests --features "$FEATURES" --test-threads=1 "$@"
fi