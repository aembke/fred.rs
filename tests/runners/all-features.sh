#!/bin/bash

declare -a arr=("REDIS_VERSION" "REDIS_USERNAME" "REDIS_PASSWORD" "REDIS_SENTINEL_PASSWORD")

for env in "${arr[@]}"
do
  if [ -z "$env" ]; then
    echo "$env must be set. Run `source tests/environ` if needed."
    exit 1
  fi
done

# cant use all-features here or it'll run chaos monkey and then the tests will take forever
FEATURES="network-logs pool-prefer-active enable-native-tls vendored-openssl enable-rustls custom-reconnect-errors
          ignore-auth-error serde-json blocking-encoding full-tracing reconnect-on-auth-error monitor metrics sentinel-client
          subscriber-client no-client-setname dns debug-ids"

if [ -z "$FRED_CI_NEXTEST" ]; then
  cargo test --release --lib --tests --features "$FEATURES" -- --test-threads=1 "$@"
else
  cargo nextest run --release --lib --tests --features "$FEATURES" --test-threads=1 "$@"
fi