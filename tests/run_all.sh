#!/bin/bash

export RUST_BACKTRACE=full

echo "Testing with default features..."
cargo test --release --lib --tests -- --test-threads=1

echo "Testing with no features..."
cargo test --release --lib --tests --no-default-features -- --test-threads=1

echo "Testing with all features..."
# cant use all-features here or it'll run chaos monkey and then the tests will take forever
cargo test --release --lib --tests --features \
  "index-map network-logs pool-prefer-active enable-tls vendored-tls
  custom-reconnect-errors ignore-auth-error blocking-encoding full-tracing
  reconnect-on-auth-error monitor metrics" \
  -- --test-threads=1