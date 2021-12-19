#!/bin/bash

export RUST_BACKTRACE=full REDIS_USERNAME=foo REDIS_PASSWORD=bar

# cant use all-features here or it'll run chaos monkey and then the tests will take forever
cargo test --release --lib --tests --features \
  "index-map network-logs pool-prefer-active enable-tls vendored-tls
  custom-reconnect-errors ignore-auth-error blocking-encoding full-tracing
  reconnect-on-auth-error monitor metrics sentinel-client" \
  -- --test-threads=1