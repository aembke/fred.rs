#!/bin/bash

export RUST_BACKTRACE=full
cargo test --release --lib --tests -- --test-threads=1
cargo test --release --lib --tests --no-default-features -- --test-threads=1
cargo test --release --lib --tests --features \
  "index-map network-logs pool-prefer-active enable-tls vendored-tls custom-reconnect-errors ignore-auth-error blocking-encoding full-tracing reconnect-on-auth-error" \
  -- --test-threads=1
