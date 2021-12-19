#!/bin/bash

export RUST_BACKTRACE=full REDIS_USERNAME=foo REDIS_PASSWORD=bar REDIS_SENTINEL_PASSWORD=baz
cargo test --release --features "sentinel-tests sentinel-auth" --lib --tests -- --test-threads=1