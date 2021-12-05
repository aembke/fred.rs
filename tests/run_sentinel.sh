#!/bin/bash

cargo test --release --features "sentinel-tests" --lib --tests -- --test-threads=1 -- "$@"
cargo test --release --features "sentinel-tests sentinel-auth" --lib --tests -- --test-threads=1 -- "$@"