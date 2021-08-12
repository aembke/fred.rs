#!/bin/bash

export RUST_BACKTRACE=full
cargo test --release --lib --tests -- --test-threads=1
cargo test --release --lib --tests --no-default-features -- --test-threads=1
cargo test --release --lib --tests --all-features -- --test-threads=1
