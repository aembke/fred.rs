#!/bin/bash

export RUST_BACKTRACE=full
cargo test --release -- --test-threads=1
cargo test --release --no-default-features -- --test-threads=1
cargo test --release --all-features -- --test-threads=1
