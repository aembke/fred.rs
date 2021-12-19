#!/bin/bash

RUST_BACKTRACE=full REDIS_USERNAME=foo REDIS_PASSWORD=bar cargo test --release --lib --tests --no-default-features -- --test-threads=1