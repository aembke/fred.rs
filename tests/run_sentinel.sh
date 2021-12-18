#!/bin/bash

# these environment variables cant be changed unless the sentinel docker compose file is updated as well
export REDIS_USERNAME=foo REDIS_PASSWORD=bar REDIS_SENTINEL_PASSWORD=baz
cargo test --release --features "network-logs sentinel-tests sentinel-auth" --lib --tests -- --test-threads=1 -- "$@"