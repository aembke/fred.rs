#!/bin/bash

REDIS_USERNAME=foo REDIS_PASSWORD=bar cargo test --release --lib --tests -- --test-threads=1 -- "$@"