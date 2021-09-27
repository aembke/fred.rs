#!/bin/bash

cargo test --release --features sentinel-tests --lib --tests -- --test-threads=1 -- "$@"