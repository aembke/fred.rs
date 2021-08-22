#!/bin/bash

cargo test --release --lib --tests -- --test-threads=1 -- "$@"