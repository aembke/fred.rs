#!/bin/bash

cargo +nightly rustdoc --all-features "$@" -- --cfg docsrs