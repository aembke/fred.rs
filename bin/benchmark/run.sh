#!/bin/bash

FEATURES="assert-expected"

docker-compose -f ../../tests/docker/compose/cluster.yml -f ../../tests/docker/compose/centralized.yml -f ./docker-compose.yml \
  run -u $(id -u ${USER}):$(id -g ${USER}) --rm fred-benchmark cargo run --release --features "$FEATURES" -- "${@:1}"