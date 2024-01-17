#!/bin/bash

[[ -z "${USE_REDIS_RS}" ]] && FEATURES="assert-expected" || FEATURES="assert-expected redis-rs"

echo $FEATURES

docker-compose -f ../../tests/docker/compose/cluster.yml \
  -f ../../tests/docker/compose/centralized.yml \
  -f ../../tests/docker/compose/unix-socket.yml -f ./docker-compose.yml \
  run -u $(id -u ${USER}):$(id -g ${USER}) --rm fred-benchmark cargo run --release --features "$FEATURES" -- "${@:1}"