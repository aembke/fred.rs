#!/bin/bash

    "i-all i-redis-stack transactions blocking-encoding dns metrics mocks monnitor \
    replicas sentinel-auth sentinel-client serde-json subscriber-client unix-sockets \
    enable-rustls enable-native-tls full-tracing"

TEST_ARGV="$1" docker-compose -f tests/docker/compose/centralized.yml \
  -f tests/docker/compose/cluster.yml \
  -f tests/docker/runners/compose/all-features.yml run -u $(id -u ${USER}):$(id -g ${USER}) --rm all-features-tests