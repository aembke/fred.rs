#!/bin/bash

# try it with docker pause
# maybe the cluster slots result is not what i expect

TEST_ARGV="$@" docker-compose -f ../../tests/docker/compose/cluster.yml -f ../../tests/docker/compose/centralized.yml -f ./docker-compose.yml \
  run -u $(id -u ${USER}):$(id -g ${USER}) --rm inf-loop