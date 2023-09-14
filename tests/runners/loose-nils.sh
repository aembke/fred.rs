#!/bin/bash

TEST_ARGV="$1" docker-compose -f tests/docker/compose/centralized.yml \
  -f tests/docker/compose/cluster.yml \
  -f tests/docker/runners/compose/loose-nils.yml run \
  -u $(id -u ${USER}):$(id -g ${USER}) --rm \
  loose-nils-tests