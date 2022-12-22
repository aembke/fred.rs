#!/bin/bash

docker-compose -f tests/docker/compose/centralized.yml \
  -f tests/docker/compose/cluster.yml \
  -f tests/docker/runners/compose/all-features.yml run --rm all-features-tests