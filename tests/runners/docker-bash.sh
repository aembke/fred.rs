#!/bin/bash

# boot all the redis servers and start a bash shell on a new container
docker-compose -f tests/docker/compose/cluster-tls.yml -f tests/docker/compose/centralized.yml -f tests/docker/compose/cluster.yml \
  -f tests/docker/compose/sentinel.yml -f tests/docker/compose/base.yml run -u $(id -u ${USER}):$(id -g ${USER}) --rm debug