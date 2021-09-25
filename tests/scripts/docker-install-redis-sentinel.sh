#!/bin/bash

if [ ! -d "./tests/tmp" ]; then
  echo "Must be in application root for redis installation scripts to work."
  exit 1
fi

if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi

echo "Note: this requires docker, docker-compose, and redis >=6.2 to work reliably."
docker-compose -f ./tests/sentinel-docker-compose.yml up --scale redis-sentinel=3 -d
