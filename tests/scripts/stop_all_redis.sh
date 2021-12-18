#!/bin/bash

if [ ! -d "./tests/tmp" ]; then
  echo "Must be in application root for redis installation scripts to work."
  exit 1
fi
if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi

echo "Stopping redis processes..."
pgrep redis | xargs kill -9

echo "Stopping sentinel redis in docker..."
docker-compose -f ./tests/sentinel-docker-compose.yml stop