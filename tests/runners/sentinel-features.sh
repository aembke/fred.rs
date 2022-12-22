#!/bin/bash

docker-compose -f tests/docker/compose/sentinel.yml -f tests/docker/runners/compose/sentinel-features.yml run --rm sentinel-tests