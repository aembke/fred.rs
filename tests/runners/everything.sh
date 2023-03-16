#!/bin/bash

tests/runners/no-features.sh "$1"\
  && tests/runners/default-features.sh "$1"\
  && tests/runners/all-features.sh "$1"\
  && tests/runners/sentinel-features.sh "$1"\
  && tests/runners/cluster-native-tls.sh "$1"\
  && tests/runners/cluster-rustls.sh "$1"