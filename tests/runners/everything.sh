#!/bin/bash

tests/runners/no-features.sh \
  && tests/runners/default-features.sh \
  && tests/runners/all-features.sh \
  && tests/runners/sentinel-features.sh \
  && tests/runners/cluster-native-tls.sh \
  && tests/runners/cluster-rustls.sh