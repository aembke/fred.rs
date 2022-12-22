FROM rust:1.66.0-slim-buster

WORKDIR /project
COPY --chown=1001:1001 . /project

ARG RUST_LOG
ARG REDIS_VERSION
ARG REDIS_USERNAME
ARG REDIS_PASSWORD
ARG REDIS_SENTINEL_PASSWORD
ARG FRED_REDIS_CLUSTER_HOST
ARG FRED_REDIS_CLUSTER_PORT
ARG FRED_REDIS_CLUSTER_TLS_HOST
ARG FRED_REDIS_CLUSTER_TLS_PORT
ARG FRED_REDIS_CENTRALIZED_HOST
ARG FRED_REDIS_CENTRALIZED_PORT
ARG FRED_REDIS_SENTINEL_HOST
ARG FRED_REDIS_SENTINEL_PORT
ARG FRED_CI_NEXTEST
ARG CIRCLECI_TESTS
ARG CACHE_VERSION
ARG CARGO_HTTP_DEBUG
ARG CARGO_NET_GIT_FETCH_WITH_CLI
ARG RUSTC_WRAPPER
ARG SCCACHE_CACHE_SIZE

RUN USER=root apt-get update && apt-get install -y libssl-dev dnsutils
RUN echo "REDIS_VERSION=$REDIS_VERSION"

# For debugging
RUN cargo --version && rustc --version
# RUN cargo install cargo-nextest

# VOLUME /project/target
# VOLUME /usr/local/cargo/bin
CMD tests/docker/runners/bash/cluster-tls.sh
