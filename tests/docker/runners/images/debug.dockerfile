# https://github.com/docker/for-mac/issues/5548#issuecomment-1029204019
# FROM rust:1.77-slim-buster
FROM rust:1.80-slim-bullseye

WORKDIR /project

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
ARG CIRCLECI_TESTS

RUN USER=root apt-get update && apt-get install -y build-essential libssl-dev dnsutils curl pkg-config cmake git vim linux-perf jq
RUN echo "REDIS_VERSION=$REDIS_VERSION"

# For debugging
RUN cargo --version && rustc --version
RUN rustup component add clippy && rustup install nightly
RUN cargo install flamegraph