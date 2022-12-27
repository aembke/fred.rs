FROM rust:1.66.0-slim-buster

WORKDIR /project
# circleci doesn't mount images so we have to copy everything
COPY --chown=1001:1001 . /project
COPY --chown=1001:1001 /home/circleci/.cargo/bin/sccache /usr/local/cargo/bin/sccache
COPY --chown=1001:1001 /home/circleci/.cache/sccache /home/root/.cache/sccache


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
ARG CARGO_HTTP_DEBUG
ARG CARGO_NET_GIT_FETCH_WITH_CLI
ARG RUSTC_WRAPPER
ARG SCCACHE_CACHE_SIZE

RUN USER=root apt-get update && apt-get install -y build-essential libssl-dev dnsutils
RUN echo "REDIS_VERSION=$REDIS_VERSION"

# For debugging
RUN cargo --version && rustc --version