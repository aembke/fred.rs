Fred
====

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/aembke/fred.rs/tree/main.svg?style=svg)](https://circleci.com/gh/aembke/fred.rs/tree/main)
[![Crates.io](https://img.shields.io/crates/v/fred.svg)](https://crates.io/crates/fred)
[![API docs](https://docs.rs/fred/badge.svg)](https://docs.rs/fred)

An async Redis client for Rust and Tokio.

## Example

```rust
use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = RedisClient::default();
  client.init().await?;

  // convert responses to many common Rust types
  let foo: Option<String> = client.get("foo").await?;
  assert!(foo.is_none());

  client.set("foo", "bar", None, None, false).await?;
  // or use turbofish to declare response types
  println!("Foo: {:?}", client.get::<String, _>("foo").await?);

  // or use a lower level interface for responses to defer parsing, etc
  let foo: RedisValue = client.get("foo").await?;
  assert_eq!(foo.as_str().unwrap(), "bar");

  client.quit().await?;
  Ok(())
}
```

See the [examples](https://github.com/aembke/fred.rs/tree/main/examples) for more.

## Features

* RESP2 and RESP3 protocol modes.
* Clustered, centralized, and sentinel Redis deployments.
* TLS via `native-tls` or `rustls`.
* Unix sockets.
* [Efficient automatic pipelining](bin/benchmark)
* [Zero-copy frame parsing](https://github.com/aembke/redis-protocol.rs)
* Optional reconnection features.
* Publish-Subscribe and keyspace events interfaces.
* A round-robin client pooling interface.
* Lua [scripts](https://redis.io/docs/interact/programmability/eval-intro/)
  or [functions](https://redis.io/docs/interact/programmability/functions-intro/).
* Streaming results from the `MONITOR` command.
* Custom commands.
* Streaming interfaces for scanning functions.
* [Transactions](https://redis.io/docs/interact/transactions/)
* [Pipelining](https://redis.io/topics/pipelining)
* [Client Tracking](https://redis.io/docs/manual/client-side-caching/)
* A [RedisJSON](https://github.com/RedisJSON/RedisJSON) interface.
* A round-robin cluster replica routing interface.
* A subscriber client that will automatically manage channel subscriptions.
* [Tracing](https://github.com/tokio-rs/tracing)

## Build Features

| Name                      | Default | Description                                                                                                                                                         |
|---------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `transactions`            | x       | Enable a [Transaction](https://redis.io/docs/interact/transactions/) interface.                                                                                     |
| `enable-native-tls`       |         | Enable TLS support via [native-tls](https://crates.io/crates/native-tls).                                                                                           |
| `enable-rustls`           |         | Enable TLS support via [rustls](https://crates.io/crates/rustls) with the default crypto backend features.                                                          |
| `enable-rustls-ring`      |         | Enable TLS support via [rustls](https://crates.io/crates/rustls) and the ring crypto backend.                                                                       |
| `vendored-openssl`        |         | Enable the `native-tls/vendored` feature.                                                                                                                           |
| `metrics`                 |         | Enable the metrics interface to track overall latency, network latency, and request/response sizes.                                                                 |
| `full-tracing`            |         | Enable full [tracing](./src/trace/README.md) support. This can emit a lot of data.                                                                                  |
| `partial-tracing`         |         | Enable partial [tracing](./src/trace/README.md) support, only emitting traces for top level commands and network latency.                                           |
| `blocking-encoding`       |         | Use a blocking task for encoding or decoding frames. This can be useful for clients that send or receive large payloads, but requires a multi-thread Tokio runtime. |
| `custom-reconnect-errors` |         | Enable an interface for callers to customize the types of errors that should automatically trigger reconnection logic.                                              |
| `monitor`                 |         | Enable an interface for running the `MONITOR` command.                                                                                                              |
| `sentinel-client`         |         | Enable an interface for communicating directly with Sentinel nodes. This is not necessary to use normal Redis clients behind a sentinel layer.                      |
| `sentinel-auth`           |         | Enable an interface for using different authentication credentials to sentinel nodes.                                                                               |
| `subscriber-client`       |         | Enable a subscriber client interface that manages channel subscription state for callers.                                                                           |
| `serde-json`              |         | Enable an interface to automatically convert Redis types to JSON via `serde-json`.                                                                                  |
| `mocks`                   |         | Enable a mocking layer interface that can be used to intercept and process commands in tests.                                                                       |
| `dns`                     |         | Enable an interface that allows callers to override the DNS lookup logic.                                                                                           |
| `replicas`                |         | Enable an interface that routes commands to replica nodes.                                                                                                          |
| `default-nil-types`       |         | Enable a looser parsing interface for `nil` values.                                                                                                                 |
| `sha-1`                   |         | Enable an interface for hashing Lua scripts.                                                                                                                        |
| `unix-sockets`            |         | Enable Unix socket support.                                                                                                                                         |

## Interface Features

The Redis interface has many functions and compile times can add up quickly. Interface features begin with `i-` and
control which public interfaces are built.

| Name            | Default | Description                                                                                                   |
|-----------------|---------|---------------------------------------------------------------------------------------------------------------|
| `i-all`         |         | Enable the interfaces included with a basic Redis installation. This does not include `redis-stack` features. |
| `i-std`         | x       | Enable the common data structure interfaces (lists, sets, streams, keys, etc).                                |
| `i-acl`         |         | Enable the ACL command interface.                                                                             |
| `i-client`      |         | Enable the CLIENT command interface.                                                                          |
| `i-cluster`     |         | Enable the CLUSTER command interface.                                                                         |
| `i-config`      |         | Enable the CONFIG command interface.                                                                          |
| `i-geo`         |         | Enable the GEO command interface.                                                                             |
| `i-hashes`      |         | Enable the hashes (HGET, etc) command interface.                                                              |
| `i-hyperloglog` |         | Enable the hyperloglog command interface.                                                                     |
| `i-keys`        |         | Enable the main keys (GET, SET, etc) command interface.                                                       |
| `i-lists`       |         | Enable the lists (LPUSH, etc) command interface.                                                              |
| `i-scripts`     |         | Enable the scripting command interfaces.                                                                      |
| `i-memory`      |         | Enable the MEMORY command interfaces.                                                                         |
| `i-pubsub`      |         | Enable the publish-subscribe command interfaces.                                                              |
| `i-server`      |         | Enable the server control (SHUTDOWN, BGSAVE, etc) interfaces.                                                 |
| `i-sets`        |         | Enable the sets (SADD, etc) interface.                                                                        |
| `i-sorted-sets` |         | Enable the sorted sets (ZADD, etc) interface.                                                                 |
| `i-slowlog`     |         | Enable the SLOWLOG interface.                                                                                 |
| `i-streams`     |         | Enable the streams (XADD, etc) interface.                                                                     |
| `i-tracking`    |         | Enable a [client tracking](https://redis.io/docs/manual/client-side-caching/) interface.                      |
| `i-time-series` |         | Enable a [Redis Timeseries](https://redis.io/docs/data-types/timeseries/).  interface.                        |
| `i-redis-json`  |         | Enable a [RedisJSON](https://github.com/RedisJSON/RedisJSON) interface.                                       |
| `i-redis-stack` |         | Enable the [Redis Stack](https://github.com/redis-stack) interfaces (`i-redis-json`, `i-time-series`, etc).   |

## Debugging Features

| Name           | Default | Description                                                     |
|----------------|---------|-----------------------------------------------------------------|
| `debug-ids`    |         | Enable a global counter used to differentiate commands in logs. |
| `network-logs` |         | Enable additional TRACE logs for all frames on all sockets.     |
