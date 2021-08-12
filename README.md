Fred
====

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/aembke/fred.rs.svg?branch=master)](https://travis-ci.org/aembke/fred.rs)
[![Crates.io](https://img.shields.io/crates/v/fred.svg)](https://crates.io/crates/fred)
[![API docs](https://docs.rs/fred/badge.svg)](https://docs.rs/fred)

An async Redis client for Rust built on Tokio and Futures.

## Example 

```rust
use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default_centralized();
  let policy = ReconnectPolicy::default();
  let client = RedisClient::new(config);
  
  // connect to the server, returning a handle to the task that drives the connection
  let _ = client.connect(Some(policy), false);
  // wait for the client to connect
  let _ = client.wait_for_connect().await?;
  
  println!("Foo: {:?}", client.get("foo").await?);
  let _ = client.set("foo", "bar", None, None, false).await?;
  println!("Foo: {:?}", client.get("foo".to_owned()).await?);
  
  let _ = client.quit().await?;
  Ok(())
}
```

See the [examples](examples/README.md) for more.

## Install

With [cargo edit](https://github.com/killercup/cargo-edit).

```
cargo add fred
```

## Features

* Supports clustered and centralized Redis deployments.
* Optional built-in reconnection logic with multiple backoff policies.
* Publish-Subscribe and keyspace events interfaces.
* Supports transactions.
* Supports Lua scripts.  
* Supports custom commands provided by third party modules. 
* Supports connections over TLS.
* Handles cluster rebalancing operations without downtime or errors.
* Supports various scanning functions.
* Automatically [pipeline](https://redis.io/topics/pipelining) requests, with an option for callers to disable this.
* Automatically retry requests under bad network conditions.
* Support for configuring global settings that can affect performance under different network conditions. Callers can configure backpressure settings, when and how the underlying socket is flushed, and how many times requests are attempted. 
* Built-in tracking for network latency and payload size metrics.
* A client pooling interface to round-robin requests among a pool of clients.
* Built in support for [tracing](https://crates.io/crates/tracing).

## Tracing

This crate supports tracing via the [tracing](https://github.com/tokio-rs/tracing) crate. See the [tracing docs](./src/trace/README.md) for more information.

This feature is disabled by default, but callers can opt-in via the `full-tracing` or `partial-tracing` features.

## Logging

To enable logs use the environment variable `RUST_LOG` with a value of `trace`, `debug`, `warn`, `error`, or `info`. See the documentation for [env_logger](http://rust-lang-nursery.github.io/log/env_logger/) for more information.

When a client is initialized it will generate a unique client name with a prefix of `fred-`. This name will appear in all logging statements on the client in order to associate client and server operations if logging is enabled on both.

## Compile Time Features

|    Name                     | Default | Description                                                                                                                                  |
|---------------------------- |---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| enable-tls                  |    x    | Enable TLS support. This requires OpenSSL (or equivalent) dependencies.                                                                      |
| vendored-tls                |         | Enable TLS support, using vendored OpenSSL (or equivalent) dependencies, if possible.                                                        |
| ignore-auth-error           |    x    | Ignore auth errors that occur when a password is supplied but not required.                                                                  |
| reconnect-on-auth-error     |         | A NOAUTH error is treated the same as a general connection failure and the client will reconnect based on the reconnection policy.           |
| index-map                   |         | Use [IndexMap](https://docs.rs/indexmap/*/indexmap/) instead of [HashMap](https://doc.rust-lang.org/std/collections/struct.HashMap.html) as the backing store for Redis Map types.   |
| pool-prefer-active          |    x    | Prefer connected clients over clients in a disconnected state when using the `RedisPool` interface.                                          |
| full-tracing                |         | Enable full [tracing](./src/trace/README.md) support. This can emit a lot of data so a partial tracing feature is also provided.           |
| partial-tracing             |         | Enable partial [tracing](./src/trace/README.md) support, only emitting traces for top level commands and network latency. Note: this has a non-trivial impact on [performance](./CONTRIBUTING.md#Performance).  |
| blocking-encoding           |         | Use a blocking task for encoding or decoding frames over a [certain size](./src/globals.rs). This can be useful for clients that send or receive large payloads, but will only work when used with a multi-thread Tokio runtime.  |
| network-logs                |         | Enable TRACE level logging statements that will print out all data sent to or received from the server.  |

## Environment Variables

|   Name                            | Default | Description                                                                              |
|-----------------------------------|---------|------------------------------------------------------------------------------------------|
| FRED_DISABLE_CERT_VERIFICATION    | `false` | Disable certificate verification when using TLS features.                                |
| FRED_DISABLE_HOST_VERIFICATION    | `false` | Disable host verification when using TLS features.                                       |

These are environment variables because they're dangerous in production and callers should be forced to surface them in a loud and obvious way.

## Pipelining

The caller can toggle [pipelining](https://redis.io/topics/pipelining) by providing flags to the client's `connect` function. These settings can drastically affect performance on both the server and client, but further performance tuning may be necessary to avoid issues such as using too much memory on the client or server while buffering commands.

See the global performance tuning functions for more information on how to tune backpressure or other relevant settings related to pipelining.

This module also contains a [separate test application](bin/pipeline_test) that can be used to demonstrate the effects of pipelining. This test application also contains some helpful information on how to use the tracing features.

## Tests

To run the unit and integration tests:

```
cargo test -- --test-threads=1
```

OR

```
./tests/run.sh
```

Note: a local Redis server must be running on port 6379, and a clustered deployment must be running on ports 30001 - 30006 for the integration tests to pass. 

Scripts are included to download and run centralized and clustered redis servers at any Redis version. These scripts will not make any modifications to your system outside the `tests/tmp` folder.

```
export REDIS_VERSION=6.2.2
./tests/scripts/install_redis_centralized.sh
./tests/scripts/install_redis_clustered.sh
```

**Beware: the tests will periodically run `flushall`.**

## Contributing g

See the [contributing](CONTRIBUTING.md) documentation for info on adding new commands.
