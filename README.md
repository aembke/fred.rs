Fred
====

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/aembke/fred.rs/tree/main.svg?style=svg)](https://circleci.com/gh/aembke/fred.rs/tree/main)
[![Crates.io](https://img.shields.io/crates/v/fred.svg)](https://crates.io/crates/fred)
[![API docs](https://docs.rs/fred/badge.svg)](https://docs.rs/fred)

A high level async Redis client for Rust built on Tokio and Futures. 

## Example 

```rust
use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let policy = ReconnectPolicy::default();
  let client = RedisClient::new(config);
  
  // connect to the server, returning a handle to the task that drives the connection
  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;
  let _ = client.flushall(false).await?;
 
  // convert responses to many common Rust types
  let foo: Option<String> = client.get("foo").await?;
  assert!(foo.is_none());
  
  let _: () = client.set("foo", "bar", None, None, false).await?;
  // or use turbofish to declare response types
  println!("Foo: {:?}", client.get<String, _>("foo").await?);
  
  // or use a lower level interface for responses to defer parsing, etc
  let foo: RedisValue = client.get("foo").await?;
  assert_eq!(foo.as_str().unwrap(), "bar");
  
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

* Flexible and generic client interfaces.
* Supports clustered, centralized, and sentinel Redis deployments.
* Optional built-in reconnection logic with multiple backoff policies.
* Publish-Subscribe and keyspace events interfaces.
* Supports transactions.
* Supports Lua scripts. 
* Supports streaming results from the `MONITOR` command. 
* Supports custom commands provided by third party modules. 
* Supports TLS connections.
* Handles cluster rebalancing operations without downtime or errors.
* Supports streaming interfaces for scanning functions.
* Options to automatically [pipeline](https://redis.io/topics/pipelining) requests when possible.
* Automatically retry requests under bad network conditions.
* Support for configuring global settings that can affect performance under different network conditions. Callers can configure backpressure settings, when and how the underlying socket is flushed, and how many times requests are attempted. 
* Built-in tracking for network latency and payload size metrics.
* A client pooling interface to round-robin requests among a pool of clients.
* Built in support for [tracing](https://crates.io/crates/tracing).

## Tracing

This crate supports tracing via the [tracing crate](https://github.com/tokio-rs/tracing). See the [tracing info](./src/trace/README.md) for more information.

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
| metrics                     |    x    | Enable the metrics interface to track overall latency, network latency, and request/response sizes.                                                                  |
| reconnect-on-auth-error     |         | A NOAUTH error is treated the same as a general connection failure and the client will reconnect based on the reconnection policy.           |
| index-map                   |         | Use [IndexMap](https://docs.rs/indexmap/*/indexmap/) instead of [HashMap](https://doc.rust-lang.org/std/collections/struct.HashMap.html) as the backing store for Redis Map types. This is useful for testing and may also be useful for callers.  |
| pool-prefer-active          |    x    | Prefer connected clients over clients in a disconnected state when using the `RedisPool` interface.                                          |
| full-tracing                |         | Enable full [tracing](./src/trace/README.md) support. This can emit a lot of data so a partial tracing feature is also provided.           |
| partial-tracing             |         | Enable partial [tracing](./src/trace/README.md) support, only emitting traces for top level commands and network latency. Note: this has a non-trivial impact on [performance](./bin/pipeline_test/README.md#Examples).  |
| blocking-encoding           |         | Use a blocking task for encoding or decoding frames over a [certain size](./src/modules/globals.rs). This can be useful for clients that send or receive large payloads, but will only work when used with a multi-thread Tokio runtime.  |
| network-logs                |         | Enable TRACE level logging statements that will print out all data sent to or received from the server.  |
| custom-reconnect-errors     |         | Enable an interface for callers to customize the types of errors that should automatically trigger reconnection logic.    |
| monitor                     |         | Enable an interface for running the `MONITOR` command.                                                                    |
| sentinel-client             |         | Enable an interface for communicating directly with Sentinel nodes. This is not necessary to use normal Redis clients behind a sentinel layer.                               |
| sentinel-auth               |         | Enable an interface for using different authentication credentials to sentinel nodes.                                     |

## Environment Variables

|   Name                            | Default | Description                                                                              |
|-----------------------------------|---------|------------------------------------------------------------------------------------------|
| FRED_DISABLE_CERT_VERIFICATION    | `false` | Disable certificate verification when using TLS features.                                |
| FRED_DISABLE_HOST_VERIFICATION    | `false` | Disable host verification when using TLS features.                                       |

These are environment variables because they're dangerous in production and callers should be forced to surface them in a loud and obvious way.

## Pipelining

The caller can toggle [pipelining](https://redis.io/topics/pipelining) via flags on the `RedisConfig` provided to a client to enable automatic pipelining for commands whenever possible. These settings can drastically affect performance on both the server and client, but further performance tuning may be necessary to avoid issues such as using too much memory on the client or server while buffering commands.

See the global performance tuning functions for more information on how to tune backpressure or other relevant settings related to pipelining.

This module also contains a [separate test application](bin/pipeline_test) that can be used to demonstrate the effects of pipelining. This test application also contains some helpful information on how to use the tracing features.

## ACL & Authentication

Prior to the introduction of ACL commands in Redis version 6 clients would authenticate with a single password. If callers are not using the ACL interface, or using Redis version <=5.x, they should configure the client to automatically authenticate by using the `password` field on the `RedisConfig` and leaving the `username` field as `None`. 

If callers are using ACLs and Redis version >=6.x they can configure the client to automatically authenticate by using the `username` and `password` fields on the provided `RedisConfig`. 

**It is required that the authentication information provided to the `RedisConfig` allows the client to run `CLIENT SETNAME` and `CLUSTER NODES`.** Callers can still change users via the `auth` command later, but it recommended to instead use the username and password provided to the `RedisConfig` so that the client can automatically authenticate after reconnecting. 

If this is not possible callers need to ensure that the default user can run the two commands above. Additionally, it is recommended to move any calls to the `auth` command inside the `on_reconnect` block.

## Redis Sentinel

To use the [Redis Sentinel](https://redis.io/topics/sentinel) interface callers should provide a `ServerConfig::Sentinel` variant when creating the client's `RedisConfig`. This should contain the host/port tuples for the known sentinel nodes when first [creating the client](https://redis.io/topics/sentinel-clients). 

The client will automatically update these values in place as sentinel nodes change whenever connections to the primary Redis server close. Callers can inspect these changes with the `client_config` function on any `RedisClient` that uses the sentinel interface.

Note: Sentinel connections will use the same TLS configuration options as the connections to the Redis servers. By default connections will also use the same authentication credentials as well unless the `sentinel-auth` feature is enabled.

Callers can also use the `sentinel-client` feature to communicate directly with Sentinel nodes.

## Customizing Error Handling

The `custom-reconnect-errors` feature enables an interface on the [globals](src/modules/globals.rs) to customize the list of errors that should automatically trigger reconnection logic (if configured). 

In many cases applications respond to Redis errors by logging the error, maybe waiting and reconnecting, and then trying again. Whether to do this often depends on [the prefix](https://github.com/redis/redis/blob/66002530466a45bce85e4930364f1b153c44840b/src/server.c#L2998-L3031) in the error message, and this interface allows callers to specify which errors should be handled this way.

Errors that trigger this can be seen with the [on_error](https://docs.rs/fred/*/fred/client/struct.RedisClient.html#method.on_error) function. 

## Tests

See the [testing documentation](./tests/README.md) for more information.

**Beware: the tests will periodically run `flushall`.**

## Contributing 

See the [contributing](CONTRIBUTING.md) documentation for info on adding new commands.
