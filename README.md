Fred
====

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/aembke/fred.rs/tree/main.svg?style=svg)](https://circleci.com/gh/aembke/fred.rs/tree/main)
[![Crates.io](https://img.shields.io/crates/v/fred.svg)](https://crates.io/crates/fred)
[![API docs](https://docs.rs/fred/badge.svg)](https://docs.rs/fred)

An async Redis client for Rust built on Tokio and Futures. 

## Example 

```rust
use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let perf = PerformanceConfig::default();
  let policy = ReconnectPolicy::default();
  let client = RedisClient::new(config, perf, Some(policy));
  
  // connect to the server, returning a handle to the task that drives the connection
  let jh = client.connect();
  let _ = client.wait_for_connect().await?;
 
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
  let _ = jh.await;
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

* Supports RESP2 and RESP3 protocol modes.
* Supports clustered, centralized, and sentinel Redis deployments.
* Optional built-in reconnection logic with multiple backoff policies.
* Publish-Subscribe and keyspace events interfaces.
* Supports transactions.
* Supports Lua scripts. 
* Supports streaming results from the `MONITOR` command. 
* Supports custom commands provided by third party modules. 
* Supports TLS connections via `native-tls` and/or `rustls`.
* Supports streaming interfaces for scanning functions.
* Options to automatically [pipeline](https://redis.io/topics/pipelining) requests across tasks when possible.
* An interface to manually [pipeline](https://redis.io/topics/pipelining) requests within a task.
* Automatically retry requests under bad network conditions.
* Optional built-in tracking for network latency and payload size metrics.
* An optional client pooling interface to round-robin requests among a pool of clients.
* An optional sentinel client for interacting directly with sentinel nodes to manually fail over servers, etc.
* An optional pubsub subscriber client that will automatically manage channel subscriptions.
* Optional support for JSON values.

**Note: Fred requires Tokio 1.x or above. Actix users must be using 4.x or above as a result.**

## Tracing

This crate supports tracing via the [tracing crate](https://github.com/tokio-rs/tracing). See the [tracing info](./src/trace/README.md) for more information.

This feature is disabled by default, but callers can opt-in via the `full-tracing` or `partial-tracing` features.

## Logging

To enable logs use the environment variable `RUST_LOG` with a value of `trace`, `debug`, `warn`, `error`, or `info`. See the documentation for [env_logger](http://rust-lang-nursery.github.io/log/env_logger/) for more information.

When a client is initialized it will generate a unique client name with a prefix of `fred-`. This name will appear in all logging statements on the client.

## Compile Time Features

| Name                    | Default | Description                                                                                                                                                                                                                                                                         |
|-------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| enable-native-tls       |         | Enable TLS support via [native-tls](https://crates.io/crates/native-tls).                                                                                                                                                                                                           |
| enable-rustls           |         | Enable TLS support via [rustls](https://crates.io/crates/rustls).                                                                                                                                                                                                                   |
| vendored-openssl        |         | Enable the `native-tls/vendored` feature, if possible.                                                                                                                                                                                                                              |
| ignore-auth-error       | x       | Ignore auth errors that occur when a password is supplied but not required.                                                                                                                                                                                                         |
| metrics                 |         | Enable the metrics interface to track overall latency, network latency, and request/response sizes.                                                                                                                                                                                 |
| reconnect-on-auth-error |         | A NOAUTH error is treated the same as a general connection failure and the client will reconnect based on the reconnection policy. This is [recommended](https://github.com/StackExchange/StackExchange.Redis/issues/1273#issuecomment-651823824) if callers are using ElastiCache. |
| pool-prefer-active      | x       | Prefer connected clients over clients in a disconnected state when using the `RedisPool` interface.                                                                                                                                                                                 |
| full-tracing            |         | Enable full [tracing](./src/trace/README.md) support. This can emit a lot of data so a partial tracing feature is also provided.                                                                                                                                                    |
| partial-tracing         |         | Enable partial [tracing](./src/trace/README.md) support, only emitting traces for top level commands and network latency. Note: this has a non-trivial impact on [performance](./bin/pipeline_test/README.md#Examples).                                                             |
| blocking-encoding       |         | Use a blocking task for encoding or decoding frames over a [certain size](./src/modules/globals.rs). This can be useful for clients that send or receive large payloads, but will only work when used with a multi-thread Tokio runtime.                                            |
| network-logs            |         | Enable TRACE level logging statements that will print out all data sent to or received from the server. These are the only logging statements that can ever contain potentially sensitive user data.                                                                                |
| custom-reconnect-errors |         | Enable an interface for callers to customize the types of errors that should automatically trigger reconnection logic.                                                                                                                                                              |
| monitor                 |         | Enable an interface for running the `MONITOR` command.                                                                                                                                                                                                                              |
| sentinel-client         |         | Enable an interface for communicating directly with Sentinel nodes. This is not necessary to use normal Redis clients behind a sentinel layer.                                                                                                                                      |
| sentinel-auth           |         | Enable an interface for using different authentication credentials to sentinel nodes.                                                                                                                                                                                               |
| subscriber-client       |         | Enable a higher level subscriber client that manages channel subscription state for callers.                                                                                                                                                                                        |
| serde-json              |         | Enable an interface to automatically convert Redis types to JSON.                                                                                                                                                                                                                   |
| no-client-setname       |         | Disable the automatic `CLIENT SETNAME` command used to associate server logs with client logs.                                                                                                                                                                                      |
| mocks                   |         | Enable a mocking layer interface that can be used to intercept and process commands in tests.                                                                                                                                                                                       |
| dns                     |         | Enable an interface that allows callers to override the DNS lookup logic.                                                                                                                                                                                                           |

## ACL & Authentication

Prior to the introduction of ACL commands in Redis version 6 clients would authenticate with a single password. If callers are not using the ACL interface, or using Redis version <=5.x, they should configure the client to automatically authenticate by using the `password` field on the `RedisConfig` and leaving the `username` field as `None`. 

If callers are using ACLs and Redis version >=6.x they can configure the client to automatically authenticate by using the `username` and `password` fields on the provided `RedisConfig`. 

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
