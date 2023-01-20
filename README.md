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
  let _ = client.connect();
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
  Ok(())
}
```

See the [examples](examples/README.md) for more.

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
* An optional interface to override DNS resolution logic.
* Optional support for JSON values.

## Build Time Features 

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
| partial-tracing         |         | Enable partial [tracing](./src/trace/README.md) support, only emitting traces for top level commands and network latency. Note: this has a non-trivial impact on performance.                                                                                                       |
| blocking-encoding       |         | Use a blocking task for encoding or decoding frames over a [certain size](./src/modules/globals.rs). This can be useful for clients that send or receive large payloads, but will only work when used with a multi-thread Tokio runtime.                                            |
| network-logs            |         | Enable TRACE level logging statements that will print out all data sent to or received from the server. These are the only logging statements that can ever contain potentially sensitive user data.                                                                                |
| custom-reconnect-errors |         | Enable an interface for callers to customize the types of errors that should automatically trigger reconnection logic.                                                                                                                                                              |
| monitor                 |         | Enable an interface for running the `MONITOR` command.                                                                                                                                                                                                                              |
| sentinel-client         |         | Enable an interface for communicating directly with Sentinel nodes. This is not necessary to use normal Redis clients behind a sentinel layer.                                                                                                                                      |
| sentinel-auth           |         | Enable an interface for using different authentication credentials to sentinel nodes.                                                                                                                                                                                               |
| subscriber-client       |         | Enable an optional subscriber client that manages channel subscription state for callers.                                                                                                                                                                                           |
| serde-json              |         | Enable an interface to automatically convert Redis types to JSON.                                                                                                                                                                                                                   |
| no-client-setname       |         | Disable the automatic `CLIENT SETNAME` command used to associate server logs with client logs.                                                                                                                                                                                      |
| mocks                   |         | Enable a mocking layer interface that can be used to intercept and process commands in tests.                                                                                                                                                                                       |
| dns                     |         | Enable an interface that allows callers to override the DNS lookup logic.                                                                                                                                                                                                           |
| check-unresponsive      |         | Enable additional monitoring to detect unresponsive connections.                                                                                                                                                                                                                    |
| replicas                |         | [Beta] Enable an interface that routes commands to replica nodes.                                                                                                                                                                                                                   |


## Tests

See the [testing documentation](./tests/README.md) for more information.

**Beware: the tests will periodically run `flushall`.**

## Contributing 

See the [contributing](CONTRIBUTING.md) documentation for info on adding new commands.
