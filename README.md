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
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;
 
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
* Optional reconnection logic with multiple backoff policies.
* Publish-Subscribe and keyspace events interfaces.
* A round-robin client pooling interface.
* Lua [scripts](https://redis.io/docs/interact/programmability/eval-intro/) or [functions](https://redis.io/docs/interact/programmability/functions-intro/). 
* Streaming results from the `MONITOR` command. 
* Custom commands.
* Streaming interfaces for scanning functions.
* [Transactions](https://redis.io/docs/interact/transactions/)
* [Pipelining](https://redis.io/topics/pipelining)
* [Client Tracking](https://redis.io/docs/manual/client-side-caching/)
* An optional [RedisJSON](https://github.com/RedisJSON/RedisJSON) interface.
* A round-robin cluster replica routing interface.
* An optional pubsub subscriber client that will automatically manage channel subscriptions.
* [Tracing](https://github.com/tokio-rs/tracing)

## Build Features 

| Name                    | Default | Description                                                                                                                                                                                          |
|-------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| transactions            | x       | Enable a [Transaction](https://redis.io/docs/interact/transactions/) interface.                                                                                                                      |
| enable-native-tls       |         | Enable TLS support via [native-tls](https://crates.io/crates/native-tls).                                                                                                                            |
| enable-rustls           |         | Enable TLS support via [rustls](https://crates.io/crates/rustls).                                                                                                                                    |
| vendored-openssl        |         | Enable the `native-tls/vendored` feature.                                                                                                                                                            |
| metrics                 |         | Enable the metrics interface to track overall latency, network latency, and request/response sizes.                                                                                                  |
| full-tracing            |         | Enable full [tracing](./src/trace/README.md) support. This can emit a lot of data.                                                                                                                   |
| partial-tracing         |         | Enable partial [tracing](./src/trace/README.md) support, only emitting traces for top level commands and network latency.                                                                            |
| blocking-encoding       |         | Use a blocking task for encoding or decoding frames. This can be useful for clients that send or receive large payloads, but requires a multi-thread Tokio runtime.                                  |
| network-logs            |         | Enable TRACE level logging statements that will print out all data sent to or received from the server. These are the only logging statements that can ever contain potentially sensitive user data. |
| custom-reconnect-errors |         | Enable an interface for callers to customize the types of errors that should automatically trigger reconnection logic.                                                                               |
| monitor                 |         | Enable an interface for running the `MONITOR` command.                                                                                                                                               |
| sentinel-client         |         | Enable an interface for communicating directly with Sentinel nodes. This is not necessary to use normal Redis clients behind a sentinel layer.                                                       |
| sentinel-auth           |         | Enable an interface for using different authentication credentials to sentinel nodes.                                                                                                                |
| subscriber-client       |         | Enable a subscriber client interface that manages channel subscription state for callers.                                                                                                            |
| serde-json              |         | Enable an interface to automatically convert Redis types to JSON via `serde-json`.                                                                                                                   |
| mocks                   |         | Enable a mocking layer interface that can be used to intercept and process commands in tests.                                                                                                        |
| dns                     |         | Enable an interface that allows callers to override the DNS lookup logic.                                                                                                                            |
| replicas                |         | Enable an interface that routes commands to replica nodes.                                                                                                                                           |
| client-tracking         |         | Enable a [client tracking](https://redis.io/docs/manual/client-side-caching/) interface.                                                                                                             |
| default-nil-types       |         | Enable a looser parsing interface for `nil` values.                                                                                                                                                  |
| redis-json              |         | Enable an interface for [RedisJSON](https://github.com/RedisJSON/RedisJSON).                                                                                                                         |
| codec                   |         | Enable a lower level framed codec interface for use with [tokio-util](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html).                                                                |
| sha-1                   |         | Enable an interface for hashing Lua scripts.                                                                                                                                                         |
| unix-sockets            |         | Enable Unix socket support.                                                                                                                                                                          |
