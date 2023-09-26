## 7.0.0

* Added a new client [builder](src/types/builder.rs) and configuration interface.
* Reworked or removed the majority of the `globals` interface.
* Support multiple IP addresses in the `Resolve` interface.
* Add `with_options` command configuration interface. 
* Replaced the `no-client-setname` feature flag with `auto-client-setname`.
* Add an interface to configure TCP socket options.
* Removed the automatic `serde_json::Value` -> `RedisValue` type conversion logic. 
  * This unintentionally introduced some ambiguity on certain interfaces.
  * The `RedisValue` -> `serde_json::Value` type conversion logic was not changed.
* Reworked the majority of the `RedisPool` interface. 
* Moved and refactored the `on_*` functions into a new `EventInterface`.
* Fixed bugs with the `Replica` routing implementation.
* Fixed bugs related to parsing single-element arrays.
* Changed several `FromRedis` type conversion rules. See below or the `FromRedis` docs for more information.
* Add a [RedisJSON](https://github.com/RedisJSON/RedisJSON/) interface.

### Upgrading from 6.x

Notable interface changes:

* `ArcStr` has been replaced with `bytes_utils::Str`.
* Timeout arguments or fields now all use `std::time::Duration`.
* Many of the old global or performance config values can now be set on individual commands via the `with_options` interface.
* The `RedisPool` interface now directly implements `ClientLike` rather than relying on `Deref` shenanigans.
* The `on_*` event functions were moved and renamed. Reconnection events now include the associated `Server`.
* The `tls_server_name` field on `Server` is now properly gated by the TLS feature flags.
* Mocks are now optional even when the feature flag is enabled.

Notable implementation Changes:

* The `Pipeline` struct can now be reused. Calling `all`, `last`, or `try_all` no longer drains the inner command buffer.
* Many of the default timeout values have been lowered significantly, often from 60 sec to 10 sec.
* In earlier versions the `FromRedis` trait implemented a few inconsistent or ambiguous type conversions policies. 
  * Most of these were consolidated under the `default-nil-types` feature flag.
  * It is recommended that callers review the updated `FromRedis` docs or see the unit tests in [responses](src/modules/response.rs).
* The `connect` function can now be called more than once to force reset all client state.

## 6.3.2

* Fix a bug with connection errors unexpectedly ending the connection task.

## 6.3.1

* Update various dependencies
* Move `pretty-env-logger` to `dev-dependencies`
* Update rustfmt.toml

## 6.3.0

* Fix cluster replica discovery with Elasticache
* Fix cluster replica `READONLY` usage
* Fix compilation error with `full-tracing`
* Support `Vec<(T1, T2, ...)>` with `FromRedis`

## 6.2.1

* Fix cluster failover with paused nodes

## 6.2.0

* Add `Pipeline::try_all`
* Add missing pubsub commands 
* Improve docs, examples

## 6.1.0

* Add a [client tracking](https://redis.io/docs/manual/client-side-caching/) interface.
* Add a global config value for broadcast channel capacity.
* Add an interface to interact with individual cluster nodes.
* Fix missing `impl StreamInterface for Transaction`
* Add all `RedisClient` command traits to `SubscriberClient`

## 6.0.0

* Refactored the connection and protocol layer.
* Add a manual `Pipeline` interface for pipelining commands within a task.
* Add a `Replica` client for interacting with replica nodes.
* Rework the `Transaction` interface to buffer commands in memory before EXEC/DISCARD.
* Rework the cluster discovery and failover implementation. 
* Rework the MOVED/ASK implementation to more quickly and reliably follow cluster redirects.
* Rework the sentinel interface to more reliably handle failover scenarios.
* Fix several bugs related to detecting closed connections.
* Support the `functions` interface.
* Add `Script`, `Library`, and `Function` structs. 
* Add `Message` and `MessageKind` pubsub structs. 
* Add a DNS configuration interface.
* Rework the `native-tls` interface to support fully customizable TLS configurations.
* Add `rustls` support.
  * Note: both TLS feature flags can be used at the same time.
* Add a mocking layer interface.

### Updating from 5.x

Potentially breaking changes in 6.x:

* Switched from `(String, u16)` tuples to the `Server` struct for all server identifiers.
* New TLS feature flags: `enable-rustls` and `enable-native-tls`.
  * `vendored-tls` is now `vendored-openssl`
* New TLS configuration process: see the [example](examples/tls.rs).
* New [transaction](examples/transactions.rs) interface. 
  * `Transaction` commands are now buffered in memory before calling `exec()` or `discard()`.
* New backpressure configuration options, most notably the `Drain` policy. This is now the default.
* Changed the type and fields on `BackpressurePolicy::Sleep`.
* New [custom command interface](examples/custom.rs) for managing cluster hash slots.
* Removed or renamed some fields on `RedisConfig`.
* Changed the pubsub receiver interface to use `Message` instead of `(String, RedisValue)` tuples.
* Changed the `on_*` family of functions to return a [BroadcastReceiver](https://docs.rs/tokio/latest/tokio/sync/broadcast/struct.Receiver.html).
* The `FromRedis` trait converts `RedisValue::Null` to `"nil"` with `String` and `Str`.

## 5.2.0

* Reduce number of `tokio` features
* Use 6379 as default cluster port in `from_url`
* Fix RESP3 auth error (https://github.com/aembke/fred.rs/issues/54)

## 5.2.0

* Reduce number of `tokio` features
* Use 6379 as default cluster port in `from_url`
* Fix RESP3 auth error (https://github.com/aembke/fred.rs/issues/54)

## 5.2.0

* Reduce number of `tokio` features
* Use 6379 as default cluster port in `from_url`
* Fix RESP3 auth error (https://github.com/aembke/fred.rs/issues/54)

## 5.1.0

* Update `redis-protocol` and `nom`
* Add `no-client-setname` feature flag

## 5.0.0

* Bug fixes
* Support URL parsing into a `RedisConfig`
* Update `bzpopmin` and `bzpopmax` return type
* Remove unimplemented `mocks` feature

## 5.0.0-beta.1

* Rewrite the [protocol parser](https://github.com/aembke/redis-protocol.rs) so it can decode frames without moving or copying the underlying bytes
* Change most command implementations to avoid unnecessary allocations when using static str slices 
* Rewrite the public interface to use different traits for different parts of the redis interface
* Relax some restrictions on certain commands being used in a transaction
* Implement the Streams interface (XADD, XREAD, etc)
* RESP3 support
* Move most perf configuration options from `globals` to client-specific config structs
* Add backpressure configuration options to the client config struct
* Fix bugs that can occur when using non-UTF8 byte arrays as keys
* Add the `serde-json` feature
* Handle more complicated failure modes with Redis clusters
* Add a more robust and specialized pubsub subscriber client
* Ergonomics improvements on the public interfaces
* Improve docs
* More tests

## 4.3.2

* Fix https://github.com/aembke/fred.rs/issues/27
* Fix https://github.com/aembke/fred.rs/issues/26

## 4.3.1

* Fix authentication bug with `sentinel-auth` tests
* Update tests and CI config for `sentinel-auth` feature
* Add more testing scripts, update docs
* Switch to CircleCI

## 4.3.0

* Add `sentinel-auth` feature

## 4.2.3

* Add `NotFound` error kind variant
* Use `NotFound` errors when casting `nil` server responses to non-nullable types

## 4.2.2

* Remove some unnecessary async locks
* Fix client pool `wait_for_connect` implementation

## 4.2.1

* Fix https://github.com/aembke/fred.rs/issues/11

## 4.2.0

* Support Sentinel clients
* Fix broken doc links

## 4.1.0

* Support Redis Sentinel
* Sentinel tests
* Move metrics behind compiler flag

## 4.0.0

* Add generic response interface.
* Add tests

## 3.0.0

See below.

## 3.0.0-beta.4

* Add support for the `MONITOR` command.

## 3.0.0-beta.3

* Redo cluster state change implementation to diff `CLUSTER NODES` changes
* MOVED/ASK errors no longer initiate reconnection logic
* Fix chaos monkey tests

## 3.0.0-beta.2

* Extend and refactor RedisConfig options 
* Change RedisKey to work with bytes, not str 
* Support unblocking clients with a control connection 
* First draft of chaos monkey tests 
* Custom reconnect errors feature 

## 3.0.0-beta.1

* Rewrite to use async/await
* Add Lua support
* Add transaction support
* Add hyperloglog, geo, acl, memory, slowlog, and cluster command support
* Add tests 
* Add [pipeline_test](bin/pipeline_test) application

## < 3.0.0

See the old repository at [azuqua/fred.rs](https://github.com/azuqua/fred.rs).