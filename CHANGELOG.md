TLDR
====

* Versions 3.x were focused on upgrading to Tokio 1.x and async/await
* Versions 4.x were focused on ergonomics and testing
* Versions 5.x are focused on feature parity with newer Redis features (streams, RESP3, etc)
* Versions 6.x will be focused on performance. 

# Upcoming Releases

## 5.1.0

* Make transactions easier to use. This includes support for concurrent transactions where commands are buffered in memory until the caller calls EXEC/DISCARD.
* Rewrite parts of the connection layer to improve code reuse between sentinel and non-sentinel clients.
* Consider making the `sentinel-auth` logic a required feature, and remove the feature flag.
* Offer callers an interface to "jump the queue" when sending commands in response to a reconnection event.
* Support Unix domain sockets
* Redis 7.x commands
* Switch to `cargo-nextest`
* Move the custom `MONITOR` parser to `redis-protocol`.
* Any other features I've forgotten.

## 6.0.0

This will be more of an exploratory release that may or may not help performance for callers depending on how they're running the Redis server(s).

* Create additional thread-local client structs that trade `Send` or `Sync` types for the ability to remove (hopefully) all containers that add synchronization overhead.
* Offer callers the option to use fixed-size command channel buffers to reduce the number of allocations.
* Change the command retry interface so retried commands only need one allocation the first time they're encoded.
* Offer callers an interface to change TCP settings when initializing connections.
* Offer callers an interface to override DNS lookup logic.
* Switch to `ArcStr` from `Arc<String>` everywhere it matters.
* Switch from `Arc<RwLock/Mutex<T>>` to `ArcSwap` wherever possible.
* Add benchmarks to CircleCI.
* Hope and pray that somebody implements support for async functions in traits. 

# Past Releases

## 5.0.0

* Rewrite the [protocol parser](https://github.com/aembke/redis-protocol.rs) so it can decode frames without moving or copying the underlying bytes
* Change most command implementations to avoid unnecessary allocations when using static str slices 
* Rewrite the public interface to use different traits for different parts of the redis interface
* Relax some restrictions on certain commands being used in a transaction
* Implement the Streams interface (XADD, XREAD, etc)
* RESP3 support
* Minor perf improvements via the removal of some locks...
* Minor perf regressions from workarounds required to use [async functions with traits](https://smallcultfollowing.com/babysteps/blog/2019/10/26/async-fn-in-traits-are-hard/). In the end it's a wash.
* Move most perf configuration options from `globals` to client-specific config structs
* Add backpressure configuration options to the client config struct
* Fix bugs that can occur when using non-UTF8 strings as keys
* Add the `serde-json` feature
* Handle more complicated failure modes with Redis clusters
* Add a more robust and specialized pubsub subscriber client
* Ergonomics improvements on the public interfaces
* Improve docs
* More tests

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