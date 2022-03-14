TLDR
====

* Versions 3.x were focused on upgrading to Tokio 1.x and async/await
* Versions 4.x were focused on ergonomics and testing
* Versions 5.x are focused on feature parity with newer Redis features (streams, RESP3, etc)
* Versions 6.x will be focused on performance. 

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