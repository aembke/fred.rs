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