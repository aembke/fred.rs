
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