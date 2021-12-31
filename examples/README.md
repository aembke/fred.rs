Examples
========

* [Basic](./basic.rs) - Basic client usage.
* [TLS](./tls.rs) - Setting up a client that uses TLS.
* [Globals](./globals.rs) - Read and modify global variables to tune the performance of the clients.
* [Publish-Subscribe](./pubsub.rs) - Use multiple clients together with the pubsub interface in a way that survives network interruptions.
* [Blocking](./blocking.rs) - Use multiple clients with the blocking list interface.
* [Transactions](./transactions.rs) - Use the MULTI/EXEC interface on a client.
* [Lua](./lua.rs) - Use the Lua scripting interface on a client.
* [Scan](./scan.rs) - Use the SCAN interface to scan and read keys.
* [Prometheus](./prometheus.rs) - Use the metrics interface with prometheus.
* [Static Pool](./static_pool.rs) - Use a redis pool that cannot be modified after being created.
* [Resilience](./resilience.rs) - Configure the client to work under bad network conditions or against unreliable servers.
* [Monitor](./monitor.rs) - Process a `MONITOR` stream.
* [Sentinel](./sentinel.rs) - Connect using a sentinel deployment.

Or check out the [tests](../tests/integration) for more examples.