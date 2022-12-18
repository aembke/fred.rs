Features
========

## ACL & Authentication

Prior to the introduction of ACL commands in Redis version 6 clients would authenticate with a single password. If callers are not using the ACL interface, or using Redis version <=5.x, they should configure the client to automatically authenticate by using the `password` field on the `RedisConfig` and leaving the `username` field as `None`.

If callers are using ACLs and Redis version >=6.x they can configure the client to automatically authenticate by using the `username` and `password` fields on the provided `RedisConfig`.

Configuring auth parameters via the `RedisConfig` is preferred over manual `AUTH` or `HELLO` commands in an `on_reconnect` block. The client will automatically retry commands that failed due to a connection closing unexpectedly, but this retry logic will run before any logic inside an `on_reconnect` block.

## Redis Sentinel

To use the [Redis Sentinel](https://redis.io/topics/sentinel) interface callers should provide a `ServerConfig::Sentinel` variant when creating the client's `RedisConfig`. This should contain the host/port tuples for the known sentinel nodes when first [creating the client](https://redis.io/topics/sentinel-clients).

The client will automatically update these values in place as sentinel nodes change whenever connections to the primary Redis server close. Callers can inspect these changes with the `client_config` function on any `RedisClient` that uses the sentinel interface.

Note: Sentinel connections will use the same TLS configuration options as the connections to the Redis servers. By default, connections will also use the same authentication credentials unless the `sentinel-auth` feature is enabled.

Callers can also use the `sentinel-client` feature to communicate directly with Sentinel nodes.

## Customizing Error Handling

The `custom-reconnect-errors` feature enables an interface on the [globals](src/modules/globals.rs) to customize the list of errors that should automatically trigger reconnection logic (if configured).

In many cases applications respond to Redis errors by logging the error, maybe waiting and reconnecting, and then trying again. Whether to do this often depends on [the prefix](https://github.com/redis/redis/blob/66002530466a45bce85e4930364f1b153c44840b/src/server.c#L2998-L3031) in the error message, and this interface allows callers to specify which errors should be handled this way.

Errors that trigger this can be seen with the [on_error](https://docs.rs/fred/*/fred/client/struct.RedisClient.html#method.on_error) function. 

## Reconnect On Auth Error

The `reconnect-on-auth-error` feature enables additional logic to treat `NOAUTH` errors as if the connection had closed unexpectedly. 

This can be helpful when using managed services like Elasticache. Some of these services may fail over cluster nodes or apply configuration changes without closing connections, which can be especially problematic for clients that rely on events at the network level to detect major server topology changes. 

See [this issue](https://github.com/StackExchange/StackExchange.Redis/issues/1273#issuecomment-651823824) for more information.

## Blocking Encoding

The `blocking-encoding` feature can enable additional logic to encode or decode frames within a "blocking" Tokio task. This can be helpful for clients that send or receive large payloads and do not wish to block important threads for long periods of time. 

This feature relies on the [block_in_place](https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html) function since the underlying codec interface does not support `async` functions. However, this function requires that the caller use a `multi-thread` Tokio runtime. 

The `globals::set_blocking_encode_threshold` function can be used to set the threshold for when this interface should be used. 

## Network Logs

In many cases it can be problematic for logs to contain potentially sensitive application data. However, there are several scenarios where it may be beneficial to view the actual network traffic sent to or received from the server. The `network-logs` feature will enable additional `TRACE` logging statements to print the contents of all frames on the codec interface.

If this feature is disabled the client will **never** log any command arguments or responses. However, the default logging statements may still contain hostnames or command names.

To enable the most verbose debugging settings callers should use `RUST_LOG=fred=trace ... --features "debug-ids network-logs"` to see everything the client is doing.

## Subscriber Client

The `subscriber-client` feature enables an additional interface for Pubsub clients. It is very common for callers to repeat the same boilerplate subscription logic when connections close unexpectedly. This client will automatically track channel subscriptions and resubscribe as needed when connections close. 

## Check Unresponsive

There are two failure modes to consider with the underlying TCP connections to the Redis server(s):

* The connection closes unexpectedly for some reason.
* The connection becomes unresponsive but does not close.

The `check-unresponsive` feature enables additional monitoring to detect the second failure mode. This failure mode is less common but can be especially problematic for clients that wish to "hide" connection or timeout errors from callers. Callers can simulate this failure mode via `docker pause` (if using docker) or by sending a `DEBUG sleep <seconds>` command. 

The `network_timeout_ms` field on the `PerformanceConfig` struct can be used to set a response timeout that determines when a connection should be considered unresponsive. This configuration option differs from the `default_command_timeout_ms` field in several ways:

* Commands that exceed the `default_command_timeout_ms` will be discarded and a `Timeout` error will be returned to the caller. 
* Commands that exceed the `network_timeout_ms` will trigger the reconnection logic. The command will be retried until it exceeds the `max_command_attempts` count. The caller will not see a `Timeout` error.

Callers can use both configuration options together but should set their values based on their application's tolerance for `Timeout` errors.

The `globals::set_unresponsive_interval_ms()` function can be used to tune how frequently the client will check for unresponsive connections.