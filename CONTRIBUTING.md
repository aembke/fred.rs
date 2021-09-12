Contributing
===========

This document gives some background on how the library is structured and how to contribute.

# General

* Use 2 spaces instead of tabs.
* Run rustfmt before submitting any changes.
* Clean up any compiler warnings.

## TODO List

* Streams
* [WIP] RESP3 support (see [redis-protocol](https://crates.io/crates/redis-protocol)). 
* Redis version 7.x commands
* Mocking layer
* Gate commands unique to a particular Redis version behind build time features.
* [WIP] Support custom DNS resolvers on the client.

If you'd like to contribute to any of the above features feel free to reach out

## Design 

This section covers some useful design considerations and assumptions that went into this module. 

* Debugging Redis issues late at night is not fun. If you find yourself adding log lines that help debug an issue please clean them up and leave them in the code. The one exception is that logs should **never** include potentially sensitive user data (i.e. don't commit changes that log full requests or responses). The `network-logs` feature can enable sensitive logs if needed.
* The `RedisClient` struct needs to be `Send + Sync` to work effectively with Tokio.
* The `RedisClient` struct should be fast and cheap to `Clone`.
* The primary command interfaces should be as flexible as possible via use of `Into` and `TryInto` for arguments.
* Assume nearly any command might be used in the context of a transaction, and so it could return a `QUEUED` response even if the docs only mention bulk strings, arrays, etc. There are some exceptions to this (blocking commands, etc) where return values could be typed to exactly match the rust-equivalent type of the return value, but generally speaking every command should return a `RedisValue`. 

There are other Redis libraries for Rust that have different goals, but the main goal of this library is to provide callers with a high level interface that abstracts away everything to do with safe and reliable connection management. This also includes some optional features to automatically handle common use cases around error handling, reconnection & backoff, retry, metrics, etc.

### Connection Management

This section covers how code close to the network layer works.

#### Redis Codec

* This library uses the [redis-protocol](https://github.com/aembke/redis-protocol.rs) crate to encode and decode commands via a `RedisCodec` from the [codec.rs](src/protocol/codec.rs) module. Each `RedisCodec` instance also tracks payload size metrics for encoded and decoded frames.
* Connections are created by combining a `RedisCodec` with a [TcpStream](https://docs.rs/tokio/1.9.0/tokio/net/struct.TcpStream.html) or [TlsStream](https://docs.rs/tokio-native-tls/0.3.0/tokio_native_tls/struct.TlsStream.html) 
to create a [Framed](https://docs.rs/tokio-util/0.6.7/tokio_util/codec/struct.Framed.html) stream. 
* `Framed` streams are [split](https://docs.rs/tokio/1.9.0/tokio/net/struct.TcpStream.html#method.split) after being created. The writer half is stored on a struct that processes and routes commands, and the reader half is used in a background task that processes the stream of frames coming from the socket. 
* If TLS features are disabled the TLS type aliases become references to the TCP types. 
* The writer half exposes 2 functions for sending frames to the socket: [feed](https://docs.rs/futures/0.3.16/futures/sink/trait.SinkExt.html#method.feed) and [send](https://docs.rs/futures/0.3.16/futures/sink/trait.SinkExt.html#method.send). `send` flushes the socket and `feed` does not. The client will use `send` under any of the following conditions:
  1. There are no queued commands following the current command.
  2. The [global max number of fed commands](src/globals.rs) was reached. In this case the client will use `send` once and reset the feed counter.
  3. The current command is QUIT or SHUTDOWN.
  4. The current command ends a transaction.
  5. The client has pipelining disabled.
  
#### Clustered Connections

When establishing fresh connections to the server(s), or rebuilding existing connections, the client does the following:

1. Try to connect to any of the nodes included in the client's `RedisConfig`.
2. Upon successfully connecting and authenticating run the `CLUSTER NODES` command.
3. Parse the response and build a `ClusterKeyCache` struct.
4. Connect and authenticate to all of the primary/main nodes in the cluster.
5. [Split](https://docs.rs/tokio/1.9.0/tokio/net/struct.TcpStream.html#method.split) each connection, storing the writer half on the `Multiplexer` and spawning a background task to read from the reader half.

The `ClusterKeyCache` struct does the following:

1. Parses the `CLUSTER NODES` response, building out a sorted array of `SlotRange` structs for each slot range in the cluster. Each `SlotRange` contains a reference to the server that owns the hash slot range and the starting and ending slots.
2. Provides an interface for mapping a Redis key to a `SlotRange`. This interface uses the [redis_keyslot](https://docs.rs/redis-protocol/2.0.0/redis_protocol/fn.redis_keyslot.html) mapping function from the `redis_protocol` crate to hash a Redis key, and then a binary search of the `SlotRange` array.
3. Provides interfaces for looking up a random node in the cluster and rebuilding the existing cache in place.

When writing a command to a Redis cluster the client does the following:

1. Hash the command's key if it has a key, otherwise pick a random cluster node.
2. Find the associated slot range for the key's hash slot.
3. Use the server ID of the hash slot range from step 2 to look up the writer half of the socket connected to that node.
4. Convert the command to a [Frame](https://docs.rs/redis-protocol/2.0.0/redis_protocol/types/enum.Frame.html).
5. Push the command onto the back of a queue of commands attached to the socket. This will be covered more in the pipelining section.
6. Pass the frame from step 4 to the `Framed` sink on top of the socket writer half from step 3, possibly flushing the socket in the process.

### Multiplexer

Redis allows clients to switch between several states:

* The client can be in a pipelined request-response state.
* The client can be in a non-pipelined request-response state.
* The client can be in a blocking request-response state.
* The client can be in a subscriber state for pubsub or keyspace event messages.

In order to support use cases where a client may switch between these states at any time this library implements a struct called the `Multiplexer` that manages all connection-related state. The
`Multiplexer` struct is not directly attached to a `RedisClient`, but instead exists as a part of a separate Tokio task that processes commands from the client. 

#### State

The `Multiplexer` struct stores the following state:

* A connection map that maps server IDs to `Framed` sinks, or just one `Framed` sink when connected to a centralized Redis deployment.
* A map that maps server IDs to a queue of in-flight commands (`VecDeque<RedisCommand>`). This is a different map/queue because it has different locking requirements than the connection map.
* A [broadcast](https://docs.rs/tokio/1.9.0/tokio/sync/broadcast/index.html) sender used to broadcast connection issues from either the reader or writer half of the socket.
* An instance of a `ClusterKeyCache` used to route commands to specific nodes in the cluster, if necessary.
* An `Arc<RedisClientInner>`, used to communicate messages back to the client when a connection dies, state changes, pubsub messages are received, etc.

#### Write Path

All commands from a `RedisClient` instance move through one [channel](https://docs.rs/tokio/1.9.0/tokio/sync/mpsc/fn.unbounded_channel.html) where the client instance writes to the channel 
and the `Multiplexer` reads from the channel. Clones of a `RedisClient` all share the same command channel, so cloning a `RedisClient` mostly boils down to cloning an `Arc<UnboundedSender>`. 
The rate at which commands are processed from this channel depends on the client's settings. 

When the client sends a command to the server the following operations occur:

1. The client prepares the command, creating a `Vec<RedisValue>` array of arguments.
2. The client attaches a [oneshot](https://docs.rs/tokio/1.9.0/tokio/sync/oneshot/index.html) sender to the command on which the response will be sent.
3. The client acquires a _read_ lock on the command channel and writes the command to this channel.
4. The client calls `await` on the receiver half of the oneshot channel from step 2.
5. Some time later the `Multiplexer` receives the command from the command stream running in a separate Tokio task.
6. The `Multiplexer` checks the command's flags to determine if it makes sense to send in the current connection context.
7. The `Multiplexer` checks if the command should be pipelined or not. If not it attaches another oneshot sender to the command.
8. The `Multiplexer` hashes the command key (if necessary), looks up the socket connected to the node that should receive the command, and writes the command to the socket.
9. If the command cannot be written due to backpressure or network connectivity issues the `Multiplexer` either calls [sleep](https://docs.rs/tokio/1.9.0/tokio/time/fn.sleep.html) or rebuilds the connections, replays any in-flight commands, and tries again.
10. If writing the command succeeds the `Multiplexer` decides to wait on a response or not based on the checks from step 7. If not it moves on to the next command without waiting on a response.
11. Some time later a response is received by the reader task which forwards the response to the oneshot channels from step 2 and step 7 (if necessary). 
12. When the future from step 4 resolves the client converts the response frame(s) to the correct output types for the Redis command and returns them to the caller.

Contributors only need to consider steps 1 and 12 when implementing commands - the other steps are the same regardless of the command.

#### Read Path

Once a connection is established to a Redis server the client splits the connection into separate reader and writer halves. The writer half is covered above, and this section covers how the reader half is used.  

The reader half processes frames asynchronously with respect to the writer half via a separate Tokio task. After the socket is split the client spawns another Tokio task where this task has access to a shallow clone of the `Multiplexer` struct. 

Once a connection is established the `Multiplexer` does the following:

1. Split the connection. The writer half is covered above.
2. Spawn a task with access to the reader half, a reference to the server ID to which the reader half is connected, and a shallow clone of the `Multiplexer`.
3. Convert the reader half to a `Stream`, calling [try_fold](https://docs.rs/futures/0.3.16/futures/stream/struct.TryFold.html) on it in the process. While this does mean the stream is processed in series the reader task never `awaits` a future so there wouldn't be any benefit of processing the stream concurrently on an event loop. By processing the stream in series it also makes it very easy to handle situations where the command should be retried, or reconnection needs to occur, since the reader task can just put the command back at the front of the in-flight queue without worrying about another task having popped from the queue in the meantime.

Inside the `try_fold` loop the reader task does the following:

1. Check if the incoming frame is a pubsub message, keyspace event, or some other out-of-band message, and if so forward the message onto any of the appropriate channels.
2. Check if the incoming frame is the response to a command. If so then determine whether more responses are necessary, or if not start processing the response. 
3. Link the response to the command at the front of the in-flight command queue, popping the command off the queue in the process.
4. Take the response oneshot channel sender from the command, and forward the response frame to this channel.

If the `try_fold` stream closes unexpectedly the reader task will broadcast a message on the `Multiplexer` connection error broadcast channel. This will then trigger reconnection and replay logic in a separate task.

The response handling logic is in the [responses.rs](src/multiplexer/responses.rs) file.

### Tracing

Automatic tracing in client libraries can be very useful. However, Redis is fast enough that emitting trace data for a Redis client can result in a ton of network traffic to the tracing collector. 

[This document](src/tracing/README.md) covers what is traced and how these relate to the two optional tracing features: `full-tracing` and `partial-tracing`. While writing and testing this library I often saw tracing exporter errors due to having filled up the buffer between the subscriber and exporter. As a result the tracing logic was separated into the two features mentioned earlier. Callers can see a lot of benefit just from the `partial-tracing` feature, but it may also be beneficial to enable `full-tracing` while debugging a difficult issue. However, callers that use `full-tracing` should expect to spend some time tuning their subscriber and exporter settings to handle a lot of tracing output.

#### Performance

One thing to note with the tracing features is that they have a big impact on performance. For example, using the included `pipeline_test` module in this app:

```
$ RUST_LOG=pipeline_test=debug cargo run --release --features "partial-tracing" -- -c 300000 -C 20 pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 300000 -C 20 pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 300000, tasks: 20, host: "127.0.0.1", port: 6379, pipeline: true }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 300000 operations in: 4.951572778s. Throughput: 60593 req/sec

$ RUST_LOG=pipeline_test=debug cargo run --release -- -c 300000 -C 20 pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 300000 -C 20 pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 300000, tasks: 20, host: "127.0.0.1", port: 6379, pipeline: true }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 300000 operations in: 2.495155038s. Throughput: 120240 req/sec
```

Note that in the first example above the tracing data isn't even being emitted to the collector (the sampler is `AlwaysOff`). Just including the tracing logic to add and track spans is enough to cut performance in half.

Many applications are not bounded by Redis throughput and so enabling the tracing features likely won't have any noticeable effect. However, the tracing features are opt-in for callers because of this performance impact. Callers will have to weigh the significant benefits of tracing against the performance loss to their application's use cases.

#### Request-Response

All commands depend on a utility function called `request_response` in the [command utils](src/utils.rs). This function implements tracing generically so that developers adding commands don't need to add any tracing logic to individual command functions. 

One of the arguments to the `request_response` utility is a closure that should be used to format and prepare any arguments. This is implemented as a closure so that the library can trace the time spent in this function without requiring callers to write the same tracing boilerplate in each command function. 

This pattern is used in most [commands](src/commands), but in some commands the preprocessing is so trivial that the closure essentially becomes a no-op. However it's good practice to move as much of the preprocessing logic into this closure as possible so this time shows up in traces.

### Command Interface

Individual commands are represented by the `RedisCommand` struct and `RedisCommandKind` enum in the [protocol types](src/protocol/types.rs) file. All commands have the same general structure, represented by the `RedisCommand` struct, while command-specific data is stored in the associated `RedisCommandKind` enum variant.

There are several types of commands that are handled differently by the `Multiplexer`:

* Blocking commands
* Compound commands (any command with a subcommand separated by a space in the command string).
* Commands that expect more than one distinct array of response frames (`PSUBSCRIBE`, etc).
* Scanning commands.
* Commands that start or end a transaction.
* Special commands that don't map directly to a Redis command (`flushall_cluster`, `split_cluster`, etc).

All commands also operate on `RedisValue` enums as arguments, even for keys or other types of values. Contributors should use `Into` or `TryInto` as much as possible to leverage the type conversion utilities that exist in the [top level types file](src/types.rs). 

#### Transactions

One thing to consider when implementing any command is how that command will function in the context of a transaction. The `RedisValue` enum supports a `Queued` variant to make this easy, but not all commands should be allowed while inside a transaction. When a command is not allowed inside a transaction developers can return types that more closely match the command's return values instead of a generic `RedisValue`. 

However, most commands should work inside a transaction, so most command functions should be written to just return a `RedisValue`.

If a command should not work inside a transaction then the command should use the `disallow_during_transaction` utility function before calling the command function. Any command function that does not return a `RedisValue` must perform this check.

# Adding Commands

This section will cover how to add new commands to the client.

## New Commands

When adding new commands a few new things often need to be added to the [protocol types](src/protocol/types.rs) file.

1. Add a variant to the `RedisCommandKind` enum for the command. For most commands this variant will be empty.
2. Add the string representation of the new variant to the `to_str_debug` function. This is what will be used in tracing fields and log lines.
3. Add the first word of the string representation of the command to the `cmd_str` function.
4. If the command is a compound command add the subcommand string to the `subcommand_str` function. If not then skip this step.
5. If the command is a blocking command add it to the `is_blocking_command` function's match statement.

## Command Files

Commands are organized in the [commands](src/commands) folder by their category.

* All private command functions in this folder take their first argument as a `&Arc<RedisClientInner>`. This struct contains all the necessary state for any command.
* Commands should take generic arguments to be flexible to the caller. Use `Into<RedisKey>`, `Into<RedisValue>`, etc. There are quite a few helper structs in the [types](src/types.rs) file to make this easier. 
* Some helpful command function generation macros exist in the [command mod.rs](src/commands/mod.rs) file to remove boilerplate for simple commands.
* All commands should use the `request_response` utility function from the [top level utils file](src/utils.rs).
* Private command functions are responsible for preparing their arguments array and converting the response frame to the appropriate return value type.
* It should not be necessary to add any tracing logic to individual command functions.

## Type Conversions

There are 2 functions in the [protocol utils](src/protocol/utils.rs) for converting response frames into `RedisValue` enums. 

* `frame_to_results` - Converts an arbitrarily nested response frame into an arbitrarily nested `RedisValue`, including support for `QUEUED` responses during a transaction. 
* `frame_to_single_result` - The same as `frame_to_results`, but with an added validation layer that only allows for non-nested `RedisValue` variants. This is useful to detect unexpected protocol errors if a command should only return a `BulkString` but receives an `Array` instead.

Both of these functions will automatically check for error frames and will generate the appropriate `RedisError`, if necessary.

**Both of these functions will automatically convert single-element response arrays to the first element in the array.** This is done because RESP2 sends all responses as an array of bulk strings, even when the response only contains one element in the array. It's up to the developer to consider when an array is an appropriate return type for a command.

There are also some utility functions for converting to other data types:

* `frame_to_map` - Convert a frame representing an array of nested frames with an even number of elements to a `RedisMap`.
* `array_to_map` - Convert an array with an even number of `RedisValue` structs to a `RedisMap`.
* `frame_to_error` - Convert a `Frame` to a `RedisError`.

... and some others. 

## Public Interface

Once the private interface has been implemented the same function needs to be exposed on the `RedisClient` struct. 

1. Copy or create the same function signature in the `RedisClient` [struct](src/client.rs), but change the first argument to `&self`.
2. The Redis interface is huge, so please keep this file organized.
3. Use the `disallow_during_transaction` utility in this function, if necessary.
4. Call the private function from this public function, using `&self.inner` as the first argument.  
5. Copy or write documentation for the command in this file. Some docs are quite long so most command docs simply include the first few sentences or a high level overview of the command.
6. Include a link to the full docs for the command from the [Redis docs website](https://redis.io/commands).

Moving from the private to public interface is a bit tedious, so I'm looking for better ways to do this. 

## Example

This example shows how to add `MSET` to the commands.

1. Add the new variant to the `RedisCommandKind` enum, if needed.

```rust
pub enum RedisCommandKind {
  // ...
  Mset,
  // ...
}

impl RedisCommandKind {
  
  // ..
  
  pub fn to_str_debug(&self) -> &'static str {
    match *self {
      // ..
      RedisCommandKind::Mset => "MSET",
      // ..
    }
  }
  
  // ..
  
  pub fn cmd_str(&self) -> &'static str {
    match *self {
      // .. 
      RedisCommandKind::Mset => "MSET"
      // ..
    }
  }
  
  // ..
}
```

2. Create the private function implementing the command in [src/commands/keys.rs](src/commands/keys.rs).

```rust
pub async fn mset<V>(inner: &Arc<RedisClientInner>, values: V) -> Result<RedisValue, RedisError>
where 
  V: Into<RedisMap>,
{
  let values = values.into();
  if values.len() == 0 {
    return Err(RedisError::new(
      RedisErrorKind::InvalidArgument,
      "Values cannot be empty.",
    ));
  }

  let frame = utils::request_response(inner, move || {
    // this closure will appear in traces
    let mut args = Vec::with_capacity(values.len() * 2);

    for (key, value) in values.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }

    Ok((RedisCommandKind::Mset, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
```

3. Create the public function in the [RedisClient](src/client.rs) struct.

```rust

// ...

impl RedisClient {
 
  // ...

  /// Sets the given keys to their respective values.
  ///
  /// <https://redis.io/commands/mset>
  pub async fn mset<V>(&self, values: V) -> Result<RedisValue, RedisError> 
  where
    V: Into<RedisMap>,
  {
    commands::keys::mset(&self.inner, values).await
  }
  
  // ...
}
```

# Adding Tests

Integration tests are in the [tests/integration](tests/integration) folder organized by category. See the tests [README](tests/README.md) for more information.

Using `MSET` as an example:

1. Write tests in the [keys](tests/integration/keys/mod.rs) file.

```rust
pub async fn should_mset_a_non_empty_map(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  // macro to panic if a value isn't nil/None
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");
  check_null!(client, "c{1}");

  let mut map: HashMap<String, RedisValue> = HashMap::new();
  // MSET args all have to map to the same cluster node
  map.insert("a{1}".into(), 1.into());
  map.insert("b{1}".into(), 2.into());
  map.insert("c{1}".into(), 3.into());

  let _ = client.mset(map).await?;
  let a = client.get("a{1}").await?;
  let b = client.get("b{1}").await?;
  let c = client.get("c{1}").await?;

  assert_eq!(a.as_i64().unwrap(), 1);
  assert_eq!(b.as_i64().unwrap(), 2);
  assert_eq!(c.as_i64().unwrap(), 3);

  Ok(())
}

// should panic
pub async fn should_error_mset_empty_map(client: RedisClient, config: RedisConfig) -> Result<(), RedisError> {
  client.mset(RedisMap::new()).await.map(|_| ())
}
```

2. Call the tests from the [centralized server tests](tests/integration/centralized.rs).

```rust
mod keys {
   
  // ..
  centralized_test!(keys, should_mset_a_non_empty_map);
  centralized_test_panic!(keys, should_error_mset_empty_map);
}

```

3. Call the tests from the [cluster server tests](tests/integration/cluster.rs).

```rust
mod keys {
  
  // ..
  cluster_test!(keys, should_mset_a_non_empty_map);
  cluster_test_panic!(keys, should_error_mset_empty_map);
}
```

This will generate test wrappers to call your test function against both centralized and clustered redis servers with pipelined and non-pipelined clients.

# Misc

### Scanning

Scanning is implemented in a way where callers can throttle the rate at which they page results from the server. Scanning typically works by repeatedly running the `SCAN` (or `HSCAN`, `SSCAN`, etc) command with a different cursor each time, where the new cursor comes from the response to the previous `SCAN` call.

There are a few ways to implement `SCAN`, but this library opted for a method that allows the caller to throttle the scanning for a few reasons:

1. `SCAN` only returns keys, so it's very common for callers to read the associated values for each page of results.
2. Background scanning can cause the in-memory buffer of pages to grow very quickly.
3. Background scanning can have a real impact on server CPU resources if it's not throttled in some way. 
4. Background scanning can have a real impact on network resources if it's not throttled.

In order to implement this pattern the client returns a stream of values such as the `ScanResult` struct. One of the functions on this struct is called `next()`, which controls when the next `SCAN` call with an updated cursor is sent to the server. Since the `SCAN` function returns a stream of results this function essentially controls when the next item in the stream will arrive, which gives the caller a mechanism to throttle the scanning.

If callers want to scan the keyspace as fast as possible they can call `next` at the top of the function that handles scan results, otherwise they can call it after an `await` point to throttle the scanning.

### Reconnect After NOAUTH Errors

As mentioned earlier this module has been used in a few applications over the last few years. These apps used Elasticache and encountered a few fun problems in that time. Some of these issues involved adding workarounds to the app layer, and in some cases these workarounds made their way into this library. 

One of the more interesting issues resulted in the addition of the `reconnect-on-auth-error` feature. This feature was added to work around the fact that sometimes Elasticache would start returning NOAUTH errors (meaning a client had not provided a password) to a client that had authenticated already. This would happen even without network interruptions or any password changes. While debugging this issue we noticed that similar issues had been filed on other Redis clients as well, but only when using Elasticache, and none of those issues had good answers as to why this was happening.

The workaround enabled by the `reconnect-on-auth-error` feature was to treat NOAUTH errors like connection errors such that they trigger a reconnection. After reconnecting the client will then run the AUTH command again before replaying any failed commands, including the one that previously saw the NOAUTH error.

### Blocking Encoding

While profiling this module a few things stood out:

* The process of copying a command's args into a redis protocol `Frame` can use a non-trivial amount of CPU for large values.
* The process of encoding `Frame` values into byte arrays can use a non-trivial amount of CPU for large values.
* The process of decoding a byte array into a `Frame` can use a non-trivial amount of CPU for large values.

This library uses the [codec](https://docs.rs/tokio-util/0.6.7/tokio_util/codec/index.html) interface to encode and decode frames, which does not allow for async functions to do the encoding or decoding. This means we cannot use [spawn_blocking](https://docs.rs/tokio/1.9.0/tokio/task/fn.spawn_blocking.html) since we can't call `await` on the returned `JoinHandle`. 

Fortunately Tokio has a mechanism for tasks like this: [block_in_place](https://docs.rs/tokio/1.9.0/tokio/task/fn.block_in_place.html). However, this function only works when using a multi-thread runtime. Trying to use this function on a current-thread runtime will panic.

To make these operations less impactful on an application this library provides a feature called `blocking-encoding`. This feature will use `block_in_place` for CPU-bound operations that operate on values over a certain size, **but should only be enabled if the caller uses a multi-thread runtime**. 

See the [globals](./src/globals.rs) file for information on configuring the size threshold where CPU-bound tasks will use this interface.