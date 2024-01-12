Documentation
=============

The documents in this folder provide a more detailed overview of how the library is built. The intended audience is potential contributors, maintainers, or anybody looking to do a deeper technical evaluation. 

The [CONTRIBUTING](../CONTRIBUTING.md) doc also provides a high level overview, but is written specifically for users that want to add new commands with as little friction as possible. These documents provide much more context.

## TLDR:

Beyond the main README, here's a quick list of things that potential users may want to consider. 

* It requires Tokio.
* It does not support `no-std` builds.
* The parsing layer uses a zero-copy parser based on [bytes](https://crates.io/crates/bytes) types. If you're willing to use these types then the library imposes almost no additional storage overhead. For example, by using `Bytes` as the primary input and output type with commands callers can entirely avoid additional allocations of these values. The response `Bytes` will be an owned view into the [Tokio socket buffer](https://docs.rs/tokio-util/latest/tokio_util/codec/trait.Decoder.html#tymethod.decode). 
* The primary request-response happy path is lock free, nor is the caller required to use one. The client uses Tokio message passing features, atomics, and [crossbeam queue](https://crates.io/crates/crossbeam-queue) types as alternatives. This creates a nice developer experience and is pretty fast. 
* The public interface is generic and supports strongly and stringly-typed usage patterns.
* There's a fallback interface for sending any commands to the server. 
* There's an optional lower level connection management interface.
* There are a ton of configuration options. Arguably too many. However, I think it's worthwhile to tune most of these settings.

See the [benchmark](../bin/benchmark) folder for more info on performance testing.

## Background
 
Fred was originally written in 2017 to support tokio-core 0.1 + futures 0.1 use cases. As of writing there have been 8 major releases, largely just trying to keep in sync with big changes in the Rust language or ecosystem. This library predates even `impl Trait`, and the language has come a long ways since then. 

Many of the design decisions described in these documents come from this initial use case. Loosely summarized:

* I'm building a web or RPC server with an HTTP, gRPC, or AMQP interface on a mostly Tokio-based stack. 
* The application makes frequent use of concurrency features, may run on large VMs, and uses Redis a lot. Ideally the client would support highly concurrent use cases in an efficient way.
* I'm using a clustered Redis deployment on ElastiCache in production, a clustered deployment in Kubernetes in lower environments, and a centralized deployment when developing locally. This effectively means I don't want most of my application code coupled to the Redis deployment model. However, there are some cases where this is unavoidable, so I'd like the option do my own connection or server management if necessary.
* I may switch Redis vendors or deployment models at any time. This can have a huge impact on network performance and reliability, and effectively means the client must handle many forms of reverse proxy or connection management shenanigans. The reconnection logic must work reliably.
* In production big clustered deployments may scale horizontally at any time. This should not cause downtime or end user errors, but it's ok if it causes minor delays. Ideally the client would handle this gracefully.
* I usually want the client to retry things (within reason) before reporting an error. Configuration options to selectively disable or tune this would be nice. 
* I need full control over the TLS configuration process. 

If this list overlaps with your use case then `fred` may be a good option. Many of these aren't hard requirements, but the higher level common interfaces were designed with this list in mind.

## Technical Overview

For the most part the library uses message passing and dependency injection patterns. The primary concern is to support highly concurrent use cases, therefore we want to minimize contention on shared resources. There's several ways to do this, but the one used here is to utilize something like the actor model. Thankfully Tokio provides all the underlying interfaces one would need to use basic actor model patterns without any additional frameworks or libraries. 

If you're not familiar with message passing in Rust I would strongly recommend reading [the Tokio docs](https://docs.rs/tokio/latest/tokio/sync/index.html#message-passing) first. 

Here's a top-down way to visualize the communication patterns between Tokio tasks within `fred` in the context of an Axum app. This diagram assumes we're targeting the use case described above. Sorry for this.

* Blue boxes are Tokio tasks.
* Green arrows use a shared [MPSC channel](https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.unbounded_channel.html).
* Brown arrows use [oneshot channels](https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html). Callers include their [oneshot sender](https://docs.rs/tokio/latest/tokio/sync/oneshot/struct.Sender.html) half in any messages they send via the green arrows.

![Bad Design Doc](./design.png)

The shared state in this diagram is an `Arc<UnboundedSender>` that's shared between the Axum request tasks. Each of these tasks can write to the channel without acquiring a lock, minimizing contention that could slow down the application layer. At a high level all the public client types are thin wrappers around an `Arc<UnboundedSender>`. A `RedisPool` is really a `Arc<Vec<Arc<UnboundedSender>>>` with an additional atomic increment-mod-length trick in the mix. Cloning anything `ClientLike` usually just clones one of these `Arc`s.

Generally speaking the router task sits in a `recv` loop. 

```rust
async fn example(connections: &mut HashMap<Server, Connection>, rx: UnboundedReceiver<Command>) -> Result<(), RedisError> {
  while let Some(command) = rx.recv().await {
    send_to_server(connections, command).await?;
  }
    
  Ok(())
}
```

Commands are processed in series, but the `auto_pipeline` flag controls whether the `send_to_server` function waits on the server to respond or not. When commands can be pipelined this way the loop can process requests as quickly as they can be written to a socket. This model also creates a pleasant developer experience where we can pretty much ignore many synchronization issues, and as a result it's much easier to reason about how features like reconnection should work. It's also much easier to implement socket flushing optimizations with this model. 

However, this has some drawbacks:
* Once a command is in the `UnboundedSender` channel it's difficult to inspect or remove. There's no practical way to get any kind of random access into this.
* It can be difficult to create a "fast lane" for "special" commands with this model. For example, forcing a reconnection should take precedence over a blocking command. This is more difficult to implement with this model.
* Callers should at least be aware of this channel so that they're aware of how server failure modes can lead to increased memory usage. This makes it perhaps surprisingly important to properly tune reconnection or backpressure settings, especially if memory is in short supply.
* Some things that would ideally be synchronous must instead be asynchronous. For example, I've often wanted a synchronous interface to inspect active connections.

As of 8.x there's a new `max_command_buffer_len` field that can be used as a circuit breaker to trigger backpressure if this buffer grows too large.

Similarly, the reader tasks also use a `recv` loop:

```rust
async fn example(state: &mut State, stream: SplitTcpStream<Frame>) -> Result<(), RedisError> {
  while let Some(frame) = stream.try_next().await? {
    // the underlying shared buffer/queue is a crossbeam-queue `SegQueue`
    let command = state.get_oldest_command();
    // responding to oneshot channels only uses atomics and is not async, so this loop is quick
    command.respond_to_caller(frame);
  }
    
  Ok(())
}
```

In order for the reader task to respond to the caller in the Axum task we need a mechanism for the caller's oneshot sender half to move between the router task and the reader task that receives the response. An [`Arc<SegQueue>`](https://docs.rs/crossbeam-queue/latest/crossbeam_queue/struct.SegQueue.html) is shared between the router and each reader task to support this. 

## Code Layout 

* The [commands](../src/commands) folder contains the public interface and private implementation for each of the Redis commands, organized by category. This is roughly the same categorization used by the [public docs](https://redis.io/commands/). Each of these public command category interfaces are exposed as a trait with default implementations for each command.
* The [clients](../src/clients) folder contains public client structs that implement and/or override the traits from [the command category traits folder](../src/commands/impls). The [interfaces](../src/interfaces.rs) file contains the shared traits required by most of the command category traits, such as `ClientLike`.
* The [monitor](../src/monitor) folder contains the implementation of the `MONITOR` command and the parser for the response stream.
* The [protocol](../src/protocol) folder contains the implementation of the base `Connection` struct and the logic for splitting a connection to interact with reader and writer halves in separate tasks. The [TLS interface](../src/protocol/tls.rs) is also implemented here.
* The [router](../src/router) folder contains the logic that implements the sentinel and cluster interfaces. Clients interact with this struct via a message passing interface. The interface exposed by the `Router` attempts to hide all the complexity associated with sentinel or clustered deployments.
* The [trace](../src/trace) folder contains the tracing implementation. Span IDs are manually tracked on each command as they move across tasks.
* The [types](../src/types) folder contains the type definitions used by the public interface. The Redis interface is relatively loosely typed but Rust can support more strongly typed interfaces. The types in this module aim to support an optional but more strongly typed interface for callers.
* The [modules](../src/modules) folder contains smaller helper interfaces such as a lazy [Backchannel](../src/modules/backchannel.rs) connection interface and the [response type conversion logic](../src/modules/response.rs).
