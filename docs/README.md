Documentation
=============

The documents in this folder provide a more detailed overview of how the library is built. The intended audience is potential contributors, maintainers, or anybody looking to do a deeper technical evaluation. 

The [CONTRIBUTING](../CONTRIBUTING.md) doc also provides a high level overview, but is written specifically for users that want to add new commands with as little friction as possible. These documents provide much more context.

## TLDR:

Beyond the main README, here's a quick list of things that potential users may want to consider. 

* It requires Tokio.
* It does not support `no-std` builds.
* The parsing layer uses a zero-copy parser based on [bytes](https://crates.io/crates/bytes) types. If you use these types then the library imposes almost no additional storage overhead. For example, by using `Bytes` or `Str` as the primary input and output type with commands callers can avoid any additional allocations of these values. The response `Bytes` will be an owned view into the [Tokio socket buffer](https://docs.rs/tokio-util/latest/tokio_util/codec/trait.Decoder.html#tymethod.decode). 
* The primary request-response happy path is lock free, nor is the caller required to use one. The client uses Tokio message passing features, atomics, and [crossbeam queue](https://crates.io/crates/crossbeam-queue) types as alternatives. This creates a nice developer experience and is pretty fast. 
* The public interface is generic and supports strongly and stringly-typed usage patterns.
* There's a fallback interface for sending any commands to the server. 
* There's an optional lower level connection management interface.
* There are a ton of configuration options. Arguably too many. However, I think it's worthwhile to tune most of these settings.

See the [benchmark](../bin/benchmark) folder for more info on performance testing.

## Background
 
Fred was originally written in 2017 to support tokio-core 0.1 + futures 0.1 use cases. As of writing there have been 8 major releases, largely just trying to keep in sync with big changes in the Rust language or ecosystem. This library even predates `impl Trait`, and the language has come a long ways since then. 

Many of the design decisions described in these documents come from this initial use case. 

* I'm building a web or RPC server on a mostly Tokio-based stack. 
* The application makes frequent use of Tokio's concurrency features, may run on large VMs, and uses Redis a lot. Ideally the client would support highly concurrent use cases in an efficient way.
* The application may run on hundreds or thousands of server instances and Redis is the main or only IO-bound dependency. Making efficient use of network resources and minimizing the impact of RTT can have a meaningful impact on my scale metrics and hosting costs. 
* I'm using a clustered Redis deployment on ElastiCache in production, a clustered deployment in Kubernetes in lower environments, and a centralized deployment when developing locally. This effectively means I don't want most of my application code coupled to the Redis deployment model. However, there are some cases where this is preferable or unavoidable, so I'd like the option do my own connection or server management if necessary.
* I sometimes have to switch Redis vendors or deployment models. This can have a huge impact on network performance and reliability, and effectively means the client must handle many forms of reverse proxy or load balancing shenanigans. The reconnection logic must work reliably.
* Big clustered deployments often scale horizontally. This should not cause downtime or end user errors, but it's ok if it causes minor delays. Ideally the client would handle this gracefully.
* I usually want the client to retry things (within reason) before reporting an error. Configuration options to selectively disable or tune this would be nice. 
* I need full control over the TLS configuration process. 

If this list overlaps with your use case then `fred` may be a good option. Many of these aren't hard requirements, but the higher level common interfaces were designed with this list in mind.

### Important Context 

The most important design decision made by this library requires understanding how and why pipelining works. I strongly recommend that folks read https://redis.io/docs/manual/pipelining.

It's important to understand what RTT is, why pipelining minimizes its impact in general, and why it's often the only thing that really matters when measuring the throughput of an IO-bound application with dependencies like Redis. The entire design of this library is based on the pipelining optimization described in this section. My hope is that folks building other network libraries that use pipelined protocols might find the ideas described here useful.

As described above my application has the following characteristics:
* Its primary throughput bottleneck is RTT to Redis.
* It uses relatively large clustered deployments, often with some managed service like Elasticache.
* It makes frequent use of Tokio concurrency features on a multi-thread runtime. Requests/jobs run in separate Tokio tasks. 
* It's using dependency injection patterns to share a small pool of connections/clients among each of the Tokio tasks.

And most importantly - making efficient use of network resources is important for my use case to scale from both a cost and performance perspective. Ideally the library could interleave frames on the wire such that concurrent request/job tasks didn't have to wait for other tasks to receive a response from the server. 

For example,

1. Task A writes command 1 to server.
2. Task B writes command 2 to server.
3. Task A reads command response 1 from server.
4. Task B reads command response 2 from server.

reduces the impact of RTT much more effectively than

1. Task A writes command 1 to server.
2. Task A reads command response 1 from server.
3. Task B writes command 2 to server.
4. Task B reads command response 2 from server.

and the effect becomes even more pronounced as concurrency (the number of tasks) increases, at least until other bottlenecks kick in. You'll often see me describe this as "pipelining across tasks", whereas most client pipelining interfaces only control pipelining __within__ a task.

A diagram may explain this better:

![Pipelining](./pipelining.png)

With this model we're not reducing network latency or RTT, but by rarely or never waiting for the server to respond we can pack many more requests on the wire and dramatically increase throughput in high concurrency scenarios. 

However, there are some interesting tradeoffs and subtle implications of the optimization described above, at least in Rust. As an example consider the following interface as the function at the core of any library request-response call like this:

```rust
pub async fn call(client: &MyClient, request: Request) -> Result<Response, Error> {
  // ...
}
```

One of the most important design decisions involves whether to use `&mut MyClient` or `&MyClient`. In a roundabout way this ends up determining whether it's possible, or at least practical, to implement the pipelining optimization described above. However, this does not mean that `&MyClient` is always the best choice.

If we want to expose a thin or generic transport layer such as `MyClient<T: MyConnectionTrait>` then `T` will almost certainly end up being based on `AsyncRead + AsyncWrite` or `Stream + Sink`, both of which use `&mut self` on their relevant send/poll interfaces. This means my `call` function will probably* have to use `&mut MyClient`. If callers want to use my client in a context with `Send` restrictions (across Tokio tasks for example) then somebody will have to use an async lock somewhere. Maybe the client hides this, or perhaps an external pool library hides this, but there's an async lock somewhere that holds a lock guard across an `await` point until the server responds. This ultimately conflicts with implementing the optimization above since no other task can interact with that `&mut MyClient` instance while the lock guard is being held.

However, there are many use cases where a thin and generic transport layer is more important than the particular pipelining optimization described above. In my case I only needed TCP, TCP+TLS, and maybe unix sockets, so the generic aspect of the interface was less important to me. In the end I decided to focus on an interface and design that worked well with the pipelining optimization above, which incidentally lead to `&MyClient` instead of `&mut MyClient`. The drawback of this decision is that callers cannot access or implement their own transport layers. 

However, under the hood all of these networking types use `&mut self` at some point, so we still have to reconcile the mutability constraint somehow, and we know that it can't involve holding an async lock guard across an `await` point that waits for the server to respond. At its core the primary challenge with this strategy is that it requires not only separating write and read operations on the socket, but also requires operating on each half of the socket concurrently according to RESP's frame ordering rules.

### Message Passing & Queues

Another mental model that tends to work well with pipelined protocols is to think of the client as a series of queues. For example:

1. An end user task interacts with the client by putting a message in an in-memory queue.
2. Some time later the client pops this message off the queue, serializes it, and sends it to the server over a TCP connection, which also effectively acts as a queue in this context. We also put the original request into another in-memory queue associated with the chosen connection.
3. Some time later we pop a frame off the TCP connection. Since the server always* responds to commands in order we know that the request at the front of the in-memory queue associated with this connection must be associated with the frame we just received.
4. We pop the request off the in-memory queue mentioned above and use this to respond to the caller in the original end user task.

This kind of approach also tends to work well in high concurrency scenarios since "thread safe" shared queues can be implemented without locks (just atomics). There are several options for this, including several interfaces within Tokio. 

If we think of the client as a series of routing adapters between a set of queues like this then it becomes much easier to reason about how the pipelining above should be implemented. All we really need to do is implement some policy checks in step 2 that determine whether we should wait for step 4 to finish before processing the next command in step 2. In most cases we want to write commands as quickly as possible, but in some cases maybe the client should wait (like `AUTH`, `HELLO`, blocking commands, etc). It may not be immediately obvious, but Tokio offers several easy ways to coordinate tasks like this.

This is why the library uses a significantly more complicated message passing implementation. The `auto_pipeline` flag controls this optimization and the benchmarking results show a dramatic improvement when enabled.

## Technical Overview

As mentioned above, for the most part the library uses message passing and dependency injection patterns. The primary concern is to support highly concurrent use cases, therefore we want to minimize contention on shared resources. There are several ways to do this, but the one used here is to utilize something like the actor model. Thankfully Tokio provides all the underlying interfaces one would need to use basic actor model patterns without any additional frameworks or libraries. 

If you're not familiar with message passing in Rust I would strongly recommend reading [the Tokio docs](https://docs.rs/tokio/latest/tokio/sync/index.html#message-passing) first. 

Here's a top-down way to visualize the communication patterns between Tokio tasks within `fred` in the context of an Axum app. This diagram assumes we're targeting the use case described above. Sorry for this.

* Blue boxes are Tokio tasks.
* Green arrows use a shared [MPSC channel](https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.unbounded_channel.html).
* Brown arrows use [oneshot channels](https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html). Callers include their [oneshot sender](https://docs.rs/tokio/latest/tokio/sync/oneshot/struct.Sender.html) half in any messages they send via the green arrows.

![Bad Design Doc](./design.png)

The shared state in this diagram is an `Arc<UnboundedSender>` that's shared between the Axum request tasks. Each of these tasks can write to the channel without acquiring a lock, minimizing contention that could slow down the application layer. 

At a high level all the public client types are thin wrappers around an `Arc<UnboundedSender>`. A `RedisPool` is really a `Arc<Vec<Arc<UnboundedSender>>>` with an additional atomic increment-mod-length trick in the mix. Cloning anything `ClientLike` usually just clones one of these `Arc`s.

Generally speaking the router task sits in a `recv` loop. 

```rust
async fn example(connections: &mut HashMap<Server, Connection>, rx: UnboundedReceiver<Command>) -> Result<(), RedisError> {
  while let Some(command) = rx.recv().await {
    send_to_server(connections, command).await?;
  }
    
  Ok(())
}
```

Commands are processed in series, but the `auto_pipeline` flag controls whether the `send_to_server` function waits on the server to respond or not. When commands can be pipelined this way the loop can process requests as quickly as they can be written to a socket. This model also creates a pleasant developer experience where we can pretty much ignore many synchronization issues, and as a result it's much easier to reason about how features like reconnection should work. It's also easy to implement socket flushing optimizations with this model. 

However, this has some drawbacks:
* Once a command is in the `UnboundedSender` channel it's difficult to inspect or remove. There's no practical way to get any kind of random access into this.
* It can be difficult to preempt commands with this model. For example, forcing a reconnection should take precedence over a blocking command. This is more difficult to implement with this model.
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
