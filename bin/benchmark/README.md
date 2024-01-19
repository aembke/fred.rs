Fred Benchmark
=============

Redis includes a [benchmarking tool](https://redis.io/docs/management/optimization/benchmarks/) that can be used to measure the throughput of a client/connection pool. This module attempts to reproduce the same process with Tokio and Fred. 

The general strategy involves using an atomic global counter and spawning `-c` Tokio tasks that fight over `-P` clients in order to send `-n` total `INCR` commands to the server as quickly as possible.

Each of the `-c` Tokio tasks use a different random key so commands are uniformly distributed across a cluster or replica set.

This strategy also has the benefit of being somewhat representative of an Axum or Actix web server use case where requests run in separate Tokio tasks but share a common client pool. 

## Tuning

`fred` supports several additional features or performance tuning options that can affect these results. For example:

* Tracing. Simply enabling the FF cut throughput by ~20% in my tests.
* Pipelining. The `auto_pipeline` feature can dramatically improve throughput in scenarios like this where a client or pool is shared among many Tokio tasks.
* Clustering
* Backpressure settings
* Network latency
* Log levels, often indirectly for the same reason as `tracing` (contention on a pipe, file handle, or socket).
* The size of the client connection pool.
* And much more...

Callers should take care to consider each of these when deciding on argv values.

This module also includes an optional `assert-expected` feature flag that adds an `assert!` call after each `INCR` command to ensure the response is actually correct. 

## Tracing 

**This part frequently breaks since I rarely use tracing while benchmarking.**

This also shows how to configure the client with tracing enabled against a local Jaeger instance. A [docker compose](../../tests/docker/compose/jaeger.yml) file is included that will run a local Jaeger instance.

```
docker-compose -f /path/to/fred/tests/docker/compose/jaeger.yml up
```

Then navigate to <http://localhost:16686>.

By default, this module does not compile any tracing features, but there are 3 flags that can toggle how tracing is configured.

* `partial-tracing` - Enables `fred/partial-tracing` and emits traces to the local jaeger instance.
* `full-tracing` - Enables `fred/full-tracing` and emits traces to the local jaeger instance.
* `stdout-tracing` - Enables `fred/partial-tracing` and emits traces to stdout.

## Docker

Linux+Docker is the best supported option via the `./run.sh` script. The `Cargo.toml` provided here has a comment/toggle around the lines that need to change if callers want to use a remote server. 

Callers may have to also change `run.sh` to enable additional features in docker.

**I would not even bother trying to run this on OS X, especially on Apple Silicon, with Docker at the moment.** It will be very slow compared to Linux or any other deployment model that avoids an Apple FS virtualization layer. All the docker tooling assumes a local docker engine and makes frequent use of `VOLUME`s.

## Usage 

```
USAGE:
    fred_benchmark [FLAGS] [OPTIONS] [SUBCOMMAND]

FLAGS:
        --cluster     Whether or not to assume a clustered deployment.
        --help        Prints help information
    -q, --quiet       Only print the final req/sec measurement.
        --replicas    Whether or not to use `GET` with replica nodes instead of `INCR` with primary nodes.
    -t, --tls         Enable TLS via whichever build flag is provided.
    -t, --tracing     Whether or not to enable tracing via a local Jeager instance. See tests/docker-compose.yml to
                      start up a local Jaeger instance.
    -V, --version     Prints version information

OPTIONS:
    -a, --auth <STRING>           The password/key to use. `REDIS_USERNAME` and `REDIS_PASSWORD` can also be used.
    -c, --concurrency <NUMBER>    The number of Tokio tasks used to run commands. [default: 100]
    -n, --commands <NUMBER>       The number of commands to run. [default: 100000]
    -h, --host <STRING>           The hostname of the redis server. [default: 127.0.0.1]
    -P, --pool <NUMBER>           The number of clients in the redis connection pool. [default: 1]
    -p, --port <NUMBER>           The port for the redis server. [default: 6379]
    -u, --unix-sock <PATH>        The path to a unix socket.

SUBCOMMANDS:
    help           Prints this message or the help of the given subcommand(s)
    no-pipeline    Run the test without pipelining [Default].
    pipeline       Run the test with pipelining.
```

## Examples

All the examples below use the following parameters:

* Clustered deployment via local docker (3 primary nodes with one replica each)
* No tracing features enabled
* No TLS
* 10_000_000 INCR commands with `assert-expected` enabled
* 10_000 Tokio tasks
* 15 clients in the connection pool

With `auto_pipeline` **disabled**:

```
foo@bar:/path/to/fred.rs/bin/benchmark$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar no-pipeline
Performed 10000000 operations in: 31.496934107s. Throughput: 317500 req/sec
```

With `auto_pipeline` **enabled**:

```
foo@bar:/path/to/fred.rs/bin/benchmark$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar pipeline
Performed 10000000 operations in: 4.125544401s. Throughput: 2424242 req/sec
```

With `auto_pipeline` **enabled** and using `GET` with replica nodes instead of `INCR` with primary nodes:

```
foo@bar:/path/to/fred.rs/bin/benchmark$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar --replicas pipeline
Performed 10000000 operations in: 3.356416674s. Throughput: 2979737 req/sec
```

Maybe Relevant Specs:
* AMD Ryzen 9 7950X (16 physical, 32 logical)
* 64 GB DDR5 @ 6000 MHz

## `redis-rs` Comparison

The `USE_REDIS_RS` environment variable can be toggled to [switch the benchmark logic](./src/_redis.rs) to use `redis-rs` instead of `fred`. There's also an `info` level log line that can confirm this at runtime. 

The `redis-rs` variant uses the same general strategy, but with [bb8-redis](https://crates.io/crates/bb8-redis) (specifically `Pool<RedisMultiplexedConnectionManager>`) instead of `fred::clients::RedisPool`. All the other more structural components in the benchmark logic are the same. 

Please reach out if you think this tooling or strategy is not representative of a real-world Tokio-based use case. 

### Background

First, I'd recommend reading this: https://redis.io/docs/manual/pipelining. It's important to understand what RTT is, why pipelining minimizes its impact in general, and why it's often the only thing that really matters for the overall throughput of an IO-bound application with dependencies like Redis.

In the past I've avoided directly comparing `fred` and `redis-rs` because in my opinion they're just built for different use cases. However, recently Rust has become more popular in the web development space, specifically with Axum, Actix, or other Tokio-based web frameworks, and I've received some requests for a more detailed comparison of the two in this context.

These use cases often share an important characteristic:

End-user requests run concurrently, often in parallel in separate Tokio tasks, but need to share a small pool of Redis connections via some kind of dependency injection interface. These request tasks rarely have any kind of synchronization requirements (there's usually no reason one user's request should have to wait for another to finish), so ideally we could efficiently interleave their Redis commands on the wire in a way that can take advantage of this. 

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

and the effect becomes even more pronounced as concurrency (the number of tasks) increases, at least until other bottlenecks kick in. You'll often see me describe this as "pipelining across tasks", whereas the `redis::Pipeline` and `fred::clients::Pipeline` interfaces control pipelining __within__ a task.

This benchmarking tool is built specifically to represent this class of high concurrency use cases and to measure the impact of this particular pipelining optimization (`auto_pipeline: true` in `fred`), so it seemed interesting to adapt it to compare the two libraries. If this pipelining strategy is really that effective then we should see significant differences in the throughput measurements between the two libraries.

If your use case is not structured this way or your stack does not use Tokio concurrency features then these results are likely less relevant. 

### Explanation

The results are interesting, so I'm going to lead with an explanation first. This touches on some deeper message passing topics that others can explain much better than I can, but hopefully folks can see where I'm going here.

Ultimately there's one main difference between the two libraries in this context:

Tokio + some opinionated design compromises allow `fred` to implement the optimization described above, but `redis-rs` effectively can't due to the mutability constraint in the `ConnectionLike` interface. However, it's important to note that this `&mut` constraint is basically unavoidable for all practical purposes if the interface needs to be thin or generic over the transport layer since both `AsyncRead` and `AsyncWrite` require `&mut self`. 

In my opinion this is the key thing that differentiates the two libraries, and it's why I generally haven't found it useful to compare them. `fred` does not try to support a generic transport layer nor does it expose any IO interfaces directly to the caller. Callers can still inspect or modify these things indirectly, but there can never be an interface for a caller to implement their own transport layer or to directly access a `TcpStream` without sacrificing this pipelining optimization. The whole trick hinges on the fact that we know, without using a lock, that nothing else can interact with the socket and break RESP's command ordering invariant.

On the other hand `redis-rs` supports a thinner and more generic transport interface and has a very different set of constraints as a result. These are just different design goals and tradeoffs.

This tension between pipelining across tasks and a thin transport layer also stems from some friction with Rust's ownership rules. The `query_async` function returns a future that cant resolve until the server responds (or it's not really useful) but requires holding onto a `&mut self` reference. Combined with Rust's mutability constraints this effectively prevents pipelining commands across tasks since nothing else can interact with the connection until the server responds.

However, one could build a wrapper library around `redis-rs` that effectively does what `fred` does with Tokio's message passing interface, but on top of `redis-rs`' transport layer. That library would be in a good position in the stack to implement the same ~~trick~~ optimization, and would probably perform roughly the same as `fred` if it did so. In this more abstract sense you can think of `fred` as one layer of indirection above `redis-rs` where we effectively wrap the transport layer in an actor-model-esque message passing layer to remove the `&mut` and optimize the pipelining implementation. 

TLDR:

* `fred` chose to implement certain pipelining optimizations at the cost of a less flexible transport interface. Also, all of this requires Tokio-specific features.
* `redis-rs` chose to expose a thinner and more generic transport layer at the cost of this pipelining feature.

If everything I've said here is correct then we should see `fred` perform roughly the same as `redis-rs` when `auto_pipeline: false`, but it should outperform `redis-rs` when `auto_pipeline: true`. 

### Examples

These examples use the following parameters:

* Centralized deployment via local docker
* No tracing features enabled
* No TLS
* 10_000_000 INCR commands with `assert-expected` enabled
* 10_000 Tokio tasks
* 15 clients in the connection pool

```
# fred without `auto_pipeline` 
$ ./run.sh -h redis-main -p 6379 -a bar -n 10000000 -P 15 -c 10000 no-pipeline
Performed 10000000 operations in: 62.79547495s. Throughput: 159248 req/sec

# redis-rs via bb8-redis
$ USE_REDIS_RS=1 ./run.sh -h redis-main -p 6379 -a bar -n 10000000 -P 15 -c 10000
Performed 10000000 operations in: 62.583055519s. Throughput: 159787 req/sec

# fred with `auto_pipeline`
$ ./run.sh -h redis-main -p 6379 -a bar -n 10000000 -P 15 -c 10000 pipeline
Performed 10000000 operations in: 5.882182708s. Throughput: 1700102 req/sec
```