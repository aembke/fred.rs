Fred Benchmark
=============

Redis includes a [benchmarking tool](https://redis.io/docs/management/optimization/benchmarks/) that can be used to measure the throughput of a client/connection pool. This module attempts to reproduce the same process with Tokio and Fred. 

The strategy used here is pretty straightforward - we use an atomic global counter and spawn `-c` Tokio tasks that fight over `-P` clients in order to send `-n` total `INCR` commands to the server as quickly as possible.

Each of the `-c` Tokio tasks use a different random key so commands are uniformly distributed across a cluster or replica set.

This strategy also has the benefit of being somewhat representative of an Axum or Actix web server use case where requests run in separate Tokio tasks but share a common client pool. 

## Tuning

`fred` supports several additional features or performance tuning options that can affect these results. For example:

* Tracing. Simply enabling the FF cut throughput by ~20% in my tests. YMMV.
* Pipelining. The `auto_pipeline` feature can dramatically improve throughput in scenarios like this where a client or pool is shared among many Tokio tasks.
* Clustering
* Backpressure settings
* Network latency
* Log levels, often indirectly for the same reason as `tracing` (contention on a pipe, file handle, or socket).
* The size of the client connection pool.
* And much more...

Callers should take care to consider each of these when deciding on argv values.

This module also includes an optional `assert-expected` feature flag that adds an `assert!` call after each `INCR` command to ensure the response is actually correct. I've found this to be useful when trying to catch race conditions.

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

I usually run this via Docker on Linux and the `run.sh` script. The `Cargo.toml` provided here has a comment/toggle around the lines that need to change if callers want to use a remote server. 

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

The `USE_REDIS_RS` environment variable can be toggled to switch the benchmark logic to use `redis-rs` instead of `fred`. There's also an `info` level log line that can confirm this at runtime. 

The `redis-rs` variant uses the same general strategy, but with [bb8-redis](https://crates.io/crates/bb8-redis) (specifically `Pool<RedisMultiplexedConnectionManager>`) instead of `fred::clients::RedisPool`. All the other more structural components in the benchmark logic are the same. 

Please reach out if you think this tooling or strategy is not representative of a real-world Tokio-based use case. I'm far from an expert on this interface. 

### Why

In general I've avoided directly comparing `fred` and `redis-rs` because in my opinion they're just built for different use cases. However, recently Rust has become more popular in the web development space and it seems like the community is starting to rally around Axum, Actix, or other Tokio-based web frameworks or libraries. These use cases usually have one very important characteristic for performance purposes - end-user requests run concurrently, often in parallel in separate tasks, but need to share a small pool of Redis connections. For this relatively narrow set of high concurrency use cases it seemed interesting to compare the two libaries.

The results are interesting so I'm going to lead with some commentary explaining them first. Just note that this is a complicated topic that involves some terms that are often overloaded. I'm going to do an incomplete job explaining this here since it touches on some deeper message passing topics that others can explain much better than I can. 

Ultimately the more opinionated design decisions in `fred` allow for some optimizations that `redis-rs` can't implement internally due to the mutability constraint in the `ConnectionLike` interface. However, this `&mut` constraint is effectively unavoidable for all practical purposes if the interface needs to be thin or generic over the transport layer since both `AsyncRead` and `AsyncWrite` require `&mut self`. 

This is one of the key things that differentiates the two libraries. `fred` does not try to support a generic transport layer nor does it expose IO interfaces directly to the caller. Callers can still inspect or modify these things indirectly, but there's no interface for a caller to implement their own transport layer or to directly access a `TcpStream`, etc. On the other hand `redis-rs` supports a more generic interface closer to the transport layer. There's no right or wrong answer here, just different design goals and trade offs.

There are some important performance implications between these two approaches though, and one of them relates to potential pipelining optimizations in highly concurrent use cases. 

Both libraries implement some form of "automatic pipelining" **across** tasks - enabled via `auto_pipeline` in `fred` and the "Multiplexed" `Connection` variants in `redis-rs`. In both libraries this interface effectively controls whether the client should wait on a response from the server before writing the next command. Obviously in most scenarios [it's much faster if we don't have to wait on the server between each command](https://redis.io/docs/manual/pipelining/). 

However, the `&mut` requirement here creates some friction with Rust's ownership rules. The `query_async` function returns a future that doesn't resolve until the server responds but requires holding onto a `&mut self` reference until the future resolves. Combined with Rust's mutability constraints this effectively prevents pipelining commands across tasks that could otherwise run concurrently on the wire (pipelined, in RESP's case). The client would need to support something like a [split](https://docs.rs/tokio/latest/tokio/io/fn.split.html) interface to separate reading and writing for this to be possible.

Again, this is just a natural and often unavoidable consequence of trying to be generic over something like `AsyncRead + AsyncWrite`. Unfortunately even adding in an async lock wouldn't help here - you're still holding onto a `&mut ConnectionLike` across an `await` point. Even using a `Pipeline` everywhere wouldn't address this since that only affects how commands are pipelined **within** a task (as opposed to across tasks). Ultimately there's just a conflict between wanting a thin and generic transport layer based on `AsyncRead + AsyncWrite` and these types of concurrency optimizations. 

However, it's worth noting that one could build a wrapper library for `redis-rs` that effectively does what `fred` does with Tokio's message passing interface, but on top of `redis-rs`' transport layer. That library would probably perform the same as `fred` if it implemented the same pipelining optimizations. In this more abstract sense you can think of `fred` as one layer of indirection above `redis-rs` where we effectively wrap the transport layer in an actor-model-esque message passing layer in order to remove the `&mut` and optimize the pipelining implementation. 

TLDR:

* `fred` chose to implement more sophisticated pipelining logic at the cost of a less flexible interface by having to effectively hide the transport layer.
* `redis-rs` chose to expose a thinner and more generic transport layer at the cost of these more complicated concurrency features.

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