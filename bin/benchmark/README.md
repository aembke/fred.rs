Fred Benchmark
==============

Redis includes a [benchmarking tool](https://redis.io/docs/management/optimization/benchmarks/) that can be used to
measure the throughput of a client/connection pool. This module attempts to reproduce the same process with Tokio and
Fred.

The general strategy involves using an atomic global counter and spawning `-c` Tokio tasks that share `-P` clients in
order to send `-n` total `INCR` commands to the server as quickly as possible.

Each of the `-c` Tokio tasks use a different random key so commands are uniformly distributed across a cluster or
replica set.

This strategy also has the benefit of being somewhat representative of an Axum or Actix web server use case where
requests run in separate Tokio tasks but share a common client pool.

The [benchmark metrics](../benchmark_metrics) folder contains a tool that can test different combinations of
concurrency (`-c`) and pool size (`-P`) argv.

## Tuning

There are several additional features or performance tuning options that can affect these results. For example:

* Tracing. Enabling the FF cut throughput by ~20% in my tests.
* Pipelining. The `auto_pipeline` feature can dramatically improve throughput in scenarios like this where a client or
  pool is shared among many Tokio tasks. The original purpose of this tool was to test this particular optimization.
* Clustering
* Backpressure settings
* Network latency
* Log levels, often indirectly for the same reason as `tracing` (contention on a pipe, file handle, or socket).
* The size of the client connection pool.

Callers should take care to consider each of these when deciding on argv values.

This module also includes an optional `assert-expected` feature flag that adds an `assert!` call after each `INCR`
command to ensure the response is actually correct.

## Tracing

This also shows how to configure the client with tracing enabled against a local Jaeger instance.
A [docker compose](../../tests/docker/compose/jaeger.yml) file is included that will run a local Jaeger instance.

```
docker-compose -f /path/to/fred/tests/docker/compose/jaeger.yml up
```

Then navigate to <http://localhost:16686>.

By default, this module does not compile any tracing features, but there are 3 flags that can toggle how tracing is
configured.

* `partial-tracing` - Enables `fred/partial-tracing` and emits traces to the local jaeger instance.
* `full-tracing` - Enables `fred/full-tracing` and emits traces to the local jaeger instance.
* `stdout-tracing` - Enables `fred/partial-tracing` and emits traces to stdout.

## Docker

Linux+Docker is the best supported option via the `./run.sh` script. The `Cargo.toml` provided here has a comment/toggle
around the lines that need to change if callers want to use a remote server.

Callers may have to also change `run.sh` to enable additional features in docker.

## Usage

```
A benchmarking module based on the `redis-benchmark` tool included with Redis.

USAGE:
    fred_benchmark [FLAGS] [OPTIONS]

FLAGS:
        --cluster     Whether to assume a clustered deployment.
        --help        Prints help information
    -q, --quiet       Only print the final req/sec measurement.
        --replicas    Whether to use `GET` with replica nodes instead of `INCR` with primary nodes.
    -t, --tls         Enable TLS via whichever build flag is provided.
    -T, --tracing     Whether to enable tracing via a local Jeager instance. See tests/docker-compose.yml to start up a
                      local Jaeger instance.
    -V, --version     Prints version information

OPTIONS:
    -a, --auth <STRING>           The password/key to use. `REDIS_USERNAME` and `REDIS_PASSWORD` can also be used.
        --bounded <NUMBER>        The size of the bounded mpsc channel used to route commands. [default: 0]
    -c, --concurrency <NUMBER>    The number of Tokio tasks used to run commands. [default: 100]
    -n, --commands <NUMBER>       The number of commands to run. [default: 100000]
    -h, --host <STRING>           The hostname of the redis server. [default: 127.0.0.1]
    -P, --pool <NUMBER>           The number of clients in the redis connection pool. [default: 1]
    -p, --port <NUMBER>           The port for the redis server. [default: 6379]
    -u, --unix-sock <PATH>        The path to a unix socket.
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
$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar no-pipeline
Performed 10000000 operations in: 27.038434665s. Throughput: 369849 req/sec
```

With `auto_pipeline` **enabled**:

```
$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar pipeline
Performed 10000000 operations in: 3.728232639s. Throughput: 2682403 req/sec
```

With `auto_pipeline` **enabled** and using `GET` with replica nodes instead of `INCR` with primary nodes:

```
$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar --replicas pipeline
erformed 10000000 operations in: 3.234255482s. Throughput: 3092145 req/sec
```

Maybe Relevant Specs:

* 32 CPUs
* 64 GB memory

## `redis-rs` Comparison

The `USE_REDIS_RS` environment variable can be toggled to [switch the benchmark logic](./src/_redis.rs) to
use `redis-rs` instead of `fred`. There's also an `info` level log line that can confirm this at runtime.

The `redis-rs` variant uses the same general strategy where multiple tasks share a client or pool and try to send `INCR`
to the server(s) as quickly as possible.

The comparison benchmarks require some explanation since there are several ways to use `redis-rs`. Both `redis-rs` and
`fred` implement a form of multiplexing across Tokio tasks such that independent tasks can share a connection without
waiting on one another (usually). In `redis-rs` this is called the `MultiplexedConnection` and in `fred` it was formerly
called `auto_pipeline`.

For many use cases a single client instance can handle more than enough concurrent commands, but in some cases callers
may want to use connection pooling (connection redundancy and failover, TCP layer bottlenecks, etc). The `redis-rs`
crate has several ways to do this, but for the purposes of this benchmark I used `bb8-redis`. It's worth noting however
that `bb8-redis` does not appear to be aware of the multiplexed nature of `MultiplexedConnection`, and as a result it
actually performs significantly worse than a single `MultiplexedConnection` since the pool acquisition interface gives
exclusive access to the connection while waiting on a response, essentially requiring the caller to wait for a full
round-trip on each command and negating any benefits of the underlying multiplexing layer. At the time of writing there
does not appear to be an out-of-the-box module that can do efficient pooling across multiple `redis-rs`'s
`MultiplexedConnection`s. For those curious, this is why `fred` ships with its own `RedisPool` interface rather than
using `bb8` (or similar). The `RedisPool` interface is specifically built to avoid this issue.

As a result there's currently no direct (or fair) comparisons between a `RedisPool` and anything in
`redis-rs`, but we can compare individual client instances that use a single connection. However, for comparison
purposes this tool does include a way to benchmark `bb8-redis` pooling, although callers should be aware that it's not a
fair, apples-to-apples to comparison with `RedisPool` due to the multiplexing issue mentioned above.

There are two env flags that can be used to enable the `redis-rs` benchmarks via the `./run.sh` script:

* `REDIS_RS_BB8` - Use the `bb8-redis` interface to pool several `MultiplexedConnection` instances. Again, note the
  above limitations.
* `REDIS_RS_MANAGER` - Use a single `MultiplexedConnection`. This is the best comparison between the two libraries, but
  it will ignore any pooling argv. At the moment it's only suitable for comparing individual centralized clients.

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
Performed 10000000 operations in: 52.156700826s. Throughput: 191732 req/sec

# redis-rs via bb8-redis
$ USE_REDIS_RS=1 ./run.sh -h redis-main -p 6379 -a bar -n 10000000 -P 15 -c 10000
Performed 10000000 operations in: 102.953612933s. Throughput: 97131 req/sec

# fred with `auto_pipeline`
$ ./run.sh -h redis-main -p 6379 -a bar -n 10000000 -P 15 -c 10000 pipeline
Performed 10000000 operations in: 5.74236423s. Throughput: 1741553 req/sec
```