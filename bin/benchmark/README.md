Fred Benchmark
==============

Redis includes a [benchmarking tool](https://redis.io/docs/management/optimization/benchmarks/) that can be used to
measure the throughput of a client/connection pool. This module attempts to reproduce the same process with Tokio and
Fred.

The general strategy involves using an atomic global counter and spawning `-c` Tokio tasks that share`-P` clients
in order to send `-n` total `INCR` commands to the server as quickly as possible.

Each of the `-c` Tokio tasks use a different random key so commands are uniformly distributed across a cluster or
replica set.

This strategy also has the benefit of being somewhat representative of an Axum or Actix web server use case where
requests run in separate Tokio tasks but share a common client pool.

## Tuning

There are several additional features or performance tuning options that can affect these results. For example:

* Tracing. Enabling the FF cut throughput by ~20% in my tests.
* Pipelining. The `auto_pipeline` feature can dramatically improve throughput in scenarios like this where a client or
  pool is shared among many Tokio tasks.
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
USAGE:
    fred_benchmark [FLAGS] [OPTIONS] [SUBCOMMAND]

FLAGS:
        --cluster     Whether to assume a clustered deployment.
        --help        Prints help information
    -q, --quiet       Only print the final req/sec measurement.
        --replicas    Whether to use `GET` with replica nodes instead of `INCR` with primary nodes.
    -t, --tls         Enable TLS via whichever build flag is provided.
    -t, --tracing     Whether to enable tracing via a local Jeager instance. See tests/docker-compose.yml to
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
$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar no-pipeline
Performed 10000000 operations in: 31.496934107s. Throughput: 317500 req/sec
```

With `auto_pipeline` **enabled**:

```
$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar pipeline
Performed 10000000 operations in: 4.125544401s. Throughput: 2424242 req/sec
```

With `auto_pipeline` **enabled** and using `GET` with replica nodes instead of `INCR` with primary nodes:

```
$ ./run.sh --cluster -c 10000 -n 10000000 -P 15 -h redis-cluster-1 -p 30001 -a bar --replicas pipeline
Performed 10000000 operations in: 3.356416674s. Throughput: 2979737 req/sec
```

Maybe Relevant Specs:

* 32 CPUs
* 64 GB memory

## `redis-rs` Comparison

The `USE_REDIS_RS` environment variable can be toggled to [switch the benchmark logic](./src/_redis.rs) to
use `redis-rs` instead of `fred`. There's also an `info` level log line that can confirm this at runtime.

The `redis-rs` variant uses the same general strategy, but with [bb8-redis](https://crates.io/crates/bb8-redis) (
specifically `Pool<RedisMultiplexedConnectionManager>`) instead of `fred::clients::RedisPool`. All the other more
structural components in the benchmark logic are the same.

Please reach out if you think this tooling or strategy is not representative of a real-world Tokio-based use case.

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