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