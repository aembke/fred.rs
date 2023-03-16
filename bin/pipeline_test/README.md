Pipeline Test
=============

A small example app that can be used to show the performance impact of pipelining. The program will only use one Redis client/connection to the provided server unless otherwise specified.

## Tracing 

This also shows how to configure the client with tracing enabled against a local Jaeger instance. A [docker compose](../../tests/docker/compose/jaeger.yml) file is included that will run a local Jaeger instance.

```
docker-compose -f /path/to/fred/tests/docker/compose/jaeger.yml up
```

Then navigate to <http://localhost:16686>.

By default, this module does not compile any tracing features, but there are 3 flags that can toggle how tracing is configured.

* `partial-tracing` - Enables `fred/partial-tracing` and emits traces to a local jaeger instance.
* `full-tracing` - Enables `fred/full-tracing` and emits traces to a local jaeger instance.
* `stdout-tracing` - Enables `fred/partial-tracing` and emits traces to stdout.

## Arguments

```
USAGE:
    pipeline_test [FLAGS] [OPTIONS] [SUBCOMMAND]

FLAGS:
        --cluster    Whether or not to assume a clustered deployment.
        --help       Prints help information
    -q, --quiet      Print a single output describing the throughput as req/sec.
    -t, --tracing    Whether or not to enable tracing via a local Jeager instance. See tests/docker-compose.yml to start
                     up a local Jaeger instance.
    -V, --version    Prints version information

OPTIONS:
    -C, --concurrency <NUMBER>    The number of concurrent tasks used to run commands. [default: 10]
    -c, --count <NUMBER>          The number of commands to run. [default: 10000]
    -h, --host <STRING>           The hostname of the redis server. [default: 127.0.0.1]
    -P, --pool <NUMBER>           The number of clients in the redis connection pool. [default: 1]
    -p, --port <NUMBER>           The port for the redis server. [default: 6379]

SUBCOMMANDS:
    help           Prints this message or the help of the given subcommand(s)
    no-pipeline    Run the test without pipelining.
    pipeline       Run the test with pipelining.
```

## Running

```
cd /path/to/fred/bin/pipeline_test
RUST_LOG=pipeline_test=info cargo run --release -- <your args>
```

## Implementation

The `-P` argument controls the size of the underlying shared client/connection pool, and the `-C` argument controls the number of concurrent requests to emulate (with the assumption that each request runs in a separate tokio task).

Each task spawned by this utility operates on a different random key, so the `-C` argv also controls the number of unique keys that are used. The script uses `INCR` commands to keep things simple and to allow for the client to validate the expected result after each command.
