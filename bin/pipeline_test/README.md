Pipeline Test
=============

A small example app that can be used to show the performance impact of pipelining. The program will only use one Redis client/connection to the provided server unless otherwise specified.

## Tracing 

This also shows how to configure the client with tracing enabled against a local Jaeger instance. A [docker compose](../../tests/docker-compose.yml) file is included that will run a local Jaeger instance.

```
docker-compose -f /path/to/fred/tests/docker-compose.yml up
```

Then navigate to <http://localhost:16686>.

**Be sure to add `--features partial-tracing` to enable tracing.** By default this module does not compile any tracing features.

## Arguments

```
USAGE:
    pipeline_test [FLAGS] [OPTIONS] [SUBCOMMAND]

FLAGS:
        --help       Prints help information
    -t, --tracing    Whether or not to enable tracing via a local Jeager instance. See tests/docker-compose.yml to start
                     up a local Jaeger instance.
    -V, --version    Prints version information

OPTIONS:
    -C, --concurrency <NUMBER>    The number of concurrent tasks used to run commands. [default: 10]
    -c, --count <NUMBER>          The number of commands to run. [default: 10000]
    -h, --host <STRING>           The hostname of the redis server. The script assumes a centralized deployment.
                                  [default: 127.0.0.1]
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

## Examples

### Without Tracing, No Connection Pool

```
$ RUST_LOG=pipeline_test=debug cargo run --release -- -c 300000 -C 50 no-pipeline
    Finished release [optimized] target(s) in 0.05s
     Running `target/release/pipeline_test -c 300000 -C 50 no-pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 300000, tasks: 50, host: "127.0.0.1", port: 6379, pipeline: false, pool: 1 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 300000 operations in: 7.538434617s. Throughput: 39798 req/sec

$ RUST_LOG=pipeline_test=debug cargo run --release -- -c 300000 -C 50 pipeline
    Finished release [optimized] target(s) in 0.05s
     Running `target/release/pipeline_test -c 300000 -C 50 pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 300000, tasks: 50, host: "127.0.0.1", port: 6379, pipeline: true, pool: 1 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 300000 operations in: 2.201901635s. Throughput: 136301 req/sec
```

### With AlwaysOn Tracing, No Connection Pool

```
$ RUST_LOG=pipeline_test=debug cargo run --features partial-tracing --release -- -c 300000 -C 50 -t no-pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 300000 -C 50 no-pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: true, count: 300000, tasks: 50, host: "127.0.0.1", port: 6379, pipeline: false, pool: 1 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 300000 operations in: 10.91252163s. Throughput: 27492 req/sec

$ RUST_LOG=pipeline_test=debug cargo run --features partial-tracing --release -- -c 300000 -C 50 -t pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 300000 -C 50 pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: true, count: 300000, tasks: 50, host: "127.0.0.1", port: 6379, pipeline: true, pool: 1 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 300000 operations in: 4.623611748s. Throughput: 64892 req/sec
```

### Without Tracing, With Connection Pooling

```
$ RUST_LOG=pipeline_test=debug cargo run --release -- -c 1000000 -C 50 -P 6 no-pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 1000000 -C 50 -P 6 no-pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 1000000, tasks: 50, host: "127.0.0.1", port: 6379, pipeline: false, pool: 6 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 1000000 operations in: 5.385872738s. Throughput: 185701 req/sec

$ RUST_LOG=pipeline_test=debug cargo run --release -- -c 1000000 -C 50 -P 6 pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 1000000 -C 50 -P 6 pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 1000000, tasks: 50, host: "127.0.0.1", port: 6379, pipeline: true, pool: 6 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 1000000 operations in: 1.888557868s. Throughput: 529661 req/sec
```

### Without Tracing, High Concurrency

```
$ RUST_LOG=pipeline_test=debug cargo run --release -- -c 10000000 -C 15000 -P 30 pipeline
    Finished release [optimized + debuginfo] target(s) in 0.04s
     Running `target/release/pipeline_test -c 10000000 -C 15000 -P 30 pipeline`
 INFO  pipeline_test > Running with configuration: Argv { tracing: false, count: 10000000, tasks: 15000, host: "127.0.0.1", port: 6379, pipeline: true, pool: 30 }
 INFO  pipeline_test > Initialized opentelemetry-jaeger pipeline.
 INFO  pipeline_test > Connecting to 127.0.0.1:6379...
 INFO  pipeline_test > Connected to 127.0.0.1:6379.
 INFO  pipeline_test > Starting commands...
Performed 10000000 operations in: 5.182856047s. Throughput: 1929756 req/sec
```