Pipeline Metrics
================

A small test utility used to repeatedly run the [pipeline test](../pipeline_test) utility with different combinations of `concurrency` and `pool` arguments. "Pipeline" metrics use the `auto_pipeline` feature while the "no-pipeline" metrics do not. 

The [metrics](metrics) folder contains the output of running the script against local server configurations on my desktop (12 cores, 32 GB memory). Steps to reproduce the results are shown in each sub-folder.

The cluster configuration uses the default cluster configuration provided by Redis (3 primary nodes with one replica each), all running locally.

Since the Redis server is running locally we can largely discount the effect of network latency, but this is still useful to understand how concurrent tasks interact with various connection pooling configurations. 

## Examples

To reproduce all the results locally:

```
cd path/to/fred.rs
export REDIS_VERSION=7.0.5
. ./tests/environ
# this does not require the TLS cluster setup
./tests/scripts/full_install.sh

cd path/to/fred.rs/bin/pipeline_metrics

# this will take a while...
cargo run --release -- -c 500000 -P 1-20 --pool-step 2 -C 1-15000 --concurrency-step 30 pipeline > metrics/centralized/pipeline.csv \
 && cargo run --release -- -c 500000 -P 1-20 --pool-step 2 -C 1-15000 --concurrency-step 30 no-pipeline > metrics/centralized/no-pipeline.csv \
 && cargo run --release -- --cluster -p 30001 -c 500000 -P 1-20 --pool-step 2 -C 1-15000 --concurrency-step 30 pipeline > metrics/cluster/pipeline.csv \
 && cargo run --release -- --cluster -p 30001 -c 500000 -P 1-20 --pool-step 2 -C 1-15000 --concurrency-step 30 no-pipeline > metrics/cluster/no-pipeline.csv 
```
