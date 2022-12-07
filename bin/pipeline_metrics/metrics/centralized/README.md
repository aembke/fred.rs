Centralized
===========

# Pipeline

```
cargo run --release -- -c 500000 -P 1-20 --pool-step 2 -C 1-15000 --concurrency-step 30 pipeline
```

# No Pipeline

```
cargo run --release -- -c 500000 -P 1-20 --pool-step 2 -C 1-15000 --concurrency-step 30 no-pipeline
```