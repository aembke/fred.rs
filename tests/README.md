# Testing

Tests are organized by category, similar to the [commands](../src/commands) folder.

By default, most tests run 8 times based on the following configuration parameters: clustered vs centralized servers, pipelined vs non-pipelined clients, and RESP2 vs RESP3 mode. Helper macros exist to make this easy so each test only has to be written once.

**The tests require Redis version >=6.2** As of writing the default version used is 7.0.5.

## Installation

The [environ](environ) file will bootstrap the local environment with all the environment variables and system settings necessary to run the tests. It will prompt the caller for certain system-wide modifications if necessary. The `/etc/hosts` modifications are only necessary if you wish to manually run the TLS tests outside the docker network.

In order to run the testing scripts the following must be installed:

* Bash (all the scripts assume `bash`)
* `docker`
* `docker-compose` (this may come with `docker` depending on the version you use)

## Running Tests

The runner scripts will set up the Redis servers and run the tests inside docker. 

* [all-features](runners/all-features.sh) will run tests with all features (except sentinel tests).
* [default-features](runners/default-features.sh) will run tests with default features (except sentinel tests).
* [no-features](runners/no-features.sh) will run the tests without any of the feature flags.
* [sentinel-features](runners/sentinel-features.sh) will run the centralized tests against a sentinel deployment. This is the only test runner that requires the sentinel deployment via docker-compose.
* [cluster-rustls](runners/cluster-rustls.sh) will set up a cluster with TLS enabled and run the cluster tests against it with `rustls`.
* [cluster-native-tls](runners/cluster-native-tls.sh) will set up a cluster with TLS enabled and run the cluster tests against it with `native-tls`.
* [everything](runners/everything.sh) will run all of the above scripts. 

These scripts will pass through any extra argv so callers can filter tests as needed.

See the [CI configuration](../.circleci/config.yml) for more information.

### Example

```
cd path/to/fred
. ./tests/environ
./tests/runners/all-features.sh
```

## Adding Tests

Adding tests is straightforward with the help of some macros and utility functions.

Note: When writing tests that operate on multiple keys be sure to use a [hash_tag](https://redis.io/topics/cluster-spec#keys-hash-tags) so that all keys used by a command exist on the same node in a cluster. 

1. If necessary create a new file in the appropriate folder.
2. Create a new async function in the appropriate file. This function should take a `RedisClient` and `RedisConfig` as arguments and should return a `Result<(), RedisError>`. The client will already be connected when this function runs.
3. This new function should **not** be marked as a `#[test]` or `#[tokio::test]`
4. Call the test from the appropriate [integration/cluster.rs](integration/cluster.rs) or [integration/centralized.rs](integration/centralized.rs) files, or both. Create a wrapping `mod` block with the same name as the test's folder if necessary.
5. Use `centralized_test!` or `cluster_test!` to generate tests in the appropriate module. Centralized tests will be automatically converted to sentinel tests if using the sentinel testing features.

Tests that use this pattern will run 8 times to check the functionality against clustered and centralized redis servers with using both pipelined and non-pipelined clients in RESP2 and RESP3 mode.

## Notes

* Since we're mutating shared state in external redis servers with these tests it's necessary to run the tests with `--test-threads=1`. The test runner scripts will do this automatically.
* **The tests will periodically call `flushall` before each test iteration.**

## Contributing

The following modules still need better test coverage:

* ACL commands
* Cluster commands. This one is more complicated though since many of these modify the cluster.