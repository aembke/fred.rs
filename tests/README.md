# Testing

Tests are organized by category, similar to the [commands](../src/commands) folder.

By default, most tests run 8 times based on the following configuration parameters: clustered vs centralized servers, pipelined vs non-pipelined clients, and RESP2 vs RESP3 mode. Helper macros exist to make this easy so each test only has to be written once.

**The tests require Redis version >=6.2** As of writing the default version used is 7.0.5.

## Makefile

The easiest way to set up the tests is to use the [Makefile](../Makefile):

```
# setup
. tests/environ
make clean
make install

# run tests
make test-default-features
make test-all-features
make test-no-features
make test-sentinel-features
```

The make commands are thin wrappers around scripts in the [scripts](scripts) and [runners](runners) folders. The rest of this document will cover how to use those scripts by hand.

## Installation

The [environ](environ) file contains any environment variables that might be needed. **This should be loaded before installing or running any tests, unless otherwise set manually.**

In order to run the installation scripts the following must be installed:

* Bash (all the scripts assume `bash`)
* `build-essential` (or equivalent)
* OpenSSL (`libssl-dev`) for testing `native-tls` features without vendored OpenSSL dependencies
* `docker`
* `docker-compose` (this may come with `docker` depending on the version you use)

### Fresh Installation

A script is included to start a fresh install, but **it will shut down any local redis processes first, including those from docker.**

```
. tests/environ
./tests/scripts/full_install.sh
```

### Custom Installation

Redis installation scripts exist to install Redis at any version. Use the env variable `REDIS_VERSION` to configure this, either setting manually **after** sourcing the [environ](environ), or by changing this file. These scripts will only modify the contents of the [tests/tmp](../tests/tmp) folder. 

* [Install Centralized](scripts/install_redis_centralized.sh) will download, install, and start a centralized server on port 6379.
* [Install Clustered](scripts/install_redis_clustered.sh) will download, install, and start a clustered deployment on ports 30001-30006.
* [Install Sentinel](scripts/docker-install-redis-sentinel.sh) will download, install, and start a sentinel deployment with docker-compose.

The tests assume that redis servers are running on the above ports. The installation scripts will modify ACL rules, so installing redis via other means may not work with the tests as they're currently written unless users manually set up the test users as well.

## Running Tests

Once the required local Redis servers are installed users can run tests with the [runner](runners) scripts.

* [all-features](runners/all-features.sh) will run tests with all features (except chaos monkey and sentinel tests).
* [default-features](runners/default-features.sh) will run tests with default features (except sentinel tests).
* [no-features](runners/no-features.sh) will run the tests without any of the feature flags.
* [sentinel-features](runners/sentinel-features.sh) will run the centralized tests against a sentinel deployment. This is the only test runner that requires the sentinel deployment via docker-compose.
* [everything](runners/everything.sh) will run all of the above scripts. 

These scripts will pass through any extra argv so callers can filter tests as needed.

See the [CI configuration](../.circleci/config.yml) for more information.

There are 4 environment variables that can be used to control the host/port for the centralized or clustered servers used for the tests. The default values can be found in the [environ](./environ) file.

* FRED_REDIS_CLUSTER_HOST
* FRED_REDIS_CLUSTER_PORT
* FRED_REDIS_CENTRALIZED_HOST
* FRED_REDIS_CENTRALIZED_PORT

Callers can change these, but need to ensure the ACL rules are properly configured on the servers. A user with the name `$REDIS_USERNAME` and password `$REDIS_PASSWORD` needs full access to run any command. The installation scripts will automatically set these rules, but if callers use a different server they may need to manually create this user.

## TLS

There are some scripts to set up and run a cluster with TLS features enabled.

Note: These scripts use ports 30001-30006, which likely conflict with the non-TLS scripts above. 

### Setup

```
export REDIS_VERSION=7.0.5
cd path/to/fred.rs
. ./tests/environ
./tests/scripts/install_tls_cluster.sh
```

The setup script will mint key pairs for a CA, the client, and one per cluster node. Each cluster node cert has a different CN and SAN entries for every other node's hostname.

The nodes use hostnames of `node-$PORT.example.com`, and the setup script will prompt the caller to modify `/etc/hosts`. If callers don't want to do this they will need to configure DNS entries for each of these hostnames in some other way.

### Running TLS Tests

There are two scripts that run tests - one for `native-tls` and one for `rustls`. 

```
# native-tls
./tests/runners/cluster-tls.sh
# rustls
./tests/runners/cluster-rustls.sh
```

Note: The `native-tls` tests [may not work on OS X (Mac)](https://github.com/sfackler/rust-native-tls/issues/143).

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