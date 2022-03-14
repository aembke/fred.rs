SHELL := /bin/bash

.PHONY: clean install test-all-features test-default-features test-no-features test-sentinel test-all test

clean:
	rm -rf tests/tmp/redis* && cargo clean

install:
	source tests/environ && tests/scripts/full_install.sh

test-all-features:
	source tests/environ && tests/runners/all-features.sh

test-default-features:
	source tests/environ && tests/runners/default-features.sh

test-no-features:
	source tests/environ && tests/runners/no-features.sh

test-sentinel:
	source tests/environ && tests/runners/sentinel-features.sh

test-all:
	source tests/environ && tests/runners/everything.sh

test: test-default-features