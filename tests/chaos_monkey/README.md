Chaos Monkey
============

![](./img.png)

A module that runs during tests to randomly stop, start, restart, and rebalance cluster slots. This should make the tests take slightly longer but should not produce any errors **as long as AOF features are enabled.**

See the [run_with_chaos_monkey.sh](../run_with_chaos_monkey.sh) script for configuration options and examples.