Chaos Monkey
============

![](./img.png)

A module that runs during tests to randomly stop, start, restart, and rebalance cluster slots. This will automatically enable AOF features on the servers.

This should make the tests take slightly longer but should not produce any errors **as long as AOF features are enabled.** In the wild if callers aren't using any persistence settings on Redis then there shouldn't be any expectation that restarting servers is safe.

See the [run_with_chaos_monkey.sh](../run_with_chaos_monkey.sh) script for more information.