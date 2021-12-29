FAQ
===

## Does this work with Actix?

Fred requires tokio >=1.0 to work, but the current stable major version of actix (3.x) uses tokio 0.2. The current beta version of actix (4.x-beta.x) uses tokio 1.x, which does work with fred. 

However, some users have [reported issues with that version of actix](https://github.com/aembke/fred.rs/issues/13) that can affect using fred. 

## I'm not using Tokio - can I still use this?

Unfortunately no, not really. I've found tokio to be just "too good to avoid", and it supports everything I wanted to do with this library and more. There are no plans to support `async-std`, but anybody is welcome to fork this and give it a try. If you need `async-std` support, or you don't want to use tokio I would recommend [redis-rs](https://crates.io/crates/redis), which is also a really good option.  

In my opinion Tokio is really great though, and I highly recommend that callers consider using it if an async runtime is needed.

## The command I want is missing. Can I still use that?

The `custom` and `custom_raw` functions on the `ClientLike` trait can run any command, and callers have full control over arguments and response types.

Feel free to file an issue though if you don't see a command implemented that you need. 

## Does this work with ElastiCache?

Yes, but ElastiCache has its own oddities that callers need to keep in mind. 

I [_highly_ recommend](https://github.com/StackExchange/StackExchange.Redis/issues/1273#issuecomment-651823824) turning on the `reconnect-on-auth-error` feature if callers use ElastiCache. 

## How is the test coverage?

The underlying protocol parser library has great test coverage (close to 100%, excluding error generation boilerplate).

This library itself has pretty good test coverage, but it could always be better. All in all there are about 1300 tests that are designed to cover real use cases from both the Redis docs website and my personal experience with Redis over the years. Pretty much every test will run multiple times to check the interaction between clustered servers, centralized servers, sentinel servers, pipelined clients, non-pipelined clients, and RESP2 or RESP3 modes.

All of the integration tests run against real, live Redis servers. In the past I tried using a mocking layer for the tests, but it proved to be unsustainable, and often didn't help prevent bugs. Redis just moves too fast and in my experience there's simply no replacement for real feedback from a real server in the tests.

The one area that is lacking test coverage is the CLUSTER command category. Those are difficult to test because they modify the cluster and are difficult to revert between test runs. If the compiler allowed for custom test orderings this would be easier to do, but at the moment it's pretty complicated. This is on the TODO list, however.

## How is the performance?

It's pretty good in general, especially when the `pipeline` flag is enabled. I can do about 2MM req/sec in one process on my desktop (12 cores, 64 GB, Debian 10) running against a single centralized Redis server. When I try to do more the Redis server starts blowing up memory buffering commands, so I haven't tried to push it too much beyond that. In my experience most deployments don't vertically scale that much anyways - apps tend to horizontally scale instead.

The hot path on all commands only requires taking one write/mutex lock (a requirement for pipelining), and the rest is lock-free due to the use of message passing semantics throughout the implementation. There are a small number of commands that require reading another lock or two, but in general the hot path is largely lock-free. If you're reading the code you may see quite a few `RwLock` types, but the vast majority of those are not in the hot path. There are a few that are that I haven't mentioned since they're not actually used in a way where there can be any contention. However, since a lot of the Tokio interface requires that types be at least `Send` (and often `Sync + 'static` too), I often didn't have a choice but to wrap a type in `Arc<RwLock<T>` for mutability purposes even though there will never be any contention on the lock.

A general rule in this library is that it never clones user data. The one exception to this is the required underlying copy that happens when data is sent out over the socket, but otherwise this library will never clone a value given as an argument or return type. This is another reason why there are some `Arc<RwLock/Mutex<T>>` types. Given that we're talking to a server over the network, and the vast majority of these locked values will never see any contention, these added abstractions will rarely be the bottleneck in my experience.

There are still some performance improvements I would like to make, however. Some less-hot code paths require taking locks, and I would like to switch to using `ArcSwap` in a few cases. The protocol parsing logic is pretty well optimized, but I'm sure it could be better too. `nom` is very fast though, and the parser really isn't that complicated, so I doubt there is much low-hanging fruit in this area. 

My suspicion is that it is likely possible to significantly improve performance, but at the cost of making parts of the interface less generic. Using tasks pinned to a single thread where lock-free thread-local storage is available would likely help quite a bit, but that would require making some assumptions about application use cases, and would introduce potential for hotspotting on certain threads. This seems like an interesting exercise, so I might implement it, but it's not high on the priority list.

The good news is that the interface is designed in such a way that many performance improvements can be made that will not require any code changes from callers that use this library. In almost every case the best performance improvement you're likely to see will come from enabling the pipelining features.

## Is this library actively maintained?

Yes, and I'm planning on keeping it up-to-date in the future. I use this at work so at a minimum there will always be somebody scanning it for vulns.

This is designed to be suitable for production use cases, and I know how frustrating it is to take a dependency on a library only to find it abandoned in a month. If for whatever reason this library stops being maintained I'll notify users via the main README if I can't find a new owner. 

## Should I use the RESP3 mode?

Maybe. RESP3 can be quite a bit different from RESP2 for certain commands, especially sorted sets and commands that return aggregate types (arrays, maps, sets, etc). 

If you have a bunch of code written against fred in RESP2 mode you will need to be careful. If you are starting fresh then you can probably start with RESP3 and things will work fine.

The general rule of thumb is that both modes work fine on their own, they're just different. Some of these differences can affect callers in that some commands return different payloads. If you have good test coverage in your app layer you may notice this in the form of failing tests (which is good), but if not then switching modes may introduce bugs.

To give an example:

When I first added RESP3 support to fred I added a test macro to run every test in both RESP2 and RESP3 mode. After doing that about 80% of the tests that operate on `RedisValue` enums directly started failing in RESP3 mode. However, the tests that relied on the `FromRedis` trait generally worked fine in both modes. 

The addition of the `Double`, `Boolean`, and `Map` enum variants can require invasive changes from callers that rely on `match` statements against `RedisValue` enums.

Also, some commands can nest response values differently in RESP2 mode. For example, `HRANDFIELD` returns a flat array in RESP2 but a nested array in RESP3. This can affect callers that rely on `FromRedis` type conversions as well. 

I did my best to normalize response types across RESP2 and RESP3 for callers, but I'm sure there's some cases that are still different between the two modes. 

Generally speaking there aren't many compelling reasons (that I know of at least) to use RESP3 at the moment, but that might change.

## How do I use this with minikube or "non-host networking" docker?

This is a common use case - please see the [minikube](tests/minikube.md) docs.

## Why do some commands return strongly-typed structs, but others return generic values?

In my experience there are some commands that are a pain to use no matter what. 

The `MEMORY` interface and `GEO` interface for example both return complicated arrays or maps of fields that can require significant parsing by callers before they're actually useful. In these situations I implemented parsing logic and returned structs instead of generic maps or arrays.

However, the vast majority of commands return generic types, and this library just returns those directly. If your use case requires generic types from a command that parses responses automatically please see the `custom` and `custom_raw` commands for a mechanism to avoid that parsing. 

## Why are there so many types that I need to import?

Redis has a very "string-y" interface, so a lot of folks are probably more familiar using it with something that looks like a REPL where you can just write everything as a string.

However, since we're using Rust here I wanted to leverage the compiler as much as possible. If you're reading this then you're probably familiar with the benefits of strongly typed languages, and the added benefits Rust provides on top of those benefits, so I won't get into that.

It's very easy to introduce typos in your commands or arguments when using a loosely-typed language with a "string-y" interface like Redis, and I wanted to prevent as many of those issues as possible at build time. As a result a lot of strongly-typed intermediate structs were defined to give the compiler as much information about command arguments or return types as possible. 

Practically speaking callers can still use this where everything is a string, but I would highly recommend that callers consider using the intermediate structs directly so the compiler can validate as many of your type declarations as possible. I tried to strike a balance with this library where most commands work with both strongly typed and looser, more generic argument types, so hopefully this design decision helps rather than hurts usability. If you have any feedback on how to make this easier to use without sacrificing the underlying strongly-typed nature of the interface please let me know.

In addition to the compiler helping out quite a bit here this library will do quite a bit of client-side validation. It will perform checks to ensure commands that operate on multiple keys all map to the same cluster node for example, as well as a number of other things. The general goal is that if the compiler can't help prevent a bug hopefully this library's validation layer will help the first time you try running your code.

## Why are there so many features and config options?

In my experience using Redis at scale can be quite difficult for reasons that have nothing to do with the core interface or your business logic.

Safely managing connections such that they can survive bad network conditions, ElastiCache's strange cluster rebalancing logic, etc, can require invasive code changes in your app layer. This library was built to abstract away as many of these issues as possible.

Nearly every feature flag or strange config option came about from an incident fighting a large Redis deployment in production. A lot of these work items made their way into the app layer over the years, but I did my best in the recent rewrite to move as many of them into this library as possible so they can help prevent issues for others. 

I hope callers feel comfortable using this in production, and maybe some of these features or config options can help prevent or shine a light on a potential issue to consider when using Redis at scale. 

I prefer to use clients that offer robust configuration options such that I can think about my resilience needs up front when I configure the client, but after that I prefer to focus on business logic without worrying about resilience issues. A lot of the configuration interface was built with this in mind.

It is possible to configure this library such that even in the event of a total Redis outage the client doesn't lose any data, and the moment your Redis deployment comes back up everything starts working again without any manual intervention in the app layer. This assumes some things about your app, but it is possible. It can take quite a few feature flags and config options to enable this level of durability and resilience on both the client and server, hence the large number of build and runtime options in this library.

With all of that said, I have a request for callers that use this library. If you use this in production and have an outage or major issue as a result of _anything_ related to Redis, please consider filing an issue with the "RCA" label, even if it's just to let me know about it. If you find yourself saying "this could have been avoided if the client did X", please let me know what X is, and I'll strongly consider adding it. I also use this in production at scale and would like to avoid as many issues as possible.

## How can I contribute?

See the [contributing](CONTRIBUTING.md) docs, or feel free to email me at the email on my github profile. I am looking for maintainers, so please reach out if you're interested.