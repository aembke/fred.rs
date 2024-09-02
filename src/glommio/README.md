## Runtime Differences

Tokio and Glommio have several important differences that can affect the public interface of client libraries like
`fred`, most notably that Tokio typically uses a work-stealing scheduler whereas Glommio intentionally
discourages moving tasks or state across threads. For more information see
the [Glommio Introduction](https://www.datadoghq.com/blog/engineering/introducing-glommio/).

This fundamental difference manifests in several ways, for example:

* [tokio::spawn](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html) requires a `Send + 'static` bound on anything
  that moves in to or out of the task being spawned. This makes sense since the Tokio scheduler may choose to move tasks
  across thread boundaries for work stealing purposes.
* In general Glommio prefers that each scheduler thread share nothing with other scheduler threads, typically
  implemented via the [sharding](https://docs.rs/glommio/latest/glommio/channels/sharding/index.html) interface. As a
  result, the [task spawning interfaces](https://docs.rs/glommio/latest/glommio/index.html#functions) such
  as [spawn_local](https://docs.rs/glommio/latest/glommio/fn.spawn_local.html)
  and [spawn_local_into](https://docs.rs/glommio/latest/glommio/fn.spawn_local_into.html), are designed to
  spawn tasks on the same thread and therefore do not have
  a `Send` bound.

`fred` was originally written with message-passing design patterns targeting a Tokio runtime and therefore the `Send`
bound from `tokio::spawn` leaked into all the public interfaces that send messages across tasks. This includes pretty
much everything in the public interface:

* All the `*Interface` traits that implement Redis commands. At the moment this includes something like ~200 functions.
* The base `ClientLike` trait behind all the public client and pool types.

See the [next section](#generic-interfaces-with-glommio) for more information.

Additionally, Glommio uses or requires some different implementations of some core interfaces used inside `fred`,
such as:

* [tokio::sync::oneshot](https://docs.rs/tokio/latest/tokio/sync/oneshot/index.html) must change
  to [async-oneshot](https://crates.io/crates/async-oneshot).
* [tokio_util::codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html) needs a compatibility layer or a
  re-implementation of `Encoder+Decoder` on top of the `AsyncRead+AsyncWrite` traits in `futures-io`. Currently, Tokio
  re-implements these traits internally. The compatibility layer in [src/glommio/io_compat.rs](./io_compat.rs) also
  allows `tokio-native-tls` and `tokio-rustls` to work with Glommio's `TcpStream`.
* Most tokio message passing `send` functions are synchronous, but most glommio `send` functions are async. To work
  around this the compat layer uses unbounded channels
  with [try_send](https://docs.rs/glommio/latest/glommio/channels/local_channel/struct.LocalSender.html#method.try_send).

Generally speaking callers won't be affected by these implementation details, but it is worth noting that `fred` still
requires some Tokio functionality (such as `select!`) when using Glommio. See [the compat layer](./compat.rs) for more
info.

## Generic Interfaces with Glommio

When building with `--features glommio` the public interfaces mentioned above will lose the `Send` bounds on all the
generic input parameters, where clause predicates, and `impl Trait` return types. This is done via
the [rm-send-macros](https://github.com/aembke/rm-send-macros) crate. The public docs will still show the `Send` bounds,
but callers can rebuild docs manually with the `glommio` feature to see the new function signatures.

```
cargo rustdoc --features "glommio i-all i-redis-stack" --open -- --cfg docsrs
```

In the future this approach may change to use separate interface traits for Glommio builds.

The modules in this folder try to bridge any compatibility gaps between the various message passing interfaces in Tokio
and Glommio.

## TODO

* Counters no longer need to be atomic when ClientLike is not Send.
* All the parking_lot locks can change to RefCell.
* Support the `ExclusivePool` interface with Glommio. Currently there's no equivalent to Tokio's `OwnedMutexGuard`.