# Glommio

See the [Glommio Introduction](https://www.datadoghq.com/blog/engineering/introducing-glommio/) for more info.

Tokio and Glommio have an important difference in their scheduling interfaces:

* [tokio::spawn](https://docs.rs/tokio/latest/tokio/task/fn.spawn.html) requires a `Send` bound on the spawned
  future so that the Tokio scheduler can implement work-stealing across threads.
* Glommio's scheduling interface is intended to be used in cases where runtime threads do not need to share or
  synchronize any state. Both the [spawn_local](https://docs.rs/glommio/latest/glommio/fn.spawn_local.html)
  and [spawn_local_into](https://docs.rs/glommio/latest/glommio/fn.spawn_local_into.html) functions
  spawn tasks on the same thread and therefore do not have a `Send` bound.

`fred` was originally written with message-passing design patterns targeting a Tokio runtime and therefore the `Send`
bound from `tokio::spawn` leaked into all the public interfaces that send messages across tasks. This includes nearly
all the public command traits, including the base `ClientLike` trait.

When building with `--features glommio` the public interface will change in several ways:

* The `Send + Sync` bounds will be removed from all generic input parameters, where clause predicates, and `impl Trait`
  return types.
* Internal `Arc` usages will change to `Rc`.
* Internal `RwLock` and `Mutex` usages will change to `RefCell`.
* Internal usages of `std::sync::atomic` types will change to thin wrappers around a `RefCell`.
* Any Tokio message passing interfaces (`BroadcastSender`, etc) will change to the closest Glommio equivalent.
* A Tokio compatability layer will be used to map between the two runtime's versions of `AsyncRead` and
  `AsyncWrite`. This enables the existing codec interface (`Encoder` + `Decoder`) to work with Glommio's network types.
  As a result, for now some Tokio dependencies are still required when using Glommio features.

[Glommio Example](https://github.com/aembke/fred.rs/blob/main/examples/glommio.rs)

The public docs
on [docs.rs](https://docs.rs/fred/latest) will continue to show the Tokio interfaces that require `Send` bounds, but
callers can find the latest rustdocs for both runtimes on the
`gh-pages` branch:

[Glommio Documentation](https://aembke.github.io/fred.rs/glommio/fred/index.html)

[Tokio Documentation](https://aembke.github.io/fred.rs/tokio/fred/index.html)

Callers can rebuild Glommio docs via the [doc-glommio.sh](../../tests/doc-glommio.sh) script:

```
path/to/fred/tests/doc-glommio.sh --open
```