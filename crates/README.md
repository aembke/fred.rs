Fred Crates
===========

The `fred` client is broken up into several separate crates.

* [fred](./fred) - The top level crate that exposes client interfaces and implements routing policies for the supported
  deployment types.
* [fred-common](./common) - Shared interfaces used by the client and outer interfaces. It's unlikely callers will have
  to interact directly with this crate unless they need to implement the `ClientLike` trait.

As well as separate crates for each of the public interface traits. The code is structured this way for several reasons:

* Compile times are adding up again. Moving all the features to their own crates allows these to be built in parallel.
* Valkey & Redis are starting to diverge more. I’ll likely need to add more feature flags and they may soon
  offer different interfaces that conflict with one another. There isn't currently a great way to deal with this when
  the client and interfaces are all in the same crate. More specifically, since Valkey/Redis use a
  stringly/loosely-typed interface and `fred` exposes a more strongly-typed interfaces there are often purely additive
  Valkey/Redis interface changes that require breaking changes in the corresponding Rust interface.
* It’s often difficult and annoying to require callers to update the entire client across major versions whenever an
  interface breaks in a way that does not affect the core client interfaces. With this model we can version the
  interfaces separately than the clients, or even expose multiple versions through `fred` feature flags to support
  partial upgrades. For example, at work we’re in the process of upgrading large version gaps, and in some cases
  switching to Valkey, but this will take months and most of our apps talk to multiple different clusters that may run
  different server versions with different interfaces.

## Interface Crates

The public command interfaces are implemented as a set of traits that are shared and implemented by the various client
types in [fred](./fred). These traits each have their own crates and are typically built when the corresponding feature
flag is provided when building `fred`. These crates generally have the following structure:

TODO