Contributing
===========

This document gives some background on how the library is structured and how to contribute.

# General

* Use 2 spaces instead of tabs.
* Run rustfmt before submitting any changes.
* Clean up any compiler warnings.
* Submit PRs against the `develop` branch.

## TODO List

* Add a FF for redis v7 changes (GET, XAUTOCLAIM, etc)
* Any missing commands.
* Support unix domain sockets

## Design 

# Adding Commands


## New Commands

## Command Files

### Interfaces Folder

### Impls Folder

## Type Conversions

## Public Interface

## Example

This example shows how to add `MGET` to the commands.

1. Add the new variant to the `RedisCommandKind` enum, if needed.

```rust
pub enum RedisCommandKind {
  // ...
  Mget,
  // ...
}

impl RedisCommandKind {
  
  // ..
  
  pub fn to_str_debug(&self) -> &'static str {
    match *self {
      // ..
      RedisCommandKind::Mget => "MGET",
      // ..
    }
  }
  
  // ..
  
  pub fn cmd_str(&self) -> &'static str {
    match *self {
      // .. 
      RedisCommandKind::Mget => "MGET"
      // ..
    }
  }
  
  // ..
}
```

2. Create the private function implementing the command in [src/commands/impls/keys.rs](src/commands/impls/keys.rs).

```rust
pub async fn mget(inner: &Arc<RedisClientInner>, keys: MultipleKeys) -> Result<RedisValue, RedisError> {
  utils::check_empty_keys(&keys)?;

  let frame = utils::request_response(inner, move || {
    // time spent here will show up in traces
    let args = keys.inner().into_iter().map(|k| k.into()).collect();
    Ok((RedisCommandKind::Mget, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
```

3. Create the public function in the [src/commands/interfaces/keys.rs](src/commands/interfaces/keys.rs) file. 

```rust

// ...

pub trait KeysInterface: ClientLike + Sized {
 
  // ...

  /// Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the special value nil is returned.
  ///
  /// <https://redis.io/commands/mget>
  fn mget<R, K>(&self, keys: K) -> AsyncResult<R> 
    where
      R: FromRedis + Unpin + Send,
      K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::keys::mget(&inner, keys).await?.convert()
    })
  }
  
  // ...
}
```

```rust
impl KeysInterface for RedisClient {}
```

# Adding Tests

Integration tests are in the [tests/integration](tests/integration) folder organized by category. See the tests [README](tests/README.md) for more information.

Using `MGET` as an example:

1. Write tests in the [keys](tests/integration/keys/mod.rs) file.

```rust
pub async fn should_mget_values(client: RedisClient, _: RedisConfig) -> Result<(), RedisError> {
  check_null!(client, "a{1}");
  check_null!(client, "b{1}");
  check_null!(client, "c{1}");

  let expected: Vec<(&str, RedisValue)> = vec![("a{1}", 1.into()), ("b{1}", 2.into()), ("c{1}", 3.into())];
  for (key, value) in expected.iter() {
    let _: () = client.set(*key, value.clone(), None, None, false).await?;
  }
  let values: Vec<i64> = client.mget(vec!["a{1}", "b{1}", "c{1}"]).await?;
  assert_eq!(values, vec![1, 2, 3]);

  Ok(())
}
```

2. Call the tests from the [centralized server tests](tests/integration/centralized.rs).

```rust
mod keys {
   
  // ..
  centralized_test!(keys, should_mget_values);
}

```

3. Call the tests from the [cluster server tests](tests/integration/cluster.rs).

```rust
mod keys {
  
  // ..
  cluster_test!(keys, should_mget_values);
}
```

This will generate test wrappers to call your test function against both centralized and clustered redis servers with pipelined and non-pipelined clients in RESP2 and RESP3 modes.

# Misc
