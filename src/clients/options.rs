use crate::{
  error::RedisError,
  interfaces::{
    AclInterface,
    AuthInterface,
    ClientInterface,
    ClientLike,
    ClusterInterface,
    ConfigInterface,
    FunctionInterface,
    GeoInterface,
    HashesInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    MemoryInterface,
    PubsubInterface,
    ServerInterface,
    SetsInterface,
    SlowlogInterface,
    SortedSetsInterface,
    StreamsInterface,
  },
  modules::inner::RedisClientInner,
  protocol::command::RedisCommand,
  types::Options,
};
use std::{fmt, sync::Arc};

/// A client interface used to customize command configuration options.
///
/// See [Options](crate::types::Options) for more information.
///
/// ```rust
/// # use fred::prelude::*;
/// # use std::time::Duration;
/// async fn example() -> Result<(), RedisError> {
///   let client = RedisClient::default();
///   let _ = client.connect();
///   let _ = client.wait_for_connect().await?;
///
///   let options = Options::default()
///     .with_max_attempts(5)
///     .with_max_redirections(3)
///     .with_timeout(Duration::from_secs(30));
///
///   let foo: Option<String> = client.with_options(&options).get("foo").await?;
///
///   // or reuse the options bindings
///   let with_safe_options = client.with_options(&options);
///   let foo: () = with_safe_options.get("foo").await?;
///   let bar: () = with_safe_options.get("bar").await?;
///
///   Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct WithOptions<C: ClientLike> {
  pub(crate) client:  C,
  pub(crate) options: Options,
}

impl<'a, C: ClientLike> fmt::Debug for WithOptions<C> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("WithOptions")
      .field("client", &self.client.id())
      .field("options", &self.options)
      .finish()
  }
}

// TODO make sure the attempts and redirection counts are set correctly on all the old code paths

impl<C: ClientLike> ClientLike for WithOptions<C> {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    self.client.inner()
  }

  #[doc(hidden)]
  fn change_command(&self, command: &mut RedisCommand) {
    self.client.change_command(command);
    self.options.apply(command);
  }

  #[doc(hidden)]
  fn send_command<T>(&self, command: T) -> Result<(), RedisError>
  where
    T: Into<RedisCommand>,
  {
    let mut command: RedisCommand = command.into();
    self.options.apply(&mut command);
    self.client.send_command(command)
  }
}

impl<C: AclInterface> AclInterface for WithOptions<C> {}
impl<C: ClientInterface> ClientInterface for WithOptions<C> {}
impl<C: ClusterInterface> ClusterInterface for WithOptions<C> {}
impl<C: PubsubInterface> PubsubInterface for WithOptions<C> {}
impl<C: ConfigInterface> ConfigInterface for WithOptions<C> {}
impl<C: GeoInterface> GeoInterface for WithOptions<C> {}
impl<C: HashesInterface> HashesInterface for WithOptions<C> {}
impl<C: HyperloglogInterface> HyperloglogInterface for WithOptions<C> {}
impl<C: KeysInterface> KeysInterface for WithOptions<C> {}
impl<C: ListInterface> ListInterface for WithOptions<C> {}
impl<C: MemoryInterface> MemoryInterface for WithOptions<C> {}
impl<C: AuthInterface> AuthInterface for WithOptions<C> {}
impl<C: ServerInterface> ServerInterface for WithOptions<C> {}
impl<C: SlowlogInterface> SlowlogInterface for WithOptions<C> {}
impl<C: SetsInterface> SetsInterface for WithOptions<C> {}
impl<C: SortedSetsInterface> SortedSetsInterface for WithOptions<C> {}
impl<C: StreamsInterface> StreamsInterface for WithOptions<C> {}
impl<C: FunctionInterface> FunctionInterface for WithOptions<C> {}
