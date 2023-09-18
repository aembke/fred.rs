use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, MultipleStrings, MultipleValues, RedisValue},
};
use bytes_utils::Str;

/// Functions that implement the [ACL](https://redis.io/commands#server) interface.
#[async_trait]
pub trait AclInterface: ClientLike + Sized {
  /// Create an ACL user with the specified rules or modify the rules of an existing user.
  ///
  /// <https://redis.io/commands/acl-setuser>
  async fn acl_setuser<S, V>(&self, username: S, rules: V) -> RedisResult<()>
  where
    S: Into<Str> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(username);
    try_into!(rules);
    commands::acl::acl_setuser(self, username, rules).await
  }

  /// When Redis is configured to use an ACL file (with the aclfile configuration option), this command will reload
  /// the ACLs from the file, replacing all the current ACL rules with the ones defined in the file.
  ///
  /// <https://redis.io/commands/acl-load>
  async fn acl_load(&self) -> RedisResult<()> {
    commands::acl::acl_load(self).await
  }

  /// When Redis is configured to use an ACL file (with the aclfile configuration option), this command will save the
  /// currently defined ACLs from the server memory to the ACL file.
  ///
  /// <https://redis.io/commands/acl-save>
  async fn acl_save(&self) -> RedisResult<()> {
    commands::acl::acl_save(self).await
  }

  /// The command shows the currently active ACL rules in the Redis server.
  ///
  /// <https://redis.io/commands/acl-list>
  async fn acl_list<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::acl::acl_list(self).await?.convert()
  }

  /// The command shows a list of all the usernames of the currently configured users in the Redis ACL system.
  ///
  /// <https://redis.io/commands/acl-users>
  async fn acl_users<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::acl::acl_users(self).await?.convert()
  }

  /// The command returns all the rules defined for an existing ACL user.
  ///
  /// <https://redis.io/commands/acl-getuser>
  async fn acl_getuser<R, S>(&self, username: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    into!(username);
    commands::acl::acl_getuser(self, username).await?.convert()
  }

  /// Delete all the specified ACL users and terminate all the connections that are authenticated with such users.
  ///
  /// <https://redis.io/commands/acl-deluser>
  async fn acl_deluser<R, S>(&self, usernames: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<MultipleStrings> + Send,
  {
    into!(usernames);
    commands::acl::acl_deluser(self, usernames).await?.convert()
  }

  /// The command shows the available ACL categories if called without arguments. If a category name is given,
  /// the command shows all the Redis commands in the specified category.
  ///
  /// <https://redis.io/commands/acl-cat>
  async fn acl_cat(&self, category: Option<Str>) -> RedisResult<Vec<String>> {
    commands::acl::acl_cat(self, category).await?.convert()
  }

  /// Generate a password with length `bits`, returning the password.
  ///
  /// <https://redis.io/commands/acl-genpass>
  async fn acl_genpass(&self, bits: Option<u16>) -> RedisResult<String> {
    commands::acl::acl_genpass(self, bits).await?.convert()
  }

  /// Return the username the current connection is authenticated with. New connections are authenticated
  /// with the "default" user.
  ///
  /// <https://redis.io/commands/acl-whoami>
  async fn acl_whoami(&self) -> RedisResult<String> {
    commands::acl::acl_whoami(self).await?.convert()
  }

  /// Read `count` recent ACL security events.
  ///
  /// <https://redis.io/commands/acl-log>
  async fn acl_log_count(&self, count: Option<u32>) -> RedisResult<RedisValue> {
    commands::acl::acl_log_count(self, count).await
  }

  /// Clear the ACL security events logs.
  ///
  /// <https://redis.io/commands/acl-log>
  async fn acl_log_reset(&self) -> RedisResult<()> {
    commands::acl::acl_log_reset(self).await
  }
}
