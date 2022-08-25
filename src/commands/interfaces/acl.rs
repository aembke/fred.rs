use crate::commands;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{AclRule, AclUser, FromRedis, MultipleStrings, RedisValue};
use crate::utils;
use bytes_utils::Str;

/// Functions that implement the [ACL](https://redis.io/commands#server) interface.
pub trait AclInterface: ClientLike + Sized {
  /// Create an ACL user with the specified rules or modify the rules of an existing user.
  ///
  /// <https://redis.io/commands/acl-setuser>
  fn acl_setuser<S>(&self, username: S, rules: Vec<AclRule>) -> AsyncResult<()>
  where
    S: Into<Str>,
  {
    into!(username);
    async_spawn(self, |_self| async move {
      commands::acl::acl_setuser(_self, username, rules).await
    })
  }

  /// When Redis is configured to use an ACL file (with the aclfile configuration option), this command will reload the
  /// ACLs from the file, replacing all the current ACL rules with the ones defined in the file.
  ///
  /// <https://redis.io/commands/acl-load>
  fn acl_load(&self) -> AsyncResult<()> {
    async_spawn(self, |_self| async move { commands::acl::acl_load(_self).await })
  }

  /// When Redis is configured to use an ACL file (with the aclfile configuration option), this command will save the
  /// currently defined ACLs from the server memory to the ACL file.
  ///
  /// <https://redis.io/commands/acl-save>
  fn acl_save(&self) -> AsyncResult<()> {
    async_spawn(self, |_self| async move { commands::acl::acl_save(_self).await })
  }

  /// The command shows the currently active ACL rules in the Redis server.
  ///
  /// <https://redis.io/commands/acl-list>
  fn acl_list<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(
      self,
      |_self| async move { commands::acl::acl_list(_self).await?.convert() },
    )
  }

  /// The command shows a list of all the usernames of the currently configured users in the Redis ACL system.
  ///
  /// <https://redis.io/commands/acl-users>
  fn acl_users<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(
      self,
      |_self| async move { commands::acl::acl_users(_self).await?.convert() },
    )
  }

  /// The command returns all the rules defined for an existing ACL user.
  ///
  /// <https://redis.io/commands/acl-getuser>
  fn acl_getuser<S>(&self, username: S) -> AsyncResult<Option<AclUser>>
  where
    S: Into<Str>,
  {
    into!(username);
    async_spawn(self, |_self| async move {
      commands::acl::acl_getuser(_self, username).await
    })
  }

  /// Delete all the specified ACL users and terminate all the connections that are authenticated with such users.
  ///
  /// <https://redis.io/commands/acl-deluser>
  fn acl_deluser<R, S>(&self, usernames: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<MultipleStrings>,
  {
    into!(usernames);
    async_spawn(self, |_self| async move {
      commands::acl::acl_deluser(_self, usernames).await?.convert()
    })
  }

  /// The command shows the available ACL categories if called without arguments. If a category name is given,
  /// the command shows all the Redis commands in the specified category.
  ///
  /// <https://redis.io/commands/acl-cat>
  fn acl_cat(&self, category: Option<Str>) -> AsyncResult<Vec<String>> {
    async_spawn(self, |_self| async move {
      commands::acl::acl_cat(_self, category).await?.convert()
    })
  }

  /// Generate a password with length `bits`, returning the password.
  ///
  /// <https://redis.io/commands/acl-genpass>
  fn acl_genpass(&self, bits: Option<u16>) -> AsyncResult<String> {
    async_spawn(self, |_self| async move {
      commands::acl::acl_genpass(_self, bits).await?.convert()
    })
  }

  /// Return the username the current connection is authenticated with. New connections are authenticated
  /// with the "default" user.
  ///
  /// <https://redis.io/commands/acl-whoami>
  fn acl_whoami(&self) -> AsyncResult<String> {
    async_spawn(self, |_self| async move {
      commands::acl::acl_whoami(_self).await?.convert()
    })
  }

  /// Read `count` recent ACL security events.
  ///
  /// <https://redis.io/commands/acl-log>
  fn acl_log_count(&self, count: Option<u32>) -> AsyncResult<RedisValue> {
    async_spawn(
      self,
      |_self| async move { commands::acl::acl_log_count(_self, count).await },
    )
  }

  /// Clear the ACL security events logs.
  ///
  /// <https://redis.io/commands/acl-log>
  fn acl_log_reset(&self) -> AsyncResult<()> {
    async_spawn(self, |_self| async move { commands::acl::acl_log_reset(_self).await })
  }
}
