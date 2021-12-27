use crate::commands;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{
  ClusterFailoverFlag, ClusterInfo, ClusterKeyCache, ClusterResetFlag, ClusterSetSlotState, FromRedis,
  MultipleHashSlots, RedisKey, RedisValue,
};
use crate::utils;

/// Functions that implement the [CLUSTER](https://redis.io/commands#cluster) interface.
pub trait ClusterInterface: ClientLike + Sized {
  /// Whether or not the client is using a clustered Redis deployment.
  fn is_clustered(&self) -> bool {
    utils::is_clustered(&self.inner().config)
  }

  /// Read the cached state of the cluster used for routing commands to the correct cluster nodes.
  fn cached_cluster_state(&self) -> Option<ClusterKeyCache> {
    self.inner().cluster_state.read().clone()
  }

  /// Advances the cluster config epoch.
  ///
  /// <https://redis.io/commands/cluster-bumpepoch>
  fn cluster_bumpepoch<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_bumpepoch(&inner).await?.convert()
    })
  }

  /// Deletes all slots from a node.
  ///
  /// <https://redis.io/commands/cluster-flushslots>
  fn cluster_flushslots(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_flushslots(&inner).await
    })
  }

  /// Returns the node's id.
  ///
  /// <https://redis.io/commands/cluster-myid>
  fn cluster_myid<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_myid(&inner).await?.convert()
    })
  }

  /// Read the current cluster node configuration.
  ///
  /// Note: The client keeps a cached, parsed version of the cluster state in memory available at [cached_cluster_state](Self::cached_cluster_state).
  ///
  /// <https://redis.io/commands/cluster-nodes>
  fn cluster_nodes(&self) -> AsyncResult<String> {
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_nodes(&inner).await?.convert()
    })
  }

  /// Forces a node to save the nodes.conf configuration on disk.
  ///
  /// <https://redis.io/commands/cluster-saveconfig>
  fn cluster_saveconfig(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_saveconfig(&inner).await
    })
  }

  /// CLUSTER SLOTS returns details about which cluster slots map to which Redis instances.
  ///
  /// <https://redis.io/commands/cluster-slots>
  fn cluster_slots(&self) -> AsyncResult<RedisValue> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_slots(&inner).await
    })
  }

  /// CLUSTER INFO provides INFO style information about Redis Cluster vital parameters.
  ///
  /// <https://redis.io/commands/cluster-info>
  fn cluster_info(&self) -> AsyncResult<ClusterInfo> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_info(&inner).await
    })
  }

  /// This command is useful in order to modify a node's view of the cluster configuration. Specifically it assigns a set of hash slots to the node receiving the command.
  ///
  /// <https://redis.io/commands/cluster-addslots>
  fn cluster_add_slots<S>(&self, slots: S) -> AsyncResult<()>
  where
    S: Into<MultipleHashSlots>,
  {
    into!(slots);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_add_slots(&inner, slots).await
    })
  }

  /// The command returns the number of failure reports for the specified node.
  ///
  /// <https://redis.io/commands/cluster-count-failure-reports>
  fn cluster_count_failure_reports<R, S>(&self, node_id: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<String>,
  {
    into!(node_id);
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_count_failure_reports(&inner, node_id)
        .await?
        .convert()
    })
  }

  /// Returns the number of keys in the specified Redis Cluster hash slot.
  ///
  /// <https://redis.io/commands/cluster-countkeysinslot>
  fn cluster_count_keys_in_slot<R>(&self, slot: u16) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_count_keys_in_slot(&inner, slot)
        .await?
        .convert()
    })
  }

  /// The CLUSTER DELSLOTS command asks a particular Redis Cluster node to forget which master is serving the hash slots specified as arguments.
  ///
  /// <https://redis.io/commands/cluster-delslots>
  fn cluster_del_slots<S>(&self, slots: S) -> AsyncResult<()>
  where
    S: Into<MultipleHashSlots>,
  {
    into!(slots);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_del_slots(&inner, slots).await
    })
  }

  /// This command, that can only be sent to a Redis Cluster replica node, forces the replica to start a manual failover of its master instance.
  ///
  /// <https://redis.io/commands/cluster-failover>
  fn cluster_failover(&self, flag: Option<ClusterFailoverFlag>) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_failover(&inner, flag).await
    })
  }

  /// The command is used in order to remove a node, specified via its node ID, from the set of known nodes of the Redis Cluster node receiving the command.
  /// In other words the specified node is removed from the nodes table of the node receiving the command.
  ///
  /// <https://redis.io/commands/cluster-forget>
  fn cluster_forget<S>(&self, node_id: S) -> AsyncResult<()>
  where
    S: Into<String>,
  {
    into!(node_id);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_forget(&inner, node_id).await
    })
  }

  /// The command returns an array of keys names stored in the contacted node and hashing to the specified hash slot.
  ///
  /// <https://redis.io/commands/cluster-getkeysinslot>
  fn cluster_get_keys_in_slot<R>(&self, slot: u16, count: u64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_get_keys_in_slot(&inner, slot, count)
        .await?
        .convert()
    })
  }

  /// Returns an integer identifying the hash slot the specified key hashes to.
  ///
  /// <https://redis.io/commands/cluster-keyslot>
  fn cluster_keyslot<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_keyslot(&inner, key).await?.convert()
    })
  }

  /// CLUSTER MEET is used in order to connect different Redis nodes with cluster support enabled, into a working cluster.
  ///
  /// <https://redis.io/commands/cluster-meet>
  fn cluster_meet<S>(&self, ip: S, port: u16) -> AsyncResult<()>
  where
    S: Into<String>,
  {
    into!(ip);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_meet(&inner, ip, port).await
    })
  }

  /// The command reconfigures a node as a replica of the specified master. If the node receiving the command is an empty master, as
  /// a side effect of the command, the node role is changed from master to replica.
  ///
  /// <https://redis.io/commands/cluster-replicate>
  fn cluster_replicate<S>(&self, node_id: S) -> AsyncResult<()>
  where
    S: Into<String>,
  {
    into!(node_id);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_replicate(&inner, node_id).await
    })
  }

  /// The command provides a list of replica nodes replicating from the specified master node.
  ///
  /// <https://redis.io/commands/cluster-replicas>
  fn cluster_replicas<S>(&self, node_id: S) -> AsyncResult<String>
  where
    S: Into<String>,
  {
    into!(node_id);
    async_spawn(self, |inner| async move {
      commands::cluster::cluster_replicas(&inner, node_id).await?.convert()
    })
  }

  /// Reset a Redis Cluster node, in a more or less drastic way depending on the reset type, that can be hard or soft. Note that
  /// this command does not work for masters if they hold one or more keys, in that case to completely reset a master node keys
  /// must be removed first, e.g. by using FLUSHALL first, and then CLUSTER RESET.
  ///
  /// <https://redis.io/commands/cluster-reset>
  fn cluster_reset(&self, mode: Option<ClusterResetFlag>) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_reset(&inner, mode).await
    })
  }

  /// This command sets a specific config epoch in a fresh node.
  ///
  /// <https://redis.io/commands/cluster-set-config-epoch>
  fn cluster_set_config_epoch(&self, epoch: u64) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_set_config_epoch(&inner, epoch).await
    })
  }

  /// CLUSTER SETSLOT is responsible of changing the state of a hash slot in the receiving node in different ways.
  ///
  /// <https://redis.io/commands/cluster-setslot>
  fn cluster_setslot(&self, slot: u16, state: ClusterSetSlotState) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::cluster::cluster_setslot(&inner, slot, state).await
    })
  }
}
