mod keys {

  cluster_test!(keys, should_set_and_get_a_value);
  cluster_test!(keys, should_set_and_del_a_value);
  cluster_test!(keys, should_set_with_get_argument);
  cluster_test!(keys, should_incr_and_decr_a_value);
  cluster_test!(keys, should_incr_by_float);
  cluster_test!(keys, should_mset_a_non_empty_map);
  cluster_test_panic!(keys, should_error_mset_empty_map);
  cluster_test!(keys, should_expire_key);
  cluster_test!(keys, should_persist_key);
  cluster_test!(keys, should_check_ttl);
  cluster_test!(keys, should_check_pttl);
  cluster_test!(keys, should_dump_key);
  cluster_test!(keys, should_dump_and_restore_key);
  cluster_test!(keys, should_modify_ranges);
  cluster_test!(keys, should_getset_value);
  cluster_test!(keys, should_getdel_value);
  cluster_test!(keys, should_get_strlen);
  cluster_test!(keys, should_mget_values);
  cluster_test!(keys, should_msetnx_values);
  cluster_test!(keys, should_copy_values);
}

mod multi {

  cluster_test!(multi, should_run_get_set_trx);
  cluster_test_panic!(multi, should_fail_with_hashslot_error);
  cluster_test_panic!(multi, should_run_error_get_set_trx);
  cluster_test_panic!(multi, should_fail_with_blocking_cmd);
}

mod other {

  #[cfg(all(not(feature = "chaos-monkey"), feature = "metrics"))]
  cluster_test!(other, should_track_size_stats);

  cluster_test!(other, should_split_clustered_connection);
  cluster_test!(other, should_run_flushall_cluster);
  cluster_test!(other, should_automatically_unblock);
  cluster_test!(other, should_manually_unblock);
  cluster_test!(other, should_error_when_blocked);
}

mod hashes {

  cluster_test!(hashes, should_hset_and_hget);
  cluster_test!(hashes, should_hset_and_hdel);
  cluster_test!(hashes, should_hexists);
  cluster_test!(hashes, should_hgetall);
  cluster_test!(hashes, should_hincryby);
  cluster_test!(hashes, should_hincryby_float);
  cluster_test!(hashes, should_get_keys);
  cluster_test!(hashes, should_hmset);
  cluster_test!(hashes, should_hmget);
  cluster_test!(hashes, should_hsetnx);
  cluster_test!(hashes, should_get_random_field);
  cluster_test!(hashes, should_get_strlen);
  cluster_test!(hashes, should_get_values);
}

#[cfg(not(feature = "chaos-monkey"))]
mod pubsub {

  cluster_test!(pubsub, should_publish_and_recv_messages);
  cluster_test!(pubsub, should_psubscribe_and_recv_messages);
}

mod hyperloglog {

  cluster_test!(hyperloglog, should_pfadd_elements);
  cluster_test!(hyperloglog, should_pfcount_elements);
  cluster_test!(hyperloglog, should_pfmerge_elements);
}

mod scanning {

  cluster_test!(scanning, should_scan_keyspace);
  cluster_test!(scanning, should_hscan_hash);
  cluster_test!(scanning, should_sscan_set);
  cluster_test!(scanning, should_zscan_sorted_set);
}

mod slowlog {

  cluster_test!(slowlog, should_read_slowlog_length);
  cluster_test!(slowlog, should_read_slowlog_entries);
  cluster_test!(slowlog, should_reset_slowlog);
}

mod server {

  cluster_test!(server, should_flushall);
  cluster_test!(server, should_read_server_info);
  cluster_test!(server, should_ping_server);
  cluster_test!(server, should_run_custom_command);
  cluster_test!(server, should_read_last_save);
  cluster_test!(server, should_read_db_size);
  cluster_test!(server, should_start_bgsave);
  cluster_test!(server, should_do_bgrewriteaof);
}

mod sets {

  cluster_test!(sets, should_sadd_elements);
  cluster_test!(sets, should_scard_elements);
  cluster_test!(sets, should_sdiff_elements);
  cluster_test!(sets, should_sdiffstore_elements);
  cluster_test!(sets, should_sinter_elements);
  cluster_test!(sets, should_sinterstore_elements);
  cluster_test!(sets, should_check_sismember);
  cluster_test!(sets, should_check_smismember);
  cluster_test!(sets, should_read_smembers);
  cluster_test!(sets, should_smove_elements);
  cluster_test!(sets, should_spop_elements);
  cluster_test!(sets, should_get_random_member);
  cluster_test!(sets, should_remove_elements);
  cluster_test!(sets, should_sunion_elements);
  cluster_test!(sets, should_sunionstore_elements);
}

pub mod memory {

  cluster_test!(memory, should_run_memory_doctor);
  cluster_test!(memory, should_run_memory_malloc_stats);
  cluster_test!(memory, should_run_memory_purge);
  cluster_test!(memory, should_run_memory_stats);
  cluster_test!(memory, should_run_memory_usage);
}

pub mod lua {

  cluster_test!(lua, should_load_script);
  cluster_test!(lua, should_load_script_cluster);
  cluster_test!(lua, should_eval_echo_script);
  cluster_test!(lua, should_eval_get_script);
  cluster_test!(lua, should_evalsha_echo_script);
  cluster_test!(lua, should_evalsha_get_script);
}

pub mod sorted_sets {

  #[cfg(not(feature = "chaos-monkey"))]
  cluster_test!(sorted_sets, should_bzpopmin);
  #[cfg(not(feature = "chaos-monkey"))]
  cluster_test!(sorted_sets, should_bzpopmax);
  cluster_test!(sorted_sets, should_zadd_values);
  cluster_test!(sorted_sets, should_zcard_values);
  cluster_test!(sorted_sets, should_zcount_values);
  cluster_test!(sorted_sets, should_zdiff_values);
  cluster_test!(sorted_sets, should_zdiffstore_values);
  cluster_test!(sorted_sets, should_zincrby_values);
  cluster_test!(sorted_sets, should_zinter_values);
  cluster_test!(sorted_sets, should_zinterstore_values);
  cluster_test!(sorted_sets, should_zlexcount);
  cluster_test!(sorted_sets, should_zpopmax);
  cluster_test!(sorted_sets, should_zpopmin);
  cluster_test!(sorted_sets, should_zrandmember);
  cluster_test!(sorted_sets, should_zrangestore_values);
  cluster_test!(sorted_sets, should_zrangebylex);
  cluster_test!(sorted_sets, should_zrevrangebylex);
  cluster_test!(sorted_sets, should_zrangebyscore);
  cluster_test!(sorted_sets, should_zrevrangebyscore);
  cluster_test!(sorted_sets, should_zrank_values);
  cluster_test!(sorted_sets, should_zrem_values);
  cluster_test!(sorted_sets, should_zremrangebylex);
  cluster_test!(sorted_sets, should_zremrangebyrank);
  cluster_test!(sorted_sets, should_zremrangebyscore);
  cluster_test!(sorted_sets, should_zrevrank_values);
  cluster_test!(sorted_sets, should_zscore_values);
  cluster_test!(sorted_sets, should_zunion_values);
  cluster_test!(sorted_sets, should_zunionstore_values);
  cluster_test!(sorted_sets, should_zmscore_values);
}

pub mod lists {

  #[cfg(not(feature = "chaos-monkey"))]
  cluster_test!(lists, should_blpop_values);
  #[cfg(not(feature = "chaos-monkey"))]
  cluster_test!(lists, should_brpop_values);
  #[cfg(not(feature = "chaos-monkey"))]
  cluster_test!(lists, should_brpoplpush_values);
  #[cfg(not(feature = "chaos-monkey"))]
  cluster_test!(lists, should_blmove_values);

  cluster_test!(lists, should_lindex_values);
  cluster_test!(lists, should_linsert_values);
  cluster_test!(lists, should_lpop_values);
  cluster_test!(lists, should_lpos_values);
  cluster_test!(lists, should_lpush_values);
  cluster_test!(lists, should_lpushx_values);
  cluster_test!(lists, should_lrange_values);
  cluster_test!(lists, should_lrem_values);
  cluster_test!(lists, should_lset_values);
  cluster_test!(lists, should_ltrim_values);
  cluster_test!(lists, should_rpop_values);
  cluster_test!(lists, should_rpoplpush_values);
  cluster_test!(lists, should_lmove_values);
  cluster_test!(lists, should_rpush_values);
  cluster_test!(lists, should_rpushx_values);
}

pub mod geo {

  cluster_test!(geo, should_geoadd_values);
  cluster_test!(geo, should_geohash_values);
  cluster_test!(geo, should_geopos_values);
  cluster_test!(geo, should_geodist_values);
  cluster_test!(geo, should_georadius_values);
  cluster_test!(geo, should_georadiusbymember_values);
  cluster_test!(geo, should_geosearch_values);
}
