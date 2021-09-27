mod keys {

  centralized_test!(keys, should_set_and_get_a_value);
  centralized_test!(keys, should_set_and_del_a_value);
  centralized_test!(keys, should_set_with_get_argument);
  centralized_test!(keys, should_incr_and_decr_a_value);
  centralized_test!(keys, should_incr_by_float);
  centralized_test!(keys, should_mset_a_non_empty_map);
  centralized_test_panic!(keys, should_error_mset_empty_map);
  centralized_test!(keys, should_expire_key);
  centralized_test!(keys, should_persist_key);
  centralized_test!(keys, should_check_ttl);
  centralized_test!(keys, should_check_pttl);
  centralized_test!(keys, should_dump_key);
  centralized_test!(keys, should_dump_and_restore_key);
  centralized_test!(keys, should_modify_ranges);
  centralized_test!(keys, should_getset_value);
  centralized_test!(keys, should_getdel_value);
  centralized_test!(keys, should_get_strlen);
  centralized_test!(keys, should_mget_values);
  centralized_test!(keys, should_msetnx_values);
  centralized_test!(keys, should_copy_values);
}

mod multi {

  centralized_test!(multi, should_run_get_set_trx);
  centralized_test_panic!(multi, should_run_error_get_set_trx);
  centralized_test_panic!(multi, should_fail_with_blocking_cmd);
}

mod other {

  #[cfg(all(not(feature = "chaos-monkey"), feature = "metrics"))]
  centralized_test!(other, should_track_size_stats);

  centralized_test!(other, should_automatically_unblock);
  centralized_test!(other, should_manually_unblock);
  centralized_test!(other, should_error_when_blocked);
}

mod hashes {

  centralized_test!(hashes, should_hset_and_hget);
  centralized_test!(hashes, should_hset_and_hdel);
  centralized_test!(hashes, should_hexists);
  centralized_test!(hashes, should_hgetall);
  centralized_test!(hashes, should_hincryby);
  centralized_test!(hashes, should_hincryby_float);
  centralized_test!(hashes, should_get_keys);
  centralized_test!(hashes, should_hmset);
  centralized_test!(hashes, should_hmget);
  centralized_test!(hashes, should_hsetnx);
  centralized_test!(hashes, should_get_random_field);
  centralized_test!(hashes, should_get_strlen);
  centralized_test!(hashes, should_get_values);
}

#[cfg(not(feature = "chaos-monkey"))]
mod pubsub {

  centralized_test!(pubsub, should_publish_and_recv_messages);
  centralized_test!(pubsub, should_psubscribe_and_recv_messages);
}

mod hyperloglog {

  centralized_test!(hyperloglog, should_pfadd_elements);
  centralized_test!(hyperloglog, should_pfcount_elements);
  centralized_test!(hyperloglog, should_pfmerge_elements);
}

mod scanning {

  centralized_test!(scanning, should_scan_keyspace);
  centralized_test!(scanning, should_hscan_hash);
  centralized_test!(scanning, should_sscan_set);
  centralized_test!(scanning, should_zscan_sorted_set);
}

mod slowlog {

  centralized_test!(slowlog, should_read_slowlog_length);
  centralized_test!(slowlog, should_read_slowlog_entries);
  centralized_test!(slowlog, should_reset_slowlog);
}

mod server {

  centralized_test!(server, should_flushall);
  centralized_test!(server, should_read_server_info);
  centralized_test!(server, should_ping_server);
  centralized_test!(server, should_run_custom_command);
  centralized_test!(server, should_read_last_save);
  centralized_test!(server, should_read_db_size);
  centralized_test!(server, should_start_bgsave);
  centralized_test!(server, should_do_bgrewriteaof);
}

mod sets {

  centralized_test!(sets, should_sadd_elements);
  centralized_test!(sets, should_scard_elements);
  centralized_test!(sets, should_sdiff_elements);
  centralized_test!(sets, should_sdiffstore_elements);
  centralized_test!(sets, should_sinter_elements);
  centralized_test!(sets, should_sinterstore_elements);
  centralized_test!(sets, should_check_sismember);
  centralized_test!(sets, should_check_smismember);
  centralized_test!(sets, should_read_smembers);
  centralized_test!(sets, should_smove_elements);
  centralized_test!(sets, should_spop_elements);
  centralized_test!(sets, should_get_random_member);
  centralized_test!(sets, should_remove_elements);
  centralized_test!(sets, should_sunion_elements);
  centralized_test!(sets, should_sunionstore_elements);
}

pub mod memory {

  centralized_test!(memory, should_run_memory_doctor);
  centralized_test!(memory, should_run_memory_malloc_stats);
  centralized_test!(memory, should_run_memory_purge);
  centralized_test!(memory, should_run_memory_stats);
  centralized_test!(memory, should_run_memory_usage);
}

pub mod lua {

  centralized_test!(lua, should_load_script);
  centralized_test!(lua, should_eval_echo_script);
  centralized_test!(lua, should_eval_get_script);
  centralized_test!(lua, should_evalsha_echo_script);
  centralized_test!(lua, should_evalsha_get_script);
}

pub mod sorted_sets {

  centralized_test!(sorted_sets, should_bzpopmin);
  centralized_test!(sorted_sets, should_bzpopmax);
  centralized_test!(sorted_sets, should_zadd_values);
  centralized_test!(sorted_sets, should_zcard_values);
  centralized_test!(sorted_sets, should_zcount_values);
  centralized_test!(sorted_sets, should_zdiff_values);
  centralized_test!(sorted_sets, should_zdiffstore_values);
  centralized_test!(sorted_sets, should_zincrby_values);
  centralized_test!(sorted_sets, should_zinter_values);
  centralized_test!(sorted_sets, should_zinterstore_values);
  centralized_test!(sorted_sets, should_zlexcount);
  centralized_test!(sorted_sets, should_zpopmax);
  centralized_test!(sorted_sets, should_zpopmin);
  centralized_test!(sorted_sets, should_zrandmember);
  centralized_test!(sorted_sets, should_zrangestore_values);
  centralized_test!(sorted_sets, should_zrangebylex);
  centralized_test!(sorted_sets, should_zrevrangebylex);
  centralized_test!(sorted_sets, should_zrangebyscore);
  centralized_test!(sorted_sets, should_zrevrangebyscore);
  centralized_test!(sorted_sets, should_zrank_values);
  centralized_test!(sorted_sets, should_zrem_values);
  centralized_test!(sorted_sets, should_zremrangebylex);
  centralized_test!(sorted_sets, should_zremrangebyrank);
  centralized_test!(sorted_sets, should_zremrangebyscore);
  centralized_test!(sorted_sets, should_zrevrank_values);
  centralized_test!(sorted_sets, should_zscore_values);
  centralized_test!(sorted_sets, should_zunion_values);
  centralized_test!(sorted_sets, should_zunionstore_values);
  centralized_test!(sorted_sets, should_zmscore_values);
}

pub mod lists {

  #[cfg(not(feature = "chaos-monkey"))]
  centralized_test!(lists, should_blpop_values);
  #[cfg(not(feature = "chaos-monkey"))]
  centralized_test!(lists, should_brpop_values);
  #[cfg(not(feature = "chaos-monkey"))]
  centralized_test!(lists, should_brpoplpush_values);
  #[cfg(not(feature = "chaos-monkey"))]
  centralized_test!(lists, should_blmove_values);

  centralized_test!(lists, should_lindex_values);
  centralized_test!(lists, should_linsert_values);
  centralized_test!(lists, should_lpop_values);
  centralized_test!(lists, should_lpos_values);
  centralized_test!(lists, should_lpush_values);
  centralized_test!(lists, should_lpushx_values);
  centralized_test!(lists, should_lrange_values);
  centralized_test!(lists, should_lrem_values);
  centralized_test!(lists, should_lset_values);
  centralized_test!(lists, should_ltrim_values);
  centralized_test!(lists, should_rpop_values);
  centralized_test!(lists, should_rpoplpush_values);
  centralized_test!(lists, should_lmove_values);
  centralized_test!(lists, should_rpush_values);
  centralized_test!(lists, should_rpushx_values);
}

pub mod geo {

  centralized_test!(geo, should_geoadd_values);
  centralized_test!(geo, should_geohash_values);
  centralized_test!(geo, should_geopos_values);
  centralized_test!(geo, should_geodist_values);
  centralized_test!(geo, should_georadius_values);
  centralized_test!(geo, should_georadiusbymember_values);
  centralized_test!(geo, should_geosearch_values);
}
