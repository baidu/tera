// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

/////////  common /////////

DEFINE_string(tera_role, "", "the role of tera running binary, should be one of (master | tabletnode)");

DEFINE_string(tera_user_identity, "", "the identity of tera user");
DEFINE_string(tera_user_passcode, "", "the passcode of tera user");
DEFINE_bool(tera_acl_enabled, false, "enable access control");
DEFINE_bool(tera_only_root_create_table, false, "only the root user can create table");

DEFINE_int64(tera_heartbeat_retry_period_factor, 1, "the heartbeat period factor when retry send heartbeat");
DEFINE_int32(tera_heartbeat_retry_times, 5, "the max retry times when fail to send report request");

DEFINE_string(tera_working_dir, "./", "the base dir for system data");

DEFINE_bool(tera_zk_enabled, true, "enable zk adapter to collaborate with other master instances");
DEFINE_string(tera_zk_addr_list, "localhost:2180", "zookeeper server list");
DEFINE_string(tera_zk_root_path, "/tera", "zookeeper root path");
DEFINE_string(tera_fake_zk_path_prefix, "../fakezk", "fake zk path prefix in onebox tera");
DEFINE_int32(tera_zk_timeout, 10000, "zookeeper session timeout");
DEFINE_int64(tera_zk_retry_period, 3000, "zookeeper operation retry period (in ms)");
DEFINE_int32(tera_zk_retry_max_times, 10, "zookeeper operation max retry times");
DEFINE_string(tera_zk_lib_log_path, "../log/zk.log", "zookeeper library log output file");
DEFINE_string(tera_log_prefix, "", "prefix of log file (INFO, WARNING)");
DEFINE_string(tera_local_addr, "", "local host's ip address");

/////////  io  /////////

DEFINE_int32(tera_tablet_max_block_log_number, 50, "max number of unsed log files produced by switching log");
DEFINE_int64(tera_tablet_write_log_time_out, 5, "max time(sec) to wait for log writing or sync");
DEFINE_bool(tera_log_async_mode, true, "enable async mode for log writing and sync");
DEFINE_int64(tera_tablet_log_file_size, 32, "the log file size (in MB) for tablet");
DEFINE_int64(tera_tablet_write_buffer_size, 32, "the buffer size (in MB) for tablet write buffer");
DEFINE_int64(tera_tablet_write_block_size, 4, "the block size (in KB) for teblet write block");
DEFINE_int64(tera_tablet_living_period, -1, "the living period of tablet");
DEFINE_int32(tera_tablet_flush_log_num, 100000, "the max log number before flush memtable");
DEFINE_bool(tera_tablet_use_memtable_on_leveldb, false, "enable memtable based on in-memory leveldb");
DEFINE_int64(tera_tablet_memtable_ldb_write_buffer_size, 1000, "the buffer size(in KB) for memtable on leveldb");
DEFINE_int64(tera_tablet_memtable_ldb_block_size, 4, "the block size (in KB) for memtable on leveldb");
DEFINE_int64(tera_tablet_ldb_sst_size, 8, "the sstable file size (in MB) on leveldb");
DEFINE_bool(tera_sync_log, true, "flush all in-memory parts of log file to stable storage");
DEFINE_bool(tera_io_cache_path_vanish_allowed, false, "if true, allow cache path not exist");

DEFINE_string(tera_dfs_so_path, "", "the dfs implementation path");
DEFINE_string(tera_dfs_conf, "", "the dfs configuration file path");
DEFINE_string(tera_leveldb_env_type, "dfs", "the default type for leveldb IO environment, should be [local | dfs]");
DEFINE_string(tera_leveldb_env_dfs_type, "hdfs", "the default type for leveldb IO dfs environment, [hdfs | nfs]");
DEFINE_string(tera_leveldb_env_hdfs2_nameservice_list, "default", "the nameservice list of hdfs2");
DEFINE_string(tera_leveldb_env_nfs_mountpoint, "/disk/tera", "the mountpoint of nfs");
DEFINE_string(tera_leveldb_env_nfs_conf_path, "../conf/nfs.conf", "the config file path of nfs");
DEFINE_string(tera_leveldb_log_path, "../log/leveldb.log", "the default path for leveldb logger");
DEFINE_int32(tera_io_retry_period, 100, "the retry interval period (in ms) when operate file");
DEFINE_int32(tera_io_retry_max_times, 20, "the max retry times when meets trouble");
DEFINE_int32(tera_leveldb_env_local_seek_latency, 50000, "the random access latency (in ns) of local storage device");
DEFINE_int32(tera_leveldb_env_dfs_seek_latency, 10000000, "the random access latency (in ns) of dfs storage device");
DEFINE_int32(tera_memenv_table_cache_size, 100, "the max open file number in leveldb table_cache");
DEFINE_int32(tera_memenv_block_cache_size, 20, "block cache size for leveldb which do not use share block cache");

DEFINE_string(tera_leveldb_compact_strategy, "default", "the default strategy to drive consum compaction, should be [default|LG|dummy]");
DEFINE_bool(tera_leveldb_verify_checksums, true, "enable verify data read from storage against checksums");
DEFINE_bool(tera_leveldb_ignore_corruption_in_compaction, true, "skip corruption blocks of sst file in compaction");

DEFINE_int64(tera_io_scan_stream_task_max_num, 5000, "the max number of concurrent rpc task");
DEFINE_int64(tera_io_scan_stream_task_pending_time, 180, "the max pending time (in sec) for timeout and interator cleaning");

DEFINE_int32(tera_rpc_client_max_inflow, -1, "the max input flow (in MB/s) for rpc-client, -1 means no limit");
DEFINE_int32(tera_rpc_client_max_outflow, -1, "the max input flow (in MB/s) for rpc-client, -1 means no limit");
DEFINE_int32(tera_rpc_timeout_period, 60000, "the timeout period (in ms) for rpc");

/////////  master /////////

DEFINE_string(tera_master_port, "10000", "the master port of tera system");
DEFINE_int64(tera_heartbeat_timeout_period_factor, 120, "the timeout period factor when lose heartbeat");
DEFINE_int32(tera_master_connect_retry_times, 5, "the max retry times when connect to master");
DEFINE_int32(tera_master_connect_retry_period, 1000, "the retry period (in ms) between two master connection");
DEFINE_int32(tera_master_connect_timeout_period, 5000, "the timeout period (in ms) for each master connection");
DEFINE_int32(tera_master_query_tabletnode_period, 10000, "the period (in ms) for query tabletnode status" );
DEFINE_int32(tera_master_common_retry_period, 1000, "the period (in ms) for common operation" );
DEFINE_int32(tera_master_meta_retry_times, 5, "the max retry times when master read/write meta");
DEFINE_bool(tera_master_meta_recovery_enabled, false, "whether recovery meta tablet at startup");
DEFINE_string(tera_master_meta_recovery_file, "../data/meta.bak", "path of meta table recovery file");

DEFINE_bool(tera_master_cache_check_enabled, true, "enable the periodic check & release cache");
DEFINE_int32(tera_master_cache_release_period, 180, "the period (in sec) to try release cache");
DEFINE_int32(tera_master_cache_keep_min, 512, "the minimal free cache size (in MB) to keep");

DEFINE_int32(tera_master_thread_min_num, 1, "the min thread number of master server");
DEFINE_int32(tera_master_thread_max_num, 10, "the max thread number of master server");
DEFINE_int32(tera_master_impl_thread_min_num, 1, "the min thread number for master impl operations");
DEFINE_int32(tera_master_impl_thread_max_num, 20, "the max thread number for master impl operations");
DEFINE_int32(tera_master_impl_retry_times, 5, "the max retry times when master impl operation fail");

DEFINE_string(tera_master_meta_table_name, "meta_table", "the meta table name");
DEFINE_string(tera_master_meta_table_path, "meta", "the path of meta table");

DEFINE_int64(tera_master_split_tablet_size, 512, "the size (in MB) of tablet to trigger split");
DEFINE_bool(tera_master_merge_enabled, false, "enable the auto-merge tablet");
DEFINE_int64(tera_master_merge_tablet_size, 0, "the size (in MB) of tablet to trigger merge");
DEFINE_string(tera_master_gc_strategy, "default", "gc strategy, [default, incremental]");

DEFINE_int32(tera_master_max_split_concurrency, 1, "the max concurrency of tabletnode for split tablet");
DEFINE_int32(tera_master_max_load_concurrency, 5, "the max concurrency of tabletnode for load tablet");
DEFINE_int32(tera_master_load_interval, 300, "the delay interval (in sec) for load tablet");

// load balance
DEFINE_bool(tera_master_move_tablet_enabled, true, "enable master to auto move tablet");
DEFINE_bool(tera_master_meta_isolate_enabled, false, "enable master to reserve a tabletnode for meta");
DEFINE_int32(tera_master_load_balance_period, 10000, "the period (in ms) for load balance policy execute");
DEFINE_bool(tera_master_load_balance_table_grained, true, "whether the load balance policy only consider the specified table");
DEFINE_double(tera_master_load_balance_size_ratio_trigger, 1.2, "ratio of heaviest node size to lightest to trigger load balance");
DEFINE_bool(tera_master_load_balance_qps_policy_enabled, false, "enable QPS load balance");
DEFINE_int32(tera_master_load_balance_accumulate_query_times, 10, "summarize how many queries to make QPS load-balance decision");

DEFINE_double(tera_safemode_tablet_locality_ratio, 0.9, "the tablet locality ratio threshold of safemode");
DEFINE_bool(tera_master_kick_tabletnode_enabled, true, "enable master to kick tabletnode");
DEFINE_int32(tera_master_kick_tabletnode_query_fail_times, 10, "the number of query fail to kick tabletnode");
DEFINE_int32(tera_master_control_tabletnode_retry_period, 60000, "the retry period (in ms) for master control tabletnode");
DEFINE_int32(tera_master_load_rpc_timeout, 60000, "the timeout period (in ms) for load rpc");
DEFINE_int32(tera_master_unload_rpc_timeout, 60000, "the timeout period (in ms) for unload rpc");
DEFINE_int32(tera_master_split_rpc_timeout, 120000, "the timeout period (in ms) for split rpc");
DEFINE_int32(tera_master_tabletnode_timeout, 60000, "the timeout period (in ms) for move tablet after tabletnode down");
DEFINE_int32(tera_master_collect_info_timeout, 3000, "the timeout period (in ms) for collect tabletnode info");
DEFINE_int32(tera_master_collect_info_retry_period, 3000, "the retry period (in ms) for collect tabletnode info");
DEFINE_int32(tera_master_collect_info_retry_times, 10, "the max retry times for collect tabletnode info");
DEFINE_int32(tera_master_load_slow_retry_times, 60, "the max retry times when master load very slow tablet");

DEFINE_int32(tera_master_rpc_server_max_inflow, -1, "the max input flow (in MB/s) for master rpc-server, -1 means no limit");
DEFINE_int32(tera_master_rpc_server_max_outflow, -1, "the max input flow (in MB/s) for master rpc-server, -1 means no limit");

DEFINE_int32(tera_max_pre_assign_tablet_num, 100000, "max num of pre-assign tablets per table");
DEFINE_bool(tera_delete_obsolete_tabledir_enabled, true, "move table dir to trash when dropping table");

DEFINE_bool(tera_master_stat_table_enabled, true, "whether dump system status to stat_table");
DEFINE_string(tera_master_stat_table_name, "stat_table", "a specific table for system status dumping");
DEFINE_int64(tera_master_stat_table_ttl, 8000000, "default ttl for stat table (s / 100d).");
DEFINE_int64(tera_master_stat_table_interval, 60, "interval of system status dumping (s)");
DEFINE_int64(tera_master_stat_table_splitsize, 100, "default split size of stat table");

DEFINE_int32(tera_master_gc_period, 60000, "the period (in ms) for master gc");

///////// tablet node  /////////

DEFINE_string(tera_tabletnode_port, "20000", "the tablet node port of tera system");
DEFINE_int32(tera_tabletnode_ctrl_thread_num, 10, "control thread number of tablet node (query/load/unload/split)");
DEFINE_int32(tera_tabletnode_write_thread_num, 10, "write thread number of tablet node");
DEFINE_int32(tera_tabletnode_read_thread_num, 40, "read thread number of tablet node");
DEFINE_int32(tera_tabletnode_scan_thread_num, 5, "scan thread number of tablet node");
DEFINE_int32(tera_tabletnode_manual_compact_thread_num, 2, "the manual compact thread number of tablet node server");
DEFINE_int32(tera_tabletnode_impl_thread_min_num, 1, "the min thread number for tablet node impl operations");
DEFINE_int32(tera_tabletnode_impl_thread_max_num, 10, "the max thread number for tablet node impl operations");
DEFINE_int32(tera_tabletnode_compact_thread_num, 10, "the max thread number for leveldb compaction");

DEFINE_int32(tera_tabletnode_connect_retry_times, 5, "the max retry times when connect to tablet node");
DEFINE_int32(tera_tabletnode_connect_retry_period, 1000, "the retry period (in ms) between retry two tablet node connection");
DEFINE_int32(tera_tabletnode_connect_timeout_period, 180000, "the timeout period (in ms) for each tablet node connection");
DEFINE_string(tera_tabletnode_path_prefix, "../data/", "the path prefix for table storage");
DEFINE_int32(tera_tabletnode_block_cache_size, 2000, "the cache size of tablet (in MB)");
DEFINE_int32(tera_tabletnode_table_cache_size, 1000, "the table cache size, means the max num of files keeping open in this tabletnode.");
DEFINE_int32(tera_tabletnode_scan_pack_max_size, 10240, "the max size(KB) of the package for scan rpc");

DEFINE_int32(tera_asyncwriter_pending_limit, 10000, "the max pending data size (KB) in async writer");
DEFINE_bool(tera_enable_level0_limit, true, "enable level0 limit");
DEFINE_int32(tera_tablet_level0_file_limit, 20, "the max level0 file num before write busy");
DEFINE_int32(tera_asyncwriter_sync_interval, 100, "the interval (in ms) to sync write buffer to disk");
DEFINE_int32(tera_asyncwriter_sync_size_threshold, 1024, "force sync per X KB");
DEFINE_int32(tera_asyncwriter_batch_size, 1024, "write batch to leveldb per X KB");
DEFINE_int32(tera_request_pending_limit, 100000, "the max read/write request pending");
DEFINE_int32(tera_scan_request_pending_limit, 1000, "the max scan request pending");
DEFINE_int32(tera_garbage_collect_period, 1800, "garbage collect period in s");

DEFINE_int32(tera_tabletnode_write_meta_rpc_timeout, 60000, "the timeout period (in ms) for tabletnode write meta");
DEFINE_int32(tera_tabletnode_retry_period, 100, "the retry interval period (in ms) when operate tablet");

DEFINE_int32(tera_tabletnode_rpc_server_max_inflow, -1, "the max input flow (in MB/s) for tabletnode rpc-server, -1 means no limit");
DEFINE_int32(tera_tabletnode_rpc_server_max_outflow, -1, "the max output flow (in MB/s) for tabletnode rpc-server, -1 means no limit");

DEFINE_bool(tera_tabletnode_cpu_affinity_enabled, false, "enable cpu affinity or not");
DEFINE_string(tera_tabletnode_cpu_affinity_set, "1,2", "the cpu set of cpu affinity setting");
DEFINE_bool(tera_tabletnode_hang_detect_enabled, false, "enable detect read/write hang");
DEFINE_int32(tera_tabletnode_hang_detect_threshold, 60000, "read/write hang detect threshold (in ms)");

DEFINE_bool(tera_tabletnode_cache_enabled, false, "enable three-level cache mechasism");
DEFINE_string(tera_tabletnode_cache_paths, "../data/cache/", "paths for cached data storage. Mutiple definition like: \"./path1/;./path2/\"");
DEFINE_int32(tera_tabletnode_cache_block_size, 8192, "the block size of cache system");
DEFINE_string(tera_tabletnode_cache_name, "tera.cache", "prefix name for cache name");
DEFINE_int32(tera_tabletnode_cache_mem_size, 2048, "the maximal size (in KB) of mem cache");
DEFINE_int32(tera_tabletnode_cache_disk_size, 1024, "the maximal size (in MB) of disk cache");
DEFINE_int32(tera_tabletnode_cache_disk_filenum, 1, "the file num of disk cache storage");
DEFINE_int32(tera_tabletnode_cache_log_level, 1, "the log level [0 - 5] for cache system (0: FATAL, 1: ERROR, 2: WARN, 3: INFO, 5: DEBUG).");
DEFINE_int32(tera_tabletnode_gc_log_level, 15, "the vlog level [0 - 16] for cache gc.");

DEFINE_bool(tera_tabletnode_tcm_cache_release_enabled, true, "enable the timer to release tcmalloc cache");
DEFINE_int32(tera_tabletnode_tcm_cache_release_period, 180, "the period (in sec) to try release tcmalloc cache");
DEFINE_int64(tera_tabletnode_tcm_cache_size, 838860800, "TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES");

///////// SDK  /////////
DEFINE_string(tera_sdk_impl_type, "tera", "the activated type of SDK impl");
DEFINE_int32(tera_sdk_retry_times, 10, "the max retry times during sdk operation fail");
DEFINE_int32(tera_sdk_retry_period, 500, "the retry period (in ms) between two operations");
DEFINE_string(tera_sdk_conf_file, "", "the path of default flag file");
DEFINE_int32(tera_sdk_show_max_num, 20000, "the max fetch meta number for each rpc connection");
DEFINE_int32(tera_sdk_async_pending_limit, 2000, "the max number for pending task in async writer");
DEFINE_int32(tera_sdk_async_sync_task_threshold, 1000, "the sync task threshold to do sync operation");
DEFINE_int32(tera_sdk_async_sync_record_threshold, 1000, "the sync kv record threshold");
DEFINE_int32(tera_sdk_async_sync_interval, 15, "the interval (in ms) to sync write buffer to disk");
DEFINE_int32(tera_sdk_async_thread_min_num, 1, "the min thread number for tablet node impl operations");
DEFINE_int32(tera_sdk_async_thread_max_num, 200, "the max thread number for tablet node impl operations");
DEFINE_int32(tera_sdk_rpc_request_max_size, 30, "the max size(MB) for the request message of RPC");
DEFINE_string(tera_sdk_root_table_addr,"127.0.0.1:22000","the default table server has root_table");
DEFINE_int32(tera_sdk_thread_min_num, 1, "the min thread number for tablet node impl operations");
DEFINE_int32(tera_sdk_thread_max_num, 20, "the max thread number for tablet node impl operations");
DEFINE_bool(tera_sdk_rpc_limit_enabled, false, "enable the rpc traffic limit in sdk");
DEFINE_int32(tera_sdk_rpc_limit_max_inflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on input flow");
DEFINE_int32(tera_sdk_rpc_limit_max_outflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on output flow");
DEFINE_int32(tera_sdk_rpc_max_pending_buffer_size, 200, "max pending buffer size (in MB) for sdk rpc");
DEFINE_int32(tera_sdk_rpc_work_thread_num, 8, "thread num of sdk rpc client");
DEFINE_int32(tera_sdk_update_meta_internal, 10000, "the sdk update meta table internal time(ms)");
DEFINE_int32(tera_sdk_check_timer_internal, 100, "the sdk check the resend quest queue internal time");
DEFINE_int32(tera_sdk_timeout, 60000, "timeout of wait in sync reader&mutation mode");
DEFINE_int32(tera_sdk_delay_send_internal, 2, "the sdk resend the request internal time(s)");
DEFINE_int32(tera_sdk_scan_buffer_limit, 2048000, "the pack size limit for scan operation");
DEFINE_bool(tera_sdk_write_sync, false, "sync flag for write");
DEFINE_int32(tera_sdk_batch_size, 100, "batch_size");
DEFINE_int32(tera_sdk_write_send_interval, 100, "write batch send interval time");
DEFINE_int32(tera_sdk_read_send_interval, 10, "read batch send interval time");
DEFINE_int64(tera_sdk_max_mutation_pending_num, INT64_MAX, "default number of pending mutations in async put op");
DEFINE_int64(tera_sdk_max_reader_pending_num, INT64_MAX, "default number of pending readers in async get op");
DEFINE_bool(tera_sdk_async_blocking_enabled, true, "enable blocking when async writing and reading");
DEFINE_int32(tera_sdk_update_meta_concurrency, 3, "the concurrency for updating meta");
DEFINE_int32(tera_sdk_update_meta_buffer_limit, 102400, "the pack size limit for updating meta");

DEFINE_bool(tera_sdk_cookie_enabled, true, "enable sdk cookie");
DEFINE_string(tera_sdk_cookie_path, "/tmp/.tera_cookie", "the default path of sdk cookie");
DEFINE_int32(tera_sdk_cookie_update_interval, 600, "the interval of cookie updating(s)");

DEFINE_bool(tera_sdk_perf_counter_enabled, true, "enable performance counter log");
DEFINE_int64(tera_sdk_perf_counter_log_interval, 1, "the interval of performance counter log dumping");

DEFINE_bool(tera_sdk_scan_async_enabled, false, "enable async scan");
DEFINE_int64(tera_sdk_scan_async_cache_size, 16, "the max buffer size (in MB) for cached scan results");
DEFINE_int32(tera_sdk_scan_async_parallel_max_num, 500, "the max number of concurrent task sending");

DEFINE_string(tera_ins_addr_list, "", "the ins cluster addr. e.g. abc.com:1234,abb.com:1234");
DEFINE_string(tera_ins_root_path, "", "root path on ins. e.g /ps/sandbox");
DEFINE_bool(tera_ins_enabled, false, "option to open ins naming");
