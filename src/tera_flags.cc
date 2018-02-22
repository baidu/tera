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

DEFINE_string(tera_coord_type, "", "the coordinator service type for tera cluster [zk,ins,mock_zk,mock_ins,fake_zk]");

DEFINE_bool(tera_zk_enabled, true, "[obsoleted replace by --tera_coord_type=zk] enable zk adapter to coord");
DEFINE_bool(tera_mock_zk_enabled, false, "[obsoleted replace by --tera_coord_type=mock_zk] enable mock zk adapter to coord");
DEFINE_string(tera_zk_addr_list, "localhost:2180", "zookeeper server list");
DEFINE_string(tera_zk_root_path, "/tera", "zookeeper root path");
DEFINE_string(tera_fake_zk_path_prefix, "../fakezk", "fake zk path prefix in onebox tera");
DEFINE_int32(tera_zk_timeout, 10000, "zookeeper session timeout");
DEFINE_int64(tera_zk_retry_period, 3000, "zookeeper operation retry period (in ms)");
DEFINE_int32(tera_zk_retry_max_times, 10, "zookeeper operation max retry times");
DEFINE_string(tera_zk_lib_log_path, "../log/zk.log", "zookeeper library log output file");
DEFINE_string(tera_log_prefix, "", "prefix of log file (INFO, WARNING)");
DEFINE_string(tera_local_addr, "", "local host's ip address");
DEFINE_bool(tera_online_schema_update_enabled, false, "enable online-schema-update");
DEFINE_bool(tera_info_log_clean_enable, true, "enable log cleaner task, enable as default");
DEFINE_int64(tera_info_log_clean_period_second, 2592000, "time period (in second) for log cleaner task, 30 days as default");
DEFINE_int64(tera_info_log_expire_second, 2592000, "expire time (in second) of log file, 30 days as default");
DEFINE_bool(tera_metric_http_server_enable, true, "enable metric http server, enable as default");
DEFINE_int32(tera_metric_http_server_listen_port, 20221, "listen port for metric http server");
DEFINE_int64(tera_hardware_collect_period_second, 5, "hardware metrics checking period (in second)");

/////////  io  /////////

DEFINE_int32(tera_tablet_max_block_log_number, 50, "max number of unsed log files produced by switching log");
DEFINE_int64(tera_tablet_write_log_time_out, 5, "max time(sec) to wait for log writing or sync");
DEFINE_bool(tera_log_async_mode, true, "enable async mode for log writing and sync");
DEFINE_int64(tera_tablet_log_file_size, 32, "the log file size (in MB) for tablet");
DEFINE_int64(tera_tablet_max_write_buffer_size, 32, "the buffer size (in MB) for tablet write buffer");
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
DEFINE_int32(tera_memenv_block_cache_size, 10000, "block cache size for leveldb which do not use share block cache");
DEFINE_bool(tera_use_flash_for_memenv, true, "Use flashenv for memery lg");

DEFINE_string(tera_leveldb_compact_strategy, "default", "the default strategy to drive consum compaction, should be [default|LG|dummy]");
DEFINE_bool(tera_leveldb_verify_checksums, true, "enable verify data read from storage against checksums");
DEFINE_bool(tera_leveldb_ignore_corruption_in_compaction, false, "skip corruption blocks of sst file in compaction");
DEFINE_bool(tera_leveldb_use_file_lock, false, "hold file lock during loading leveldb");

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
DEFINE_int32(tera_master_impl_thread_max_num, 20, "the max thread number for master impl operations");
DEFINE_int32(tera_master_impl_query_thread_num, 20, "the thread number for master impl query tabletnodes");
DEFINE_int32(tera_master_impl_retry_times, 5, "the max retry times when master impl operation fail");

DEFINE_string(tera_master_meta_table_name, "meta_table", "the meta table name");
DEFINE_string(tera_master_meta_table_path, "meta", "the path of meta table");

DEFINE_double(tera_master_workload_merge_threshold, 1.0, "if workload(wwl) < 1.0, enable merge on this tablet");
DEFINE_double(tera_master_workload_split_threshold, 9.9, "if workload(wwl) > 9.9, trigger split by workload");
DEFINE_int64(tera_master_split_tablet_size, 512, "the size (in MB) of tablet to trigger split");
DEFINE_int64(tera_master_min_split_size, 64, "the size (in MB) of tablet to trigger split");
DEFINE_double(tera_master_min_split_ratio, 0.25, "min ratio of split size of tablet schema to trigger split");
DEFINE_int64(tera_master_split_history_time_interval, 600000, "minimal split time interval(ms)");
DEFINE_int64(tera_master_merge_tablet_size, 0, "the size (in MB) of tablet to trigger merge");
DEFINE_string(tera_master_gc_strategy, "trackable", "gc strategy, [default, trackable]");

DEFINE_int32(tera_master_max_split_concurrency, 1, "the max concurrency of tabletnode for split tablet");
DEFINE_int32(tera_master_max_load_concurrency, 5, "the max concurrency of tabletnode for load tablet");
DEFINE_int32(tera_master_max_move_concurrency, 50, "the max concurrency for move tablet");
DEFINE_int32(tera_master_load_interval, 300, "the delay interval (in sec) for load tablet");

DEFINE_int32(tera_master_schema_update_retry_period, 1, "the period (in second) to poll schema update");
DEFINE_int32(tera_master_schema_update_retry_times, 60000, "the max retry times of syncing new schema to ts");

// load balance
DEFINE_bool(tera_master_move_tablet_enabled, true, "enable master to auto move tablet");
DEFINE_bool(tera_master_meta_isolate_enabled, false, "enable master to reserve a tabletnode for meta");
DEFINE_bool(tera_master_load_balance_table_grained, true, "whether the load balance policy only consider the specified table");
DEFINE_double(tera_master_load_balance_size_ratio_trigger, 1.2, "ratio of heaviest node size to lightest to trigger load balance");
DEFINE_int32(tera_master_load_balance_ts_load_threshold, 1000000000, "threshold of one tabletnode in QPS load-balance decision");
DEFINE_int64(tera_master_load_balance_ts_size_threshold, 0, "threshold of one tabletnode in Size load-balance decision");
DEFINE_int32(tera_master_load_balance_scan_weight, 300, "scan weight in load-balance decision");

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
DEFINE_bool(tera_master_gc_trash_enabled, true, "enable master gc trash");
DEFINE_int64(tera_master_gc_trash_expire_time_s, 86400, "time (in second) for gc file keeped in trash");
DEFINE_int64(tera_master_gc_trash_clean_period_s, 3600, "period (in second) for clean gc trash");
DEFINE_int64(tera_master_ins_session_timeout, 10000000, "ins session timeout(us), default 10sec");

DEFINE_bool(tera_master_availability_check_enabled, true, "whether execute availability check");    // reload config safety
DEFINE_bool(tera_master_availability_show_details_enabled, false, "whether show details of not-ready tablets"); // reload config safety
DEFINE_int64(tera_master_not_available_threshold, 0, "the threshold (in s) of not available");     // reload config safety
DEFINE_int64(tera_master_availability_check_period, 60, "the period (in s) of availability check"); // reload config safety
DEFINE_int64(tera_master_availability_warning_threshold, 60, "1 minute, the threshold (in s) of warning availability"); // reload config safety
DEFINE_int64(tera_master_availability_error_threshold, 600, "10 minutes, the threshold (in s) of error availability");        // reload config safety
DEFINE_int64(tera_master_availability_fatal_threshold, 3600, "1 hour, the threshold (in s) of fatal availability");        // reload config safety
DEFINE_bool(tera_master_update_split_meta, true, "[split] update child tablets meta from master");

///////// tablet node  /////////

DEFINE_string(tera_tabletnode_port, "20000", "the tablet node port of tera system");
DEFINE_int32(tera_tabletnode_ctrl_thread_num, 20, "control thread number of tablet node (query/load/unload/split)");
DEFINE_int32(tera_tabletnode_write_thread_num, 10, "write thread number of tablet node");
DEFINE_int32(tera_tabletnode_read_thread_num, 40, "read thread number of tablet node");
DEFINE_int32(tera_tabletnode_scan_thread_num, 30, "scan thread number of tablet node");
DEFINE_int32(tera_tabletnode_manual_compact_thread_num, 2, "the manual compact thread number of tablet node server");
DEFINE_int32(tera_tabletnode_impl_thread_min_num, 1, "the min thread number for tablet node impl operations");
DEFINE_int32(tera_tabletnode_impl_thread_max_num, 10, "the max thread number for tablet node impl operations");
DEFINE_int32(tera_tabletnode_compact_thread_num, 30, "the max thread number for leveldb compaction");

DEFINE_int32(tera_tabletnode_scanner_cache_size, 5, "default tablet scanner manager cache no more than 100 stream");
DEFINE_int32(tera_tabletnode_connect_retry_times, 5, "the max retry times when connect to tablet node");
DEFINE_int32(tera_tabletnode_connect_retry_period, 1000, "the retry period (in ms) between retry two tablet node connection");
DEFINE_int32(tera_tabletnode_connect_timeout_period, 180000, "the timeout period (in ms) for each tablet node connection");
DEFINE_string(tera_tabletnode_path_prefix, "../data/", "the path prefix for table storage");
DEFINE_int32(tera_tabletnode_block_cache_size, 2000, "the cache size of tablet (in MB)");
DEFINE_int32(tera_tabletnode_table_cache_size, 2000, "the table cache size (in MB)");
DEFINE_int32(tera_tabletnode_scan_pack_max_size, 10240, "the max size(KB) of the package for scan rpc");

DEFINE_int32(tera_asyncwriter_pending_limit, 10000, "the max pending data size (KB) in async writer");
DEFINE_bool(tera_enable_level0_limit, true, "enable level0 limit");
DEFINE_int32(tera_tablet_level0_file_limit, 500, "the max level0 file num before write busy");
DEFINE_int32(tera_tablet_ttl_percentage, 99, "percentage of ttl tag in sst file begin to trigger compaction");
DEFINE_int32(tera_tablet_del_percentage, 20, "percentage of del tag in sst file begin to trigger compaction");
DEFINE_int32(tera_asyncwriter_sync_interval, 10, "the interval (in ms) to sync write buffer to disk");
DEFINE_int32(tera_asyncwriter_sync_size_threshold, 1024, "force sync per X KB");
DEFINE_int32(tera_asyncwriter_batch_size, 1024, "write batch to leveldb per X KB");
DEFINE_int32(tera_request_pending_limit, 100000, "the max read/write request pending");
DEFINE_int32(tera_scan_request_pending_limit, 1000, "the max scan request pending");
DEFINE_int32(tera_garbage_collect_period, 1800, "garbage collect period in s");
DEFINE_int32(tera_garbage_collect_debug_log, 0, "garbage collect debug log");
DEFINE_bool(tera_leveldb_ignore_corruption_in_open, false, "ignore fs error when open db");
DEFINE_int32(tera_leveldb_slow_down_level0_score_limit, 100, "control level 0 score compute, score / 2 or sqrt(score / 2)");
DEFINE_int32(tera_leveldb_max_background_compactions, 8, "multi-thread compaction number");
DEFINE_int32(tera_tablet_max_sub_parallel_compaction, 10, "max sub compaction in parallel");

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
DEFINE_int32(tera_tabletnode_cache_update_thread_num, 4, "thread num for update cache");
DEFINE_bool(tera_tabletnode_cache_force_read_from_cache, true, "force update cache before any read");
DEFINE_int32(tera_tabletnode_gc_log_level, 15, "the vlog level [0 - 16] for cache gc.");

DEFINE_bool(tera_tabletnode_tcm_cache_release_enabled, true, "enable the timer to release tcmalloc cache");
DEFINE_int32(tera_tabletnode_tcm_cache_release_period, 180, "the period (in sec) to try release tcmalloc cache");
DEFINE_int64(tera_tabletnode_tcm_cache_size, 838860800, "TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES");
DEFINE_bool(tera_tabletnode_dump_running_info, true, "dump tabletnode running info");
DEFINE_string(tera_tabletnode_running_info_dump_file, "../monitor/ts.info.data", "file path for dump running info");
DEFINE_int64(tera_tabletnode_sysinfo_check_interval, 9223372036854775806, "sysinfo check db health interval in us, default int64_max - 1");

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
DEFINE_int32(tera_sdk_timeout, 60000, "timeout of sdk read/write request (in ms)");
DEFINE_int32(tera_sdk_timeout_precision, 100, "precision of sdk read/write timeout detector (in ms)");
DEFINE_int32(tera_sdk_delay_send_internal, 2, "the sdk resend the request internal time(s)");
DEFINE_int32(tera_sdk_scan_buffer_limit, 2048000, "the pack size limit for scan operation");
DEFINE_bool(tera_sdk_write_sync, false, "sync flag for write");
DEFINE_int32(tera_sdk_batch_size, 250, "batch_size");
DEFINE_int32(tera_sdk_write_send_interval, 10, "write batch send interval time");
DEFINE_int32(tera_sdk_read_send_interval, 5, "read batch send interval time");
DEFINE_int64(tera_sdk_max_mutation_pending_num, INT64_MAX, "default number of pending mutations in async put op");
DEFINE_int64(tera_sdk_max_reader_pending_num, INT64_MAX, "default number of pending readers in async get op");
DEFINE_bool(tera_sdk_async_blocking_enabled, true, "enable blocking when async writing and reading");
DEFINE_int32(tera_sdk_update_meta_concurrency, 3, "the concurrency for updating meta");
DEFINE_int32(tera_sdk_update_meta_buffer_limit, 102400, "the pack size limit for updating meta");
DEFINE_bool(tera_sdk_table_rename_enabled, false, "enable sdk table rename");

DEFINE_bool(tera_sdk_cookie_enabled, true, "enable sdk cookie");
DEFINE_string(tera_sdk_cookie_path, "/tmp/.tera_cookie", "the default path of sdk cookie");
DEFINE_int32(tera_sdk_cookie_update_interval, 600, "the interval of cookie updating(s)");

DEFINE_bool(tera_sdk_perf_counter_enabled, true, "enable performance counter log");
DEFINE_int64(tera_sdk_perf_counter_log_interval, 60, "the interval period (in sec) of performance counter log dumping");
DEFINE_bool(tera_sdk_perf_collect_enabled, false, "enable collect perf counter for metrics");
DEFINE_int32(tera_sdk_perf_collect_interval, 10000, "the interval of collect perf counter(ms)");

DEFINE_bool(tera_sdk_batch_scan_enabled, true, "enable batch scan");
DEFINE_int64(tera_sdk_scan_buffer_size, 65536, "default buffer limit for scan");
DEFINE_int64(tera_sdk_scan_number_limit, 1000000000, "default number limit for scan");
DEFINE_int32(tera_sdk_max_batch_scan_req, 30, "the max number of concurrent scan req");
DEFINE_int64(tera_sdk_scan_timeout, 30000, "scan timeout");
DEFINE_int32(tera_sdk_batch_scan_max_retry, 60, "the max retry times for session scan");
DEFINE_int64(batch_scan_delay_retry_in_us, 1000000, "timewait in us before retry batch scan");
DEFINE_int32(tera_sdk_sync_scan_max_retry, 10, "the max retry times for sync scan");
DEFINE_int64(sync_scan_delay_retry_in_ms, 1000, "timewait in ms before retry sync scan");

DEFINE_string(tera_ins_addr_list, "", "the ins cluster addr. e.g. abc.com:1234,abb.com:1234");
DEFINE_string(tera_ins_root_path, "", "root path on ins. e.g /ps/sandbox");
DEFINE_bool(tera_ins_enabled, false, "[obsoleted replace by --tera_coord_type=ins] option to open ins naming");
DEFINE_bool(tera_mock_ins_enabled, false, "[obsoleted replace by --tera_coord_type=mock_ins] option to open mock ins naming");
DEFINE_int64(tera_ins_session_timeout, 600000000, "ins session timeout(us), default 10min");
DEFINE_int64(tera_sdk_ins_session_timeout, 10000000, "ins session timeout(us), default 10s");

DEFINE_int64(tera_sdk_status_timeout, 600, "(s) check tablet/tabletnode status timeout");
DEFINE_uint64(tera_sdk_read_max_qualifiers, 18446744073709551615U, "read qu limit of each cf, default value is the max of uint64");

/////////  http /////////
DEFINE_string(tera_http_port, "8657", "the http proxy port of tera");
DEFINE_int32(tera_http_request_thread_num, 30, "the http proxy thread num for handle client request");
DEFINE_int32(tera_http_ctrl_thread_num, 10, "the http proxy thread num for it self");

/////////  timeoracle /////////
DEFINE_string(tera_timeoracle_port, "30000", "the timeoracle port of tera");
DEFINE_int32(tera_timeoracle_max_lease_second, 30, "timeoracle work this seconds for a lease");
DEFINE_int32(tera_timeoracle_refresh_lease_second, 10, "timeoracle refresh lease before this seconds");

// only used by timeoracle
DEFINE_bool(tera_timeoracle_mock_enabled, false, "used local filesystem replace zk and ins.");
DEFINE_string(tera_timeoracle_mock_root_path, "/tmp/", "the root path of local filesystem.");
DEFINE_int32(tera_timeoracle_work_thread_num, 16, "timeoracle sofarpc server work_thread_number");
DEFINE_int32(tera_timeoracle_io_service_pool_size, 4, "timeoracle sofarpc server io_service_pool_size");

/////////  global transaction  ////////
DEFINE_bool(tera_sdk_client_for_gtxn, false, "build thread_pool for global transaction");
DEFINE_bool(tera_sdk_tso_client_enabled, false, "get timestamp from timeoracle, default from local timestamp");
DEFINE_int32(tera_gtxn_thread_max_num, 20, "the max thread number for global transaction operations");
DEFINE_int32(tera_gtxn_timeout_ms, 600000, "global transaction timeout limit (ms) default 10 minutes");
DEFINE_int32(tera_gtxn_get_waited_times_limit, 10, "global txn wait other locked times limit");
DEFINE_int32(tera_gtxn_all_puts_size_limit, 10000, "global txn all puts data size limit");

//////// observer ///////
DEFINE_int32(observer_proc_thread_num, 3, "");
DEFINE_int64(observer_max_pending_task, 10000, "");
DEFINE_int32(observer_scanner_thread_num, 20, "");
DEFINE_int32(observer_read_thread_num, 20, "observer read thread num");
DEFINE_int32(observer_ack_conflict_timeout, 3600, "timeout for ack column conflict check");
DEFINE_int32(observer_rowlock_client_thread_num, 20, "");

//////// rowlock server ////////
DEFINE_bool(rowlock_rpc_limit_enabled, false, "enable the rpc traffic limit in sdk");
DEFINE_int32(rowlock_rpc_limit_max_inflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on input flow");
DEFINE_int32(rowlock_rpc_limit_max_outflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on output flow");
DEFINE_int32(rowlock_rpc_max_pending_buffer_size, 200, "max pending buffer size (in MB) for sdk rpc");
DEFINE_int32(rowlock_rpc_work_thread_num, 2, "thread num of sdk rpc client");

DEFINE_string(rowlock_server_ip, "0.0.0.0", "rowlock server ip");
DEFINE_string(rowlock_server_port, "22222", "rowlock server port");
DEFINE_string(rowlock_zk_root_path, "/rowlock", "");
DEFINE_int32(rowlock_zk_timeout, 10000, "zk timeout");
DEFINE_string(rowlock_ins_root_path, "/rowlock", "ins rowlock root path");
DEFINE_int32(rowlock_server_node_num, 1, "number of rowlock servers in cluster");

DEFINE_int32(rowlock_db_ttl, 600000, "timeout for an unlocked lock, 10min");
DEFINE_int32(rowlock_timing_wheel_patch_num, 600, "the number of timing wheel, every patch_num step the oldest data will be cleared");
DEFINE_int32(rowlock_db_sharding_number, 1024, "sharding number, enhance concurrency");
DEFINE_string(rowlock_fake_root_path, "../fakezk/rowlock", "one box fake zk root path");
DEFINE_int32(rowlock_thread_max_num, 20, "the max thread number of rowlock server");
DEFINE_int32(rowlock_client_max_fail_times, 5, "client max failure time");

DEFINE_bool(rowlock_proxy_async_enable, false, "sync | async");
DEFINE_string(rowlock_proxy_port, "22223", "rowlock proxy port");
/////////  load balancer  ////////
DEFINE_string(tera_lb_server_addr, "0.0.0.0", "default load balancer rpc server addr");
DEFINE_string(tera_lb_server_port, "31000", "default load balancer rpc server port");
DEFINE_int32(tera_lb_server_thread_num, 2, "default load balancer rpc server thread pool num");
DEFINE_int32(tera_lb_impl_thread_num, 1, "default load balancer impl thread pool num");
DEFINE_int32(tera_lb_load_balance_period_s, 300, "default load balance period(s)");
DEFINE_int32(tera_lb_max_compute_steps, 1000000, "default max compute steps for one balance procedure");
DEFINE_int32(tera_lb_max_compute_steps_per_tablet, 1000, "default max compute steps per tablet for one balance procedure");
DEFINE_int32(tera_lb_max_compute_time_ms, 30000, "default max compute time(ms) for one balance procedure");
DEFINE_double(tera_lb_min_cost_need_balance, 0.1, "min cost needed for balance");
DEFINE_double(tera_lb_move_count_cost_weight, 10, "move cost weight");
DEFINE_int32(tera_lb_tablet_max_move_num, 10, "default tablet max move num for one balance procedure");
DEFINE_double(tera_lb_tablet_max_move_percent, 0.001, "default tablet max move percent for one balance procedure");
DEFINE_double(tera_lb_move_frequency_cost_weight, 10, "move frequency cost weight");
DEFINE_int32(tera_lb_tablet_move_too_frequently_threshold_s, 600, "if move a tablet in this threshold time(s) again, it's been moved too frequently");
DEFINE_double(tera_lb_abnormal_node_cost_weight, 10, "abnormal node cost weight");
DEFINE_double(tera_lb_abnormal_node_ratio, 0.5, "abnormal node ratio");
DEFINE_double(tera_lb_read_pending_node_cost_weight, 10, "read pending node cost weight");
DEFINE_double(tera_lb_write_pending_node_cost_weight, 10, "write pending node cost weight");
DEFINE_double(tera_lb_scan_pending_node_cost_weight, 10, "scan pending node cost weight");
DEFINE_double(tera_lb_tablet_count_cost_weight, 0, "tablet count cost weight");
DEFINE_double(tera_lb_size_cost_weight, 100, "size cost weight");
DEFINE_double(tera_lb_read_load_cost_weight, 0, "read load cost weight");
DEFINE_double(tera_lb_write_load_cost_weight, 0, "write load cost weight");
DEFINE_double(tera_lb_scan_load_cost_weight, 0, "scan load cost weight");
DEFINE_bool(tera_lb_debug_mode_enabled, false, "debug mode");

DEFINE_int32(rowlock_io_service_pool_size, 4, "rowlock server sofarpc server io_service_pool_size");

DEFINE_bool(mock_rowlock_enable, false, "test case switch");
DEFINE_int64(tera_metric_hold_max_time, 300000, "interval of prometheus collectors push a value to hold_queue in ms");

////////// PROFILER ///////////
DEFINE_bool(cpu_profiler_enabled, false, "enable cpu profiler");
DEFINE_bool(heap_profiler_enabled, false, "enable heap profiler");
DEFINE_int32(cpu_profiler_dump_interval, 120, "cpu profiler dump interval");
DEFINE_int32(heap_profiler_dump_interval, 120, "heap profiler dump interval");
DEFINE_int64(heap_profile_allocation_interval, 1073741824, "Env variable for heap profiler's allocation interval");
DEFINE_int64(heap_profile_inuse_interval, 1073741824, "Env variable for heap profiler's inuse interval");
