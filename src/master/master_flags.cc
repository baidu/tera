// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"
/////////  master /////////

DEFINE_string(tera_master_port, "10000", "the master port of tera system");
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

DEFINE_double(tera_master_workload_merge_threshold, 1.0, "if workload(wwl) < 1.0, enable merge on this tablet");
DEFINE_double(tera_master_workload_split_threshold, 9.9, "if workload(wwl) > 9.9, trigger split by workload");
DEFINE_int64(tera_master_min_split_size, 64, "the size (in MB) of tablet to trigger split");
DEFINE_double(tera_master_min_split_ratio, 0.5, "min ratio of split size of tablet schema to trigger split");
DEFINE_int64(tera_master_split_history_time_interval, 600000, "minimal split time interval(ms)");
DEFINE_string(tera_master_gc_strategy, "trackable", "gc strategy, [default, trackable]");

DEFINE_int32(tera_master_max_split_concurrency, 1, "the max concurrency of tabletnode for split tablet");
DEFINE_int32(tera_master_max_load_concurrency, 20, "the max concurrency of tabletnode for load tablet");
DEFINE_int32(tera_master_max_move_concurrency, 50, "the max concurrency for move tablet");
DEFINE_int32(tera_master_max_unload_concurrency, 50, "the max concurrency for unload tablet");
DEFINE_int32(tera_master_load_interval, 300, "the delay interval (in sec) for load tablet");

DEFINE_int32(tera_master_schema_update_retry_period, 1, "the period (in second) to poll schema update");
DEFINE_int32(tera_master_schema_update_retry_times, 60000, "the max retry times of syncing new schema to ts");

DEFINE_int32(tera_garbage_collect_debug_log, 0, "garbage collect debug log");

// load balance
DEFINE_bool(tera_master_move_tablet_enabled, false, "enable master to auto move tablet");
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

DEFINE_int32(tera_master_gc_period, 60000, "the period (in ms) for master gc");
DEFINE_bool(tera_master_gc_trash_enabled, true, "enable master gc trash");
DEFINE_int64(tera_master_gc_trash_clean_period_s, 3600, "period (in second) for clean gc trash");

DEFINE_bool(tera_master_availability_check_enabled, true, "whether execute availability check");    // reload config safety
DEFINE_int64(tera_master_availability_check_period, 60, "the period (in s) of availability check"); // reload config safety
DEFINE_bool(tera_master_update_split_meta, true, "[split] update child tablets meta from master");


