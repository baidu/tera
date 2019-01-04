// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

DEFINE_string(tera_tabletnode_port, "20000", "the tablet node port of tera system");
DEFINE_int32(tera_tabletnode_ctrl_thread_num, 20,
             "control thread number of tablet node (load/unload)");
DEFINE_int32(tera_tabletnode_lightweight_ctrl_thread_num, 10,
             "control thread number of tablet node (query/split)");
DEFINE_int32(tera_tabletnode_ctrl_query_thread_num, 10,
             "control query thread num(query/load query/unload query)");
DEFINE_int32(tera_tabletnode_write_thread_num, 10, "write thread number of tablet node");
DEFINE_int32(tera_tabletnode_read_thread_num, 40, "read thread number of tablet node");
DEFINE_int32(tera_tabletnode_scan_thread_num, 30, "scan thread number of tablet node");
DEFINE_int32(tera_tabletnode_manual_compact_thread_num, 2,
             "the manual compact thread number of tablet node server");
DEFINE_int32(tera_tabletnode_impl_thread_max_num, 10,
             "the max thread number for tablet node impl operations");
DEFINE_int32(tera_tabletnode_compact_thread_num, 30,
             "the max thread number for leveldb compaction");

DEFINE_int32(tera_tabletnode_block_cache_size, 2000, "the cache size of tablet (in MB)");
DEFINE_int32(tera_tabletnode_table_cache_size, 2000, "the table cache size (in MB)");

DEFINE_int32(tera_request_pending_limit, 100000, "the max read/write request pending");
DEFINE_int32(tera_scan_request_pending_limit, 1000, "the max scan request pending");
DEFINE_int32(tera_garbage_collect_period, 1800, "garbage collect period in s");

DEFINE_int32(tera_tabletnode_retry_period, 100,
             "the retry interval period (in ms) when operate tablet");

DEFINE_int32(tera_tabletnode_rpc_server_max_inflow, -1,
             "the max input flow (in MB/s) for tabletnode rpc-server, -1 means "
             "no limit");
DEFINE_int32(tera_tabletnode_rpc_server_max_outflow, -1,
             "the max output flow (in MB/s) for tabletnode rpc-server, -1 "
             "means no limit");

DEFINE_bool(tera_tabletnode_cpu_affinity_enabled, false, "enable cpu affinity or not");
DEFINE_string(tera_tabletnode_cpu_affinity_set, "1,2", "the cpu set of cpu affinity setting");
DEFINE_bool(tera_tabletnode_hang_detect_enabled, false, "enable detect read/write hang");
DEFINE_int32(tera_tabletnode_hang_detect_threshold, 60000,
             "read/write hang detect threshold (in ms)");

DEFINE_bool(tera_tabletnode_delete_old_flash_cache_enabled, true, "delete old flash cache");
DEFINE_int64(meta_block_cache_size, 2000, "(MB) mem block cache size for meta leveldb");
DEFINE_int64(meta_table_cache_size, 500, "(MB) mem table cache size for meta leveldb");
DEFINE_int32(tera_tabletnode_gc_log_level, 15, "the vlog level [0 - 16] for cache gc.");

DEFINE_bool(tera_tabletnode_tcm_cache_release_enabled, true,
            "enable the timer to release tcmalloc cache");
DEFINE_int32(tera_tabletnode_tcm_cache_release_period, 180,
             "the period (in sec) to try release tcmalloc cache");
DEFINE_int64(tera_tabletnode_tcm_cache_size, 838860800, "TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES");
DEFINE_bool(tera_tabletnode_dump_running_info, true, "dump tabletnode running info");
DEFINE_string(tera_tabletnode_running_info_dump_file, "../monitor/ts.info.data",
              "file path for dump running info");
DEFINE_int64(tera_refresh_tablets_status_interval_ms, 1800000,
             "background thread refresh tablets status interval in ms, default 0.5h");

DEFINE_bool(tera_tabletnode_dump_level_size_info_enabled, false,
            "enable dump level size or not, it's mainly used for performance-test");

DEFINE_int64(tera_tabletnode_parallel_read_task_num, 10,
             "max tasks can be splited from a read request");
DEFINE_int64(tera_tabletnode_parallel_read_rows_per_task, 30,
             "min row num of a parallel read task");
DEFINE_bool(tera_tabletnode_clean_persistent_cache_paths, false,
            "Clean persistent cache paths when roll back to env flash");

DEFINE_double(tera_quota_unlimited_pending_ratio, 0.1,
              "while pending queue less then ratio*pending_limit, quota limit doesn't need to use");
DEFINE_int32(tera_quota_scan_max_retry_times, 100,
             "quota limit maximum retry times for every scan slot rpc");
DEFINE_int32(tera_quota_scan_retry_delay_interval, 100,
             "quota limit retry task delay time for every scan slot rpc(ms)");
DEFINE_uint64(tera_quota_max_retry_queue_length, 1000,
              "max length of quota retry queue work after check quota failed");