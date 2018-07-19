// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

DEFINE_string(tera_tabletnode_port, "20000", "the tablet node port of tera system");
DEFINE_int32(tera_tabletnode_ctrl_thread_num, 20, "control thread number of tablet node (query/load/unload/split)");
DEFINE_int32(tera_tabletnode_write_thread_num, 10, "write thread number of tablet node");
DEFINE_int32(tera_tabletnode_read_thread_num, 40, "read thread number of tablet node");
DEFINE_int32(tera_tabletnode_scan_thread_num, 30, "scan thread number of tablet node");
DEFINE_int32(tera_tabletnode_manual_compact_thread_num, 2, "the manual compact thread number of tablet node server");
DEFINE_int32(tera_tabletnode_impl_thread_max_num, 10, "the max thread number for tablet node impl operations");
DEFINE_int32(tera_tabletnode_compact_thread_num, 30, "the max thread number for leveldb compaction");

DEFINE_int32(tera_tabletnode_block_cache_size, 2000, "the cache size of tablet (in MB)");
DEFINE_int32(tera_tabletnode_table_cache_size, 2000, "the table cache size (in MB)");

DEFINE_int32(tera_request_pending_limit, 100000, "the max read/write request pending");
DEFINE_int32(tera_scan_request_pending_limit, 1000, "the max scan request pending");
DEFINE_int32(tera_garbage_collect_period, 1800, "garbage collect period in s");

DEFINE_int32(tera_tabletnode_retry_period, 100, "the retry interval period (in ms) when operate tablet");

DEFINE_int32(tera_tabletnode_rpc_server_max_inflow, -1, "the max input flow (in MB/s) for tabletnode rpc-server, -1 means no limit");
DEFINE_int32(tera_tabletnode_rpc_server_max_outflow, -1, "the max output flow (in MB/s) for tabletnode rpc-server, -1 means no limit");

DEFINE_bool(tera_tabletnode_cpu_affinity_enabled, false, "enable cpu affinity or not");
DEFINE_string(tera_tabletnode_cpu_affinity_set, "1,2", "the cpu set of cpu affinity setting");
DEFINE_bool(tera_tabletnode_hang_detect_enabled, false, "enable detect read/write hang");
DEFINE_int32(tera_tabletnode_hang_detect_threshold, 60000, "read/write hang detect threshold (in ms)");

DEFINE_bool(tera_tabletnode_delete_old_flash_cache_enabled, true, "delete old flash cache");
DEFINE_bool(flash_block_cache_force_update_conf_enabled, false, "force update conf from FLAG file");
DEFINE_int64(flash_block_cache_size, 350UL << 30, "max capacity size can be use for each ssd, default 350GB");
DEFINE_int64(flash_block_cache_blockset_size, 1UL << 30, "block set size, default 1GB");
DEFINE_int64(flash_block_cache_block_size, 8192, "block size in each block set, default 8KB");
DEFINE_int64(flash_block_cache_fid_batch_num, 100000, "fid batch write number");
DEFINE_int64(meta_block_cache_size, 2000, "(MB) mem block cache size for meta leveldb");
DEFINE_int64(meta_table_cache_size, 500, "(MB) mem table cache size for meta leveldb");
DEFINE_int64(flash_block_cache_write_buffer_size, 1048576, "(B) write buffer size for meta leveldb");
DEFINE_string(tera_tabletnode_cache_paths, "../data/cache/", "paths for cached data storage. Mutiple definition like: \"./path1/;./path2/\"");
DEFINE_int32(tera_tabletnode_cache_update_thread_num, 4, "thread num for update cache");
DEFINE_bool(tera_tabletnode_cache_force_read_from_cache, true, "force update cache before any read");
DEFINE_int32(tera_tabletnode_gc_log_level, 15, "the vlog level [0 - 16] for cache gc.");

DEFINE_bool(tera_tabletnode_tcm_cache_release_enabled, true, "enable the timer to release tcmalloc cache");
DEFINE_int32(tera_tabletnode_tcm_cache_release_period, 180, "the period (in sec) to try release tcmalloc cache");
DEFINE_int64(tera_tabletnode_tcm_cache_size, 838860800, "TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES");
DEFINE_bool(tera_tabletnode_dump_running_info, true, "dump tabletnode running info");
DEFINE_string(tera_tabletnode_running_info_dump_file, "../monitor/ts.info.data", "file path for dump running info");
DEFINE_int64(tera_refresh_tablets_status_interval_ms, 1800000, "background thread refresh tablets status interval in ms, default 0.5h");

DEFINE_bool(tera_tabletnode_dump_level_size_info_enabled, false, "enable dump level size or not, it's mainly used for performance-test");
