// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

DEFINE_int32(tera_leveldb_slow_down_level0_score_limit, 100, "control level 0 score compute, score / 2 or sqrt(score / 2)");
DEFINE_int32(tera_leveldb_max_background_compactions, 8, "multi-thread compaction number");
DEFINE_int32(tera_tablet_max_sub_parallel_compaction, 10, "max sub compaction in parallel");
DEFINE_bool(tera_leveldb_ignore_corruption_in_open, false, "ignore fs error when open db");
DEFINE_int32(tera_tablet_del_percentage, 20, "percentage of del tag in sst file begin to trigger compaction");
DEFINE_int32(tera_tablet_ttl_percentage, 99, "percentage of ttl tag in sst file begin to trigger compaction");
DEFINE_int32(tera_tablet_level0_file_limit, 20000, "the max level0 file num before write busy");
DEFINE_int32(tera_asyncwriter_sync_size_threshold, 1024, "force sync per X KB");
DEFINE_int32(tera_asyncwriter_pending_limit, 10000, "the max pending data size (KB) in async writer");
DEFINE_int32(tera_asyncwriter_sync_interval, 10, "the interval (in ms) to sync write buffer to disk");
DEFINE_bool(tera_enable_level0_limit, true, "enable level0 limit");
DEFINE_int32(tera_tabletnode_scanner_cache_size, 5, "default tablet scanner manager cache no more than 100 stream");
DEFINE_uint64(tera_tabletnode_prefetch_scan_size, 1 << 20, "Max size for prefetch scan");
DEFINE_int32(tera_asyncwriter_batch_size, 1024, "write batch to leveldb per X KB");

DEFINE_int32(tera_tablet_max_block_log_number, 50, "max number of unsed log files produced by switching log");
DEFINE_int64(tera_tablet_write_log_time_out, 5, "max time(sec) to wait for log writing or sync");
DEFINE_bool(tera_log_async_mode, true, "enable async mode for log writing and sync");
DEFINE_int64(tera_tablet_log_file_size, 32, "the log file size (in MB) for tablet");
DEFINE_int64(tera_tablet_max_write_buffer_size, 32, "the buffer size (in MB) for tablet write buffer");
DEFINE_int64(tera_tablet_living_period, -1, "the living period of tablet");
DEFINE_int32(tera_tablet_flush_log_num, 100000, "the max log number before flush memtable");
DEFINE_bool(tera_tablet_use_memtable_on_leveldb, false, "enable memtable based on in-memory leveldb");
DEFINE_int64(tera_tablet_memtable_ldb_write_buffer_size, 1000, "the buffer size(in KB) for memtable on leveldb");
DEFINE_bool(tera_sync_log, true, "flush all in-memory parts of log file to stable storage");
DEFINE_bool(tera_io_cache_path_vanish_allowed, false, "if true, allow cache path not exist");

DEFINE_string(tera_dfs_so_path, "", "the dfs implementation path");
DEFINE_string(tera_dfs_conf, "", "the dfs configuration file path");
DEFINE_string(tera_leveldb_env_dfs_type, "hdfs", "the default type for leveldb IO dfs environment, [hdfs | nfs]");
DEFINE_string(tera_leveldb_env_hdfs2_nameservice_list, "default", "the nameservice list of hdfs2");
DEFINE_string(tera_leveldb_env_nfs_mountpoint, "/disk/tera", "the mountpoint of nfs");
DEFINE_string(tera_leveldb_env_nfs_conf_path, "../conf/nfs.conf", "the config file path of nfs");
DEFINE_int32(tera_io_retry_period, 100, "the retry interval period (in ms) when operate file");
DEFINE_int32(tera_io_retry_max_times, 20, "the max retry times when meets trouble");
DEFINE_int32(tera_leveldb_env_local_seek_latency, 50000, "the random access latency (in ns) of local storage device");
DEFINE_int32(tera_leveldb_env_dfs_seek_latency, 10000000, "the random access latency (in ns) of dfs storage device");
DEFINE_int32(tera_memenv_table_cache_size, 100, "the max open file number in leveldb table_cache");
DEFINE_int32(tera_memenv_block_cache_size, 10000, "(MB) block cache size for leveldb which do not use share block cache");
DEFINE_bool(tera_use_flash_for_memenv, true, "Use flashenv for memery lg");
DEFINE_int32(tera_leveldb_block_cache_env_thread_num, 30, "thread num of flash blcok cache");

DEFINE_string(tera_leveldb_compact_strategy, "default", "the default strategy to drive consum compaction, should be [default|LG|dummy]");
DEFINE_bool(tera_leveldb_verify_checksums, true, "enable verify data read from storage against checksums");
DEFINE_bool(tera_leveldb_ignore_corruption_in_compaction, false, "skip corruption blocks of sst file in compaction");
DEFINE_bool(tera_leveldb_use_file_lock, false, "hold file lock during loading leveldb");

DEFINE_bool(tera_leveldb_use_direct_io_read, true, "enable random read from local SATA or SSD device use Direct I/O");
DEFINE_bool(tera_leveldb_use_direct_io_write, true, "enable write to local SATA or SSD device use Direct I/O");
DEFINE_uint64(tera_leveldb_posix_write_buffer_size, 512<<10, "write buffer size for PosixWritableFile");
DEFINE_uint64(tera_leveldb_table_builder_write_batch_size, 256<<10, "table builder's batch write size, 0 means disable table builder batch write");

DEFINE_int32(tera_tablet_unload_count_limit, 3, "the upper bound of try unload, broken this limit will speed up unloading");

/*** Only for DEBUG online ***/
DEFINE_bool(debug_tera_tablet_unload, false, "enable to print tablet unload log more detail");
