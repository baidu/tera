// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4 << 20),
      l0_slowdown_writes_trigger(10),
      max_open_files(1000),
      table_cache(NULL),
      block_cache(NULL),
      block_size(kDefaultBlockSize),
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(NULL),
      exist_lg_list(NULL),
      lg_info_list(NULL),
      enable_strategy_when_get(false),
      compact_strategy_factory(NULL),
      log_file_size(2 << 20),
      log_async_mode(true),
      max_block_log_number(50),
      write_log_time_out(5),
      flush_triggered_log_num(100000),
      flush_triggered_log_size(40 << 20),
      manifest_switch_interval(60*60),
      raw_key_format(kReadable),
      seek_latency(10000000),
      dump_mem_on_shutdown(true),
      use_memtable_on_leveldb(false),
      memtable_ldb_write_buffer_size(1 << 20),
      memtable_ldb_block_size(kDefaultBlockSize),
      drop_base_level_del_in_compaction(true),
      sst_size(kDefaultSstSize),
      verify_checksums_in_compaction(false),
      ignore_corruption_in_compaction(false),
      disable_wal(false) {
}

}  // namespace leveldb
