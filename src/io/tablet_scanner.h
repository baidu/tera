// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef TERA_IO_TABLET_SCANNER_H_
#define TERA_IO_TABLET_SCANNER_H_

#include "types.h"
#include <limits>
#include <queue>

#include "common/mutex.h"
#include "leveldb/cache.h"
#include "leveldb/compact_strategy.h"
#include "leveldb/db.h"
#include "proto/tabletnode_rpc.pb.h"
#include "tera.h"

namespace tera {
namespace io {

class TabletIO;

typedef std::map<std::string, std::set<std::string> > ColumnFamilyMap;
struct ScanOptions {
  uint32_t max_versions;
  uint32_t max_size;
  int64_t number_limit;  // kv number > number_limit, return to user
  int64_t ts_start;
  int64_t ts_end;
  uint64_t snapshot_id;
  filter::FilterPtr filter;
  ColumnFamilyMap column_family_list;
  std::set<std::string> iter_cf_set;
  filter::ColumnSet filter_column_set;
  int64_t timeout;
  uint64_t max_qualifiers;
  // If sdk uses batch scan, we will use prefetch scan iterator.;
  bool is_batch_scan;
  bool enable_dfs_read_thread_limiter;

  ScanOptions()
      : max_versions(std::numeric_limits<uint32_t>::max()),
        max_size(std::numeric_limits<uint32_t>::max()),
        number_limit(std::numeric_limits<int64_t>::max()),
        ts_start(kOldestTs),
        ts_end(kLatestTs),
        snapshot_id(0),
        timeout(std::numeric_limits<int64_t>::max() / 2),
        max_qualifiers(std::numeric_limits<uint64_t>::max()),
        is_batch_scan(false),
        enable_dfs_read_thread_limiter(false) {}
};

class ScanContextManager;
typedef std::pair<ScanTabletResponse*, google::protobuf::Closure*> ScanJob;
struct ScanContext {
  int64_t session_id;
  TabletIO* tablet_io;

  // use for lowlevelscan
  std::string start_tera_key;
  std::string end_row_key;
  ScanOptions scan_options;
  leveldb::Iterator* it;  // init to NULL
  leveldb::CompactStrategy* compact_strategy;
  uint32_t version_num;
  uint64_t qu_num;
  std::string last_key;
  std::string last_col;
  std::string last_qual;

  // use for reture
  StatusCode ret_code;  // set by lowlevelscan
  bool complete;        // test this flag know whether scan finish or not
  RowResult* result;    // scan result for one round
  uint64_t data_idx;    // return data_id
  uint32_t cell_count;  // scan total cell count for one round, kvtable cell_count equal row_count
  uint32_t row_count;   // scan total row count for one round
  uint32_t data_size;   // scan total data size for one round

  // protect by manager lock
  std::queue<ScanJob> jobs;
  leveldb::Cache::Handle* handle;
};

class ScanContextManager {
 public:
  ScanContextManager();
  ~ScanContextManager();

  ScanContext* GetScanContext(TabletIO* tablet_io, const ScanTabletRequest* request,
                              ScanTabletResponse* response, google::protobuf::Closure* done);
  bool ScheduleScanContext(ScanContext* context);

 private:
  void DeleteScanContext(ScanContext* context);

  // <session_id, ScanContext>

  Mutex lock_;
  ::leveldb::Cache* cache_;
};

}  // namespace io
}  // namespace tera

#endif  // TERA_IO_TABLET_SCANNER_H
