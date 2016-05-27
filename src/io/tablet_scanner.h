// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef TERA_IO_TABLET_SCANNER_H_
#define TERA_IO_TABLET_SCANNER_H_

#include "common/mutex.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/compact_strategy.h"
#include "proto/tabletnode_rpc.pb.h"
#include "types.h"
#include <limits>
#include <queue>

namespace tera {
namespace io {

class TabletIO;

typedef std::map< std::string, std::set<std::string> > ColumnFamilyMap;
struct ScanOptions {
    uint32_t max_versions;
    uint32_t max_size;
    int64_t number_limit; // kv number > number_limit, return to user
    int64_t ts_start;
    int64_t ts_end;
    uint64_t snapshot_id;
    FilterList filter_list;
    ColumnFamilyMap column_family_list;
    std::set<std::string> iter_cf_set;
    int64_t timeout;

    ScanOptions()
            : max_versions(std::numeric_limits<uint32_t>::max()),
              max_size(std::numeric_limits<uint32_t>::max()),
              number_limit(std::numeric_limits<int64_t>::max()),
              ts_start(kOldestTs), ts_end(kLatestTs), snapshot_id(0), timeout(std::numeric_limits<int64_t>::max() / 2)
    {}
};

class ScanContextManager;
typedef std::pair<ScanTabletResponse*, google::protobuf::Closure*> ScanJob;
struct ScanContext {
    int64_t m_session_id;
    TabletIO* m_tablet_io;

    // use for lowlevelscan
    std::string m_start_tera_key;
    std::string m_end_row_key;
    ScanOptions m_scan_options;
    leveldb::Iterator* m_it; // init to NULL
    leveldb::CompactStrategy* m_compact_strategy;
    uint32_t m_version_num;
    std::string m_last_key;
    std::string m_last_col;
    std::string m_last_qual;

    // use for reture
    StatusCode m_ret_code; // set by lowlevelscan
    bool m_complete; // test this flag know whether scan finish or not
    RowResult* m_result; // scan result for one round
    uint64_t m_data_idx; // return data_id

    // protect by manager lock
    std::queue<ScanJob> m_jobs;
    std::queue<leveldb::Cache::Handle*> m_handles;
};

class ScanContextManager {
public:
    ScanContextManager();
    ~ScanContextManager();

    ScanContext* GetScanContext(TabletIO* tablet_io, const ScanTabletRequest* request,
                ScanTabletResponse* response, google::protobuf::Closure* done);
    bool ScheduleScanContext(ScanContext* context);
    void DestroyScanCache();

private:
    void DeleteScanContext(ScanContext* context);

    // <session_id, ScanContext>

    Mutex m_lock;
    ::leveldb::Cache* cache_;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_TABLET_SCANNER_H
