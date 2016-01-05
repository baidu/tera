// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef TERA_IO_TABLET_SCANNER_H_
#define TERA_IO_TABLET_SCANNER_H_

#include "proto/tabletnode_rpc.pb.h"
#include "common/mutex.h"
#include "types.h"
#include "leveldb/db.h"
#include <queue>

namespace tera {
namespace io {

class TabletIO;

typedef std::map< std::string, std::set<std::string> > ColumnFamilyMap;
struct ScanOptions {
    uint32_t max_versions;
    uint32_t version_num; // restore version_num for stream scan
    uint32_t max_size;
    int64_t ts_start;
    int64_t ts_end;
    uint64_t snapshot_id;
    FilterList filter_list;
    ColumnFamilyMap column_family_list;
    std::set<std::string> iter_cf_set;
    int64_t timeout;

    ScanOptions()
        : max_versions(UINT32_MAX), version_num(0), max_size(UINT32_MAX),
        ts_start(kOldestTs), ts_end(kLatestTs), snapshot_id(0), timeout(INT64_MAX / 2)
    {}
};

typedef std::pair<ScanTabletResponse*, google::protobuf::Closure*> ScanJob;
struct ScanContext {
    int64_t m_session_id;
    TabletIO* m_tablet_io;

    // use for lowlevelscan
    std::string m_start_tera_key;
    std::string m_end_row_key;
    ScanOptions m_scan_options;
    leveldb::Iterator* m_it; // init to NULL

    // use for reture
    StatusCode m_ret_code; // set by lowlevelscan
    bool m_complete; // test this flag know whether scan finish or not
    RowResult m_result; // scan result for one round
    uint64_t m_data_idx; // return data_id

    // protect by manager lock
    uint64_t m_ref; // init value to 1, use for free scan context
    uint64_t m_lru_seq_no;
    std::queue<ScanJob> m_jobs;
};

class ScanContextManager {
public:
    ScanContextManager();
    ~ScanContextManager();

    ScanContext* GetScanContext(TabletIO* tablet_io, const ScanTabletRequest* request,
                ScanTabletResponse* response, google::protobuf::Closure* done);
    bool ReleaseScanContext(ScanContext* context);
    void DestroyScanCache();

private:
    ScanContext* GetContextFromCache(int64_t session_id);
    bool InsertContextToCache(ScanContext* context);
    void DeleteContextFromCache(ScanContext* context);
    void EvictCache();
    void DeleteScanContext(ScanContext* context);

    Mutex m_lock;
    std::map<int64_t, ScanContext*> m_context_cache;
    uint64_t m_seq_no; // use for lru
    std::map<uint64_t, int64_t> m_context_lru; // <seq_no, session id>
};

} // namespace io
} // namespace tera

#endif // TERA_IO_TABLET_SCANNER_H
