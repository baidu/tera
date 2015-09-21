// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_IO_TABLET_IO_H_
#define TERA_IO_TABLET_IO_H_

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/base/scoped_ptr.h"
#include "common/mutex.h"
#include "io/stream_scan.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "proto/proto_helper.h"
#include "proto/status_code.pb.h"
#include "proto/table_meta.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "types.h"
#include "utils/counter.h"
#include "utils/rpc_timer_list.h"

namespace tera {
namespace io {

class TabletWriter;

class TabletIO {
public:
    enum TabletStatus {
        kNotInit = kTabletNotInit,
        kReady = kTabletReady,
        kOnLoad = kTabletOnLoad,
        kOnSplit = kTabletOnSplit,
        kSplited = kTabletSplited,
        kUnLoading = kTabletUnLoading,
        kUnLoading2 = kTabletUnLoading2
    };
    typedef std::map< std::string, std::set<std::string> > ColumnFamilyMap;
    struct ScanOptions {
        uint32_t max_versions;
        uint32_t max_size;
        int64_t ts_start;
        int64_t ts_end;
        uint64_t snapshot_id;
        FilterList filter_list;
        ColumnFamilyMap column_family_list;
        std::set<std::string> iter_cf_set;

        ScanOptions()
            : max_versions(UINT32_MAX), max_size(UINT32_MAX),
              ts_start(kOldestTs), ts_end(kLatestTs), snapshot_id(0)
        {}
    };

    struct StatCounter {
        tera::Counter low_read_cell;
        tera::Counter scan_rows;
        tera::Counter scan_kvs;
        tera::Counter scan_size;
        tera::Counter read_rows;
        tera::Counter read_kvs;
        tera::Counter read_size;
        tera::Counter write_rows;
        tera::Counter write_kvs;
        tera::Counter write_size;
    };

public:
    TabletIO();
    virtual ~TabletIO();

    std::string GetTableName() const;
    std::string GetTablePath() const;
    std::string GetStartKey() const;
    std::string GetEndKey() const;
    virtual CompactStatus GetCompactStatus() const;
    virtual const TableSchema& GetSchema() const;
    bool KvOnly() const { return m_kv_only; }
    StatCounter& GetCounter();
    // tablet
    virtual bool Load(const TableSchema& schema,
                      const std::string& key_start, const std::string& key_end,
                      const std::string& path,
                      const std::vector<uint64_t>& parent_tablets,
                      std::map<uint64_t, uint64_t> snapshots,
                      std::map<uint64_t, uint64_t> rollbacks,
                      leveldb::Logger* logger = NULL,
                      leveldb::Cache* block_cache = NULL,
                      leveldb::TableCache* table_cache = NULL,
                      StatusCode* status = NULL);
    virtual bool Unload(StatusCode* status = NULL);
    virtual bool Split(std::string* split_key, StatusCode* status = NULL);
    virtual bool Compact(StatusCode* status = NULL);
    bool CompactMinor(StatusCode* status = NULL);
    bool Destroy(StatusCode* status = NULL);
    virtual int64_t GetDataSize(std::vector<uint64_t>* lgsize = NULL,
                                StatusCode* status = NULL);
    virtual int64_t GetDataSize(const std::string& start_key,
                                const std::string& end_key,
                                StatusCode* status = NULL);
    virtual bool AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live);

    bool IsBusy();

    bool SnapshotIDToSeq(uint64_t snapshot_id, uint64_t* snapshot_sequence);

    virtual bool Read(const leveldb::Slice& key, std::string* value,
                      uint64_t snapshot_id = 0, StatusCode* status = NULL);

    // read a row
    virtual bool ReadCells(const RowReaderInfo& row_reader, RowResult* value_list,
                           uint64_t snapshot_id = 0, StatusCode* status = NULL);
    /// scan from leveldb return ture means complete flase means not complete
    bool LowLevelScan(const std::string& start_tera_key,
                      const std::string& end_row_key,
                      const ScanOptions& scan_options,
                      RowResult* value_list,
                      uint32_t* read_row_count,
                      uint32_t* read_bytes,
                      bool* is_complete,
                      StatusCode* status = NULL);

    bool LowLevelSeek(const std::string& row_key, const ScanOptions& scan_options,
                      RowResult* value_list, StatusCode* status = NULL);

    bool WriteOne(const std::string& key, const std::string& value,
                  bool sync = true, StatusCode* status = NULL);
    bool WriteBatch(leveldb::WriteBatch* batch, bool disable_wal = false, bool sync = true,
                    StatusCode* status = NULL);
    virtual bool Write(const WriteTabletRequest* request,
                       WriteTabletResponse* response,
                       google::protobuf::Closure* done,
                       const std::vector<int32_t>* index_list,
                       Counter* done_counter, WriteRpcTimer* timer = NULL,
                       StatusCode* status = NULL);

    virtual bool Scan(const ScanOption& option, KeyValueList* kv_list,
                      bool* complete, StatusCode* status = NULL);

    virtual bool ScanRows(const ScanTabletRequest* request,
                          ScanTabletResponse* response,
                          google::protobuf::Closure* done);

    uint64_t GetSnapshot(uint64_t id, uint64_t snapshot_sequence,
                         StatusCode* status = NULL);
    bool ReleaseSnapshot(uint64_t snapshot_id,  StatusCode* status = NULL);
    void ListSnapshot(std::vector<uint64_t>* snapshot_id);

    uint64_t Rollback(uint64_t snapshot_id, StatusCode* status);

    uint32_t GetLGidByCFName(const std::string& cfname);

    const leveldb::RawKeyOperator* GetRawKeyOperator();

    void SetStatus(TabletStatus status);
    TabletStatus GetStatus();

    void GetAndClearCounter(TabletCounter* counter, int64_t interval);

    int32_t AddRef();
    int32_t DecRef();
    int32_t GetRef() const;

private:
    friend class TabletWriter;
    bool WriteWithoutLock(const std::string& key, const std::string& value,
                          bool sync = false, StatusCode* status = NULL);
//     int64_t GetDataSizeWithoutLock(StatusCode* status = NULL);

    void SetupOptionsForLG();
    void TearDownOptionsForLG();
    void IndexingCfToLG();

    void SetupIteratorOptions(const ScanOptions& scan_options,
                              leveldb::ReadOptions* leveldb_opts);
    void TearDownIteratorOptions(leveldb::ReadOptions* opts);

    void ProcessRowBuffer(std::list<KeyValuePair>& row_buf,
                          const ScanOptions& scan_options,
                          RowResult* value_list,
                          uint32_t* buffer_size);

    StatusCode InitedScanInterator(const std::string& start_tera_key,
                                   const ScanOptions& scan_options,
                                   leveldb::Iterator** scan_it);

    bool ScanRowsRestricted(const ScanTabletRequest* request,
                            ScanTabletResponse* response,
                            google::protobuf::Closure* done);
    bool ScanRowsStreaming(const ScanTabletRequest* request,
                           ScanTabletResponse* response,
                           google::protobuf::Closure* done);

    void SetupScanInternalTeraKey(const ScanTabletRequest* request,
                                  std::string* start_tera_key,
                                  std::string* end_row_key);
    void SetupScanRowOptions(const ScanTabletRequest* request,
                             ScanOptions* scan_options);

    bool LowLevelScan(const std::string& start_tera_key,
                      const std::string& end_row_key,
                      const ScanOptions& scan_options,
                      leveldb::Iterator* it,
                      RowResult* value_list,
                      uint32_t* read_row_count,
                      uint32_t* read_bytes,
                      bool* is_complete,
                      StatusCode* status);

private:
    mutable Mutex m_mutex;
    TabletWriter* m_async_writer;

    std::string m_tablet_path;
    std::string m_start_key;
    std::string m_end_key;
    std::string m_raw_start_key;
    std::string m_raw_end_key;
    CompactStatus m_compact_status;

    TabletStatus m_status;
    volatile int32_t m_ref_count;
    volatile int32_t m_db_ref_count;
    leveldb::Options m_ldb_options;
    leveldb::DB* m_db;
    bool m_mem_store_activated;
    TableSchema m_table_schema;
    bool m_kv_only;
    std::map<uint64_t, uint64_t> id_to_snapshot_num_;
    std::map<uint64_t, uint64_t> rollbacks_;

    const leveldb::RawKeyOperator* m_key_operator;

    std::map<std::string, uint32_t> m_cf_lg_map;
    std::map<std::string, uint32_t> m_lg_id_map;
    StreamScanManager m_stream_scan;
    StatCounter m_counter;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_TABLET_IO_H_
