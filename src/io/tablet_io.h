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

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "common/base/scoped_ptr.h"
#include "common/mutex.h"
#include "io/tablet_scanner.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/raw_key_operator.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "proto/proto_helper.h"
#include "proto/status_code.pb.h"
#include "proto/table_meta.pb.h"
#include "proto/table_schema.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "types.h"
#include "utils/counter.h"

namespace tera {
namespace io {

class TabletWriter;
struct ScanOptions;
struct ScanContext;
class ScanContextManager;

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

    typedef boost::function<void (std::vector<const RowMutationSequence*>*,
                                  std::vector<StatusCode>*)> WriteCallback;

public:
    TabletIO(const std::string& key_start, const std::string& key_end);
    virtual ~TabletIO();

    std::string GetTableName() const;
    std::string GetTablePath() const;
    std::string GetStartKey() const;
    std::string GetEndKey() const;
    virtual CompactStatus GetCompactStatus() const;
    virtual TableSchema GetSchema() const;
    RawKey RawKeyType() const;
    bool KvOnly() const { return m_kv_only; }
    StatCounter& GetCounter();
    // tablet
    virtual bool Load(const TableSchema& schema,
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
    virtual bool Compact(int lg_no = -1, StatusCode* status = NULL);
    bool CompactMinor(StatusCode* status = NULL);
    bool Destroy(StatusCode* status = NULL);
    virtual bool GetDataSize(uint64_t* size, std::vector<uint64_t>* lgsize = NULL,
                             StatusCode* status = NULL);
    virtual bool AddInheritedLiveFiles(std::vector<std::set<uint64_t> >* live);

    bool IsBusy();
    bool Workload(double* write_workload);

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
                      KeyValuePair* next_start_point,
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
    bool Write(std::vector<const RowMutationSequence*>* row_mutation_vec,
               std::vector<StatusCode>* status_vec, bool is_instant,
               WriteCallback callback, StatusCode* status = NULL);

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

    void GetAndClearCounter(TabletCounter* counter);

    int32_t AddRef();
    int32_t DecRef();
    int32_t GetRef() const;

    static bool FindAverageKey(const std::string& start, const std::string& end,
                               std::string* res);
    void ProcessScan(ScanContext* context);
    void ApplySchema(const TableSchema& schema);

private:
    friend class TabletWriter;
    friend class ScanConextManager;
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
                          uint32_t* buffer_size,
                          int64_t* number_limit);

    StatusCode InitedScanInterator(const std::string& start_tera_key,
                                   const ScanOptions& scan_options,
                                   leveldb::Iterator** scan_it);

    bool ScanRowsRestricted(const ScanTabletRequest* request,
                            ScanTabletResponse* response,
                            google::protobuf::Closure* done);
    // tablet scanner
    bool HandleScan(const ScanTabletRequest* request,
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
                      ScanContext* scan_context,
                      RowResult* value_list,
                      KeyValuePair* next_start_point,
                      uint32_t* read_row_count,
                      uint32_t* read_bytes,
                      bool* is_complete,
                      StatusCode* status);

    void MakeKvPair(leveldb::Slice key, leveldb::Slice col, leveldb::Slice qual,
                    int64_t ts, leveldb::Slice value, KeyValuePair* kv);

    bool ParseRowKey(const std::string& tera_key, std::string* row_key);
    bool ShouldFilterRowBuffer(std::list<KeyValuePair>& row_buf,
                               const ScanOptions& scan_options);

    bool ScanWithFilter(const ScanOptions& scan_options);
    bool IsCompleteRow(const std::list<KeyValuePair>& row_buf,
                       leveldb::Iterator* it);
    bool ShouldFilterRow(const ScanOptions& scan_options,
                           const std::list<KeyValuePair>& row_buf,
                           leveldb::Iterator* it);
    void GotoNextRow(const std::list<KeyValuePair>& row_buf,
                     leveldb::Iterator* it,
                     KeyValuePair* next);
    void SetSchema(const TableSchema& schema);
private:
    mutable Mutex m_mutex;
    TabletWriter* m_async_writer;
    ScanContextManager* m_scan_context_manager;

    std::string m_tablet_path;
    const std::string m_start_key;
    const std::string m_end_key;
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
    StatCounter m_counter;
    mutable Mutex m_schema_mutex;
};

} // namespace io
} // namespace tera

#endif // TERA_IO_TABLET_IO_H_
