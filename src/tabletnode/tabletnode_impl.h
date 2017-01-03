// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_IMPL_H_
#define TERA_TABLETNODE_TABLETNODE_IMPL_H_

#include <string>

#include "common/base/scoped_ptr.h"
#include "common/thread_pool.h"

#include "io/tablet_io.h"
#include "proto/master_rpc.pb.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "tabletnode/rpc_compactor.h"
#include "tabletnode/tabletnode_sysinfo.h"
#include "utils/rpc_timer_list.h"

namespace tera {
namespace tabletnode {

class TabletManager;
class TabletNodeZkAdapterBase;

class TabletNodeImpl {
public:
    enum TabletNodeStatus {
        kNotInited = kTabletNodeNotInited,
        kIsIniting = kTabletNodeIsIniting,
        kIsBusy = kTabletNodeIsBusy,
        kIsReadonly = kTabletNodeIsReadonly,
        kIsRunning = kTabletNodeIsRunning
    };

    struct WriteTabletTask {
        std::vector<const RowMutationSequence*> row_mutation_vec;
        std::vector<StatusCode> row_status_vec;
        std::vector<int32_t> row_index_vec;
        Counter* row_done_counter;

        const WriteTabletRequest* request;
        WriteTabletResponse* response;
        google::protobuf::Closure* done;
        WriteRpcTimer* timer;

        WriteTabletTask(const WriteTabletRequest* req, WriteTabletResponse* resp,
                   google::protobuf::Closure* d, WriteRpcTimer* t, Counter* c)
            : row_done_counter(c), request(req), response(resp), done(d), timer(t) {}
    };

    TabletNodeImpl();
    ~TabletNodeImpl();

    bool Init();

    bool Exit();

    void GarbageCollect();

    void LoadTablet(const LoadTabletRequest* request,
                    LoadTabletResponse* response,
                    google::protobuf::Closure* done);

    bool UnloadTablet(const std::string& tablet_name,
                      const std::string& start, const std::string& end,
                      StatusCode* status);

    void UnloadTablet(const UnloadTabletRequest* request,
                      UnloadTabletResponse* response,
                      google::protobuf::Closure* done);

    void CompactTablet(const CompactTabletRequest* request,
                       CompactTabletResponse* response,
                       google::protobuf::Closure* done);

    void Update(const UpdateRequest* request,
                UpdateResponse* response,
                google::protobuf::Closure* done);

    void ReadTablet(int64_t start_micros,
                    const ReadTabletRequest* request,
                    ReadTabletResponse* response,
                    google::protobuf::Closure* done);

    void WriteTablet(const WriteTabletRequest* request,
                     WriteTabletResponse* response,
                     google::protobuf::Closure* done,
                     WriteRpcTimer* timer = NULL);

    void ScanTablet(const ScanTabletRequest* request,
                    ScanTabletResponse* response,
                    google::protobuf::Closure* done);

    void GetSnapshot(const SnapshotRequest* request, SnapshotResponse* response,
                     google::protobuf::Closure* done);

    void ReleaseSnapshot(const ReleaseSnapshotRequest* request,
                         ReleaseSnapshotResponse* response,
                         google::protobuf::Closure* done);

    void Rollback(const SnapshotRollbackRequest* request, SnapshotRollbackResponse* response,
                  google::protobuf::Closure* done);

    void CmdCtrl(const TsCmdCtrlRequest* request, TsCmdCtrlResponse* response,
                 google::protobuf::Closure* done);

    void Query(const QueryRequest* request, QueryResponse* response,
               google::protobuf::Closure* done);

    void SplitTablet(const SplitTabletRequest* request,
                     SplitTabletResponse* response,
                     google::protobuf::Closure* done);

    void EnterSafeMode();
    void LeaveSafeMode();
    void ExitService();

    void SetTabletNodeStatus(const TabletNodeStatus& status);
    TabletNodeStatus GetTabletNodeStatus();

    void SetRootTabletAddr(const std::string& root_tablet_addr);

    void SetSessionId(const std::string& session_id);
    std::string GetSessionId();

    std::string BlockCacheProfileInfo();

    std::string TableCacheProfileInfo();

    TabletNodeSysInfo& GetSysInfo();

    void RefreshSysInfo();

    void TryReleaseMallocCache();

private:
    // call this when fail to write TabletIO
    void WriteTabletFail(WriteTabletTask* tablet_task, StatusCode status);

    // write callback for TabletIO::Write()
    void WriteTabletCallback(WriteTabletTask* tablet_task,
                             std::vector<const RowMutationSequence*>* row_mutation_vec,
                             std::vector<StatusCode>* status_vec);

    bool CheckInKeyRange(const KeyList& key_list,
                         const std::string& key_start,
                         const std::string& key_end);
    bool CheckInKeyRange(const KeyValueList& pair_list,
                         const std::string& key_start,
                         const std::string& key_end);
    bool CheckInKeyRange(const RowMutationList& row_list,
                         const std::string& key_start,
                         const std::string& key_end);
    bool CheckInKeyRange(const RowReaderList& reader_list,
                         const std::string& key_start,
                         const std::string& key_end);

    void UpdateMetaTableAsync(const SplitTabletRequest* request,
             SplitTabletResponse* response, google::protobuf::Closure* done,
             const std::string& path, const std::string& key_split,
             const TableSchema& schema, int64_t first_size, int64_t second_size,
             const TabletMeta& meta);
    void UpdateMetaTableCallback(const SplitTabletRequest* rpc_request,
             SplitTabletResponse* rpc_response, google::protobuf::Closure* rpc_done,
             WriteTabletRequest* request, WriteTabletResponse* response,
             bool failed, int error_code);

    void InitCacheSystem();

    void ReleaseMallocCache();
    void EnableReleaseMallocCacheTimer(int32_t expand_factor = 1);
    void DisableReleaseMallocCacheTimer();

    void GetInheritedLiveFiles(std::vector<InheritedLiveFiles>& inherited);

    void GarbageCollectInPath(const std::string& path, leveldb::Env* env,
                              const std::set<std::string>& inherited_files,
                              const std::set<std::string> active_tablets);

    bool ApplySchema(const UpdateRequest* request);
private:
    mutable Mutex status_mutex_;
    TabletNodeStatus status_;
    Mutex mutex_;

    scoped_ptr<TabletManager> tablet_manager_;
    scoped_ptr<TabletNodeZkAdapterBase> zk_adapter_;

    uint64_t this_sequence_id_;
    std::string local_addr_;
    std::string root_tablet_addr_;
    std::string session_id_;
    int64_t release_cache_timer_id_;

    TabletNodeSysInfo sysinfo_;

    scoped_ptr<ThreadPool> thread_pool_;

    leveldb::Logger* ldb_logger_;
    leveldb::Cache* ldb_block_cache_;
    leveldb::TableCache* ldb_table_cache_;
};

} // namespace tabletnode
} // namespace tera

#endif // TERA_TABLETNODE_TABLETNODE_IMPL_H_
