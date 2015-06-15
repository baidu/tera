// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MASTER_IMPL_H_
#define TERA_MASTER_MASTER_IMPL_H_

#include <semaphore.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "common/base/scoped_ptr.h"
#include "common/event.h"
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "gflags/gflags.h"

#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "proto/master_rpc.pb.h"
#include "proto/table_meta.pb.h"
#include "sdk/client_impl.h"
#include "sdk/table_impl.h"

DECLARE_int32(tera_master_impl_retry_times);

namespace tera {

class LoadTabletRequest;
class LoadTabletResponse;
class MergeTabletRequest;
class MergeTabletResponse;
class QueryRequest;
class QueryResponse;
class ScanTabletRequest;
class ScanTabletResponse;
class SplitTabletRequest;
class SplitTabletResponse;
class UnloadTabletRequest;
class UnloadTabletResponse;
class WriteTabletRequest;
class WriteTabletResponse;

namespace master {

class MasterZkAdapterBase;
class MetaTable;
class Scheduler;
class TabletManager;
class TabletNodeManager;

class MasterImpl {
public:
    enum MasterStatus {
        kNotInited = kMasterNotInited,
        kIsBusy = kMasterIsBusy,
        kIsSecondary = kMasterIsSecondary,
        kIsReadonly = kMasterIsReadonly,
        kIsRunning = kMasterIsRunning,
        kOnRestore = kMasterOnRestore,
        kOnWait = kMasterOnWait
    };

    // great number comes great priority
    enum ConcurrencyTaskPriority {
        // unload
        kTaskUnloadForDisable = 5,
        kTaskUnload = 10,
        kTaskUnloadForMerge = 15,
        kTaskUnloadForBalance = 20,

        // load
        kTaskLoad = 10,

        // split
        kTaskSplit = 10
    };

    MasterImpl();
    ~MasterImpl();

    bool Init();

    bool Restore(const std::map<std::string, std::string>& tabletnode_list);

    void GetSnapshot(const GetSnapshotRequest* request,
                     GetSnapshotResponse* response,
                     google::protobuf::Closure* done);

    void DelSnapshot(const DelSnapshotRequest* request,
                     DelSnapshotResponse* response,
                     google::protobuf::Closure* done);

    void CreateTable(const CreateTableRequest* request,
                     CreateTableResponse* response,
                     google::protobuf::Closure* done);

    void DeleteTable(const DeleteTableRequest* request,
                     DeleteTableResponse* response,
                     google::protobuf::Closure* done);

    void DisableTable(const DisableTableRequest* request,
                      DisableTableResponse* response,
                      google::protobuf::Closure* done);

    void EnableTable(const EnableTableRequest* request,
                     EnableTableResponse* response,
                     google::protobuf::Closure* done);

    void UpdateTable(const UpdateTableRequest* request,
                     UpdateTableResponse* response,
                     google::protobuf::Closure* done);

    void CompactTable(const CompactTableRequest* request,
                      CompactTableResponse* response,
                      google::protobuf::Closure* done);

    void SearchTable(const SearchTableRequest* request,
                     SearchTableResponse* response,
                     google::protobuf::Closure* done);

    void ShowTables(const ShowTablesRequest* request,
                    ShowTablesResponse* response,
                    google::protobuf::Closure* done);

    void ShowTabletNodes(const ShowTabletNodesRequest* request,
                         ShowTabletNodesResponse* response,
                         google::protobuf::Closure* done);

    void CmdCtrl(const CmdCtrlRequest* request,
                 CmdCtrlResponse* response);

    void RefreshTabletNodeList(const std::map<std::string, std::string>& ts_node_list);

    bool SetMasterStatus(const MasterStatus& new_status,
                         MasterStatus* old_status = NULL);
    MasterStatus GetMasterStatus();

    void EnableQueryTabletNodeTimer();
    void DisableQueryTabletNodeTimer();

    bool GetMetaTabletAddr(std::string* addr);

private:
    typedef Closure<void, SnapshotRequest*, SnapshotResponse*, bool, int> SnapshotClosure;
    typedef Closure<void, ReleaseSnapshotRequest*, ReleaseSnapshotResponse*, bool, int> DelSnapshotClosure;
    typedef Closure<void, QueryRequest*, QueryResponse*, bool, int> QueryClosure;
    typedef Closure<void, LoadTabletRequest*, LoadTabletResponse*, bool, int> LoadClosure;
    typedef Closure<void, UnloadTabletRequest*, UnloadTabletResponse*, bool, int> UnloadClosure;
    typedef Closure<void, SplitTabletRequest*, SplitTabletResponse*, bool, int> SplitClosure;
    typedef Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int> WriteClosure;
    typedef Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int> ScanClosure;

    enum MetaTaskType {
        kWrite = 0,
        kScan,
        kRepair
    };
    struct MetaTask {
        MetaTaskType m_type;
    };
    struct WriteTask {
        MetaTaskType m_type;
        WriteClosure* m_done;
        TablePtr m_table;
        std::vector<TabletPtr> m_tablet;
        bool m_is_delete;
    };
    struct ScanTask {
        MetaTaskType m_type;
        ScanClosure* m_done;
        std::string m_table_name;
        std::string m_tablet_key_start;
        std::string m_tablet_key_end;
    };
    struct RepairTask {
        MetaTaskType m_type;
        WriteClosure* m_done;
        TabletPtr m_tablet;
        ScanTabletResponse* m_scan_resp;
    };
    struct SnapshotTask {
        const GetSnapshotRequest* request;
        GetSnapshotResponse* response;
        google::protobuf::Closure* done;
        TablePtr table;
        std::vector<TabletPtr> tablets;
        std::vector<uint64_t> snapshot_id;
        int task_num;
        int finish_num;
        mutable Mutex mutex;
        bool aborted;
    };

    void SafeModeCmdCtrl(const CmdCtrlRequest* request,
                         CmdCtrlResponse* response);
    void TabletCmdCtrl(const CmdCtrlRequest* request,
                       CmdCtrlResponse* response);
    void MetaCmdCtrl(const CmdCtrlRequest* request,
                     CmdCtrlResponse* response);

    bool LoadTabletSync(const TabletMeta& meta,
                        const TableSchema& schema,
                        StatusCode* status);
    bool UnloadTabletSync(const std::string& table_name,
                          const std::string& key_start,
                          const std::string& key_end,
                          const std::string& server_addr, StatusCode* status);
    void UnloadTabletAsync(std::string table_name, std::string key_start,
                           std::string server_addr, int32_t retry);

    void RetryLoadTablet(TabletPtr tablet, int32_t retry_times);
    void RetryUnloadTablet(TabletPtr tablet, int32_t retry_times);
    void TryLoadTablet(TabletPtr tablet, int32_t priority, std::string addr = "");
    bool TrySplitTablet(TabletPtr tablet, int32_t priority);
    void TryUnloadTablet(TabletPtr tablet, UnloadClosure* done, int32_t priority);
    bool TryMergeTablet(TabletPtr tablet, int32_t priority);
    void TryMoveTablet(TabletPtr tablet, int32_t priority, const std::string& server_addr = "");

    void TryReleaseCache(bool enbaled_debug = false);
    void ReleaseCacheWrapper();
    void EnableReleaseCacheTimer();
    void DisableReleaseCacheTimer();
    void EnableLoadBalanceTimer();
    void DisableLoadBalanceTimer();

    void InitAsync();

    bool CreateAndLoadTable(const std::string& table_name,
                            bool compress, StoreMedium store, StatusCode* status);
    void LoadTabletAsync(TabletPtr tablet, LoadClosure* done,
                         uint64_t timer_id = 0);
    void LoadTabletCallback(TabletPtr tablet, int32_t retry,
                            LoadTabletRequest* request,
                            LoadTabletResponse* response, bool failed,
                            int error_code);

    bool RemoveTablet(const TabletMeta& meta, StatusCode* status);
    void UnloadTabletAsync(TabletPtr tablet, UnloadClosure* done);
    void UnloadTabletCallback(TabletPtr tablet, int32_t retry,
                              UnloadTabletRequest* request,
                              UnloadTabletResponse* response, bool failed,
                              int error_code);
    void MoveTabletCallback(TabletPtr tablet, int32_t retry,
                            UnloadTabletRequest* request,
                            UnloadTabletResponse* response,
                            bool failed, int error_code);
    void DeleteTabletCallback(TabletPtr tablet, int32_t retry,
                              UnloadTabletRequest* request,
                              UnloadTabletResponse* response,
                              bool failed, int error_code);

    void LoadBalance();
    void TabletNodeLoadBalance(const std::string& tabletnode_addr,
                               const std::vector<TabletPtr>& tablet_list);
    void TabletNodeLoadBalance(const std::string& tabletnode_addr,
                               const std::string& table_name,
                               const std::vector<TabletPtr>& tablet_list);

    void GetSnapshotAsync(TabletPtr tablet, int32_t timeout,
                          SnapshotClosure* done);
    void GetSnapshotCallback(int32_t tablet_id, SnapshotTask* task,
                             SnapshotRequest* master_request,
                             SnapshotResponse* master_response,
                             bool failed, int error_code);
    void AddSnapshotCallback(TablePtr table,
                             std::vector<TabletPtr> tablets,
                             int32_t retry_times,
                             const GetSnapshotRequest* rpc_request,
                             GetSnapshotResponse* rpc_response,
                             google::protobuf::Closure* rpc_done,
                             WriteTabletRequest* request,
                             WriteTabletResponse* response,
                             bool failed, int error_code);
    void DelSnapshotCallback(TablePtr table,
                             std::vector<TabletPtr> tablets,
                             int32_t retry_times,
                             const DelSnapshotRequest* rpc_request,
                             DelSnapshotResponse* rpc_response,
                             google::protobuf::Closure* rpc_done,
                             WriteTabletRequest* request,
                             WriteTabletResponse* response,
                             bool failed, int error_code);
    void QueryTabletNode();
    void QueryTabletNodeAsync(std::string addr, int32_t timeout,
                              bool is_gc, QueryClosure* done);

    void ReleaseSnpashot(TabletPtr tablet, uint64_t snapshot);
    void ReleaseSnapshotCallback(ReleaseSnapshotRequest* request,
                               ReleaseSnapshotResponse* response,
                               bool failed,
                               int error_code);
    void ClearUnusedSnapshots(TabletPtr tablet, const TabletMeta& meta);
    void QueryTabletNodeCallback(std::string addr, QueryRequest* request,
                                 QueryResponse* response, bool failed,
                                 int error_code);
    void CollectTabletInfoCallback(std::string addr,
                                   std::vector<TabletMeta>* tablet_list,
                                   sem_t* finish_counter, Mutex* mutex,
                                   QueryRequest* request,
                                   QueryResponse* response, bool failed,
                                   int error_code);
    void TabletNodeRecoveryCallback(std::string addr, QueryRequest* request,
                                    QueryResponse* response, bool failed,
                                    int error_code);
    void RetryCollectTabletInfo(std::string addr,
                                std::vector<TabletMeta>* tablet_list,
                                sem_t* finish_counter, Mutex* mutex);
    void RetryQueryNewTabletNode(std::string addr);

    void SplitTabletAsync(TabletPtr tablet);
    void SplitTabletCallback(TabletPtr tablet, SplitTabletRequest* request,
                             SplitTabletResponse* response, bool failed,
                             int error_code);

    void MergeTabletAsync(TabletPtr tablet_p1, TabletPtr tablet_p2, int32_t priority);
    void MergeTabletAsyncPhase2(TabletPtr tablet_p1, TabletPtr tablet_p2);
    void MergeTabletUnloadCallback(TabletPtr tablet, TabletPtr tablet2, Mutex* mutex,
                                           UnloadTabletRequest* request,
                                           UnloadTabletResponse* response,
                                           bool failed, int error_code);
    void MergeTabletWriteMetaCallback(TabletMeta new_meta, TabletPtr tablet_p1,
                                      TabletPtr tablet_p2, int32_t retry_times,
                                      WriteTabletRequest* request,
                                      WriteTabletResponse* response,
                                      bool failed, int error_code);
    void MergeTabletFailed(TabletPtr tablet_p1, TabletPtr tablet_p2);

    void WriteMetaTableAsync(TablePtr table, bool is_delete,
                             WriteClosure* done);
    void WriteMetaTableAsync(TabletPtr tablet, bool is_delete,
                             WriteClosure* done);
    void WriteMetaTableAsync(TablePtr table, TabletPtr tablet, bool is_delete,
                             WriteClosure* done);
    void BatchWriteMetaTableAsync(TablePtr table,
                                  const std::vector<TabletPtr>& tablets,
                                  bool is_delete, WriteClosure* done);
    void AddMetaCallback(TablePtr table, std::vector<TabletPtr> tablets,
                         int32_t retry_times,
                         const CreateTableRequest* rpc_request,
                         CreateTableResponse* rpc_response,
                         google::protobuf::Closure* rpc_done,
                         WriteTabletRequest* request,
                         WriteTabletResponse* response,
                         bool failed, int error_code);
    void UpdateTableRecordForDisableCallback(TablePtr table, int32_t retry_times,
                                             DisableTableResponse* rpc_response,
                                             google::protobuf::Closure* rpc_done,
                                             WriteTabletRequest* request,
                                             WriteTabletResponse* response,
                                             bool failed, int error_code);
    void UpdateTableRecordForEnableCallback(TablePtr table, int32_t retry_times,
                                            EnableTableResponse* rpc_response,
                                            google::protobuf::Closure* rpc_done,
                                            WriteTabletRequest* request,
                                            WriteTabletResponse* response,
                                            bool failed, int error_code);
    void UpdateTableRecordForUpdateCallback(TablePtr table, int32_t retry_times,
                                            UpdateTableResponse* rpc_response,
                                            google::protobuf::Closure* rpc_done,
                                            WriteTabletRequest* request,
                                            WriteTabletResponse* response,
                                            bool failed, int error_code);
    void UpdateTabletRecordCallback(TabletPtr tablet, int32_t retry_times,
                                    WriteTabletRequest* request,
                                    WriteTabletResponse* response,
                                    bool failed, int error_code);
    void UpdateMetaForLoadCallback(TabletPtr tablet, int32_t retry_times,
                                   int32_t priority,
                                   WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   bool failed, int error_code);
    void DeleteTableRecordCallback(TablePtr table, int32_t retry_times,
                                   WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   bool failed, int error_code);
    void DeleteTabletRecordCallback(TabletPtr tablet, int32_t retry_times,
                                    WriteTabletRequest* request,
                                    WriteTabletResponse* response,
                                    bool failed, int error_code);

    void ScanMetaTableAsync(const std::string& table_name,
                            const std::string& tablet_key_start,
                            const std::string& tablet_key_end,
                            ScanClosure* done);
    void ScanMetaCallbackForSplit(TabletPtr tablet,
                                  ScanTabletRequest* request,
                                  ScanTabletResponse* response,
                                  bool failed, int error_code);

    void RepairMetaTableAsync(TabletPtr tablet,
                              ScanTabletResponse* response,
                              WriteClosure* done);
    void RepairMetaAfterSplitCallback(TabletPtr tablet,
                                      ScanTabletResponse* scan_resp,
                                      int32_t retry_times,
                                      WriteTabletRequest* request,
                                      WriteTabletResponse* response,
                                      bool failed, int error_code);


    bool LoadMetaTablet(std::string* server_addr);
    void UnloadMetaTablet(const std::string& server_addr);

    void AddTabletNode(const std::string& tabletnode_addr,
                       const std::string& tabletnode_id);
    void DeleteTabletNode(const std::string& tabletnode_addr);
    void TryKickTabletNode(const std::string& tabletnode_addr);
    void KickTabletNode(TabletNodePtr node);
    void TryEnterSafeMode();
    void TryLeaveSafeMode();
    bool EnterSafeMode(StatusCode* status = NULL);
    bool LeaveSafeMode(StatusCode* status = NULL);
    void TryMovePendingTablets(std::string tabletnode_addr);
    void TryMovePendingTablet(TabletPtr tablet);
    void MoveOffLineTablets(const std::vector<TabletPtr>& tablet_list);
    double LiveNodeTabletRatio();
    void LoadAllDeadNodeTablets();
    void LoadAllOffLineTablets();

    void CollectAllTabletInfo(const std::map<std::string, std::string>& tabletnode_list,
                              std::vector<TabletMeta>* tablet_list);
    bool RestoreMetaTablet(const std::vector<TabletMeta>& tablet_list,
                           std::string* meta_tablet_addr);
    void RestoreUserTablet(const std::vector<TabletMeta>& report_tablet_list);
    void LoadAllOffLineTablet();

    void SuspendMetaOperation(TablePtr table, bool is_delete, WriteClosure* done);
    void SuspendMetaOperation(TabletPtr tablet, bool is_delete, WriteClosure* done);
    void SuspendMetaOperation(TablePtr table, TabletPtr tablet, bool is_delete,
                              WriteClosure* done);
    void SuspendMetaOperation(TablePtr table, const std::vector<TabletPtr>& tablets,
                              bool is_delete, WriteClosure* done);
    void SuspendMetaOperation(const std::string& table_name,
                              const std::string& tablet_key_start,
                              const std::string& tablet_key_end,
                              ScanClosure* done);
    void SuspendMetaOperation(TabletPtr tablet, ScanTabletResponse* scan_resp,
                              WriteClosure* done);
    void PushToMetaPendingQueue(MetaTask* task);
    void ResumeMetaOperation();
    void ProcessOffLineTablet(TabletPtr tablet);
    void ProcessReadyTablet(TabletPtr tablet);

    bool CheckStatusSwitch(MasterStatus old_status, MasterStatus new_status);

    // stat table
    bool CreateStatTable();
    static void DumpStatCallBack(RowMutation* mutation);
    void DumpTabletNodeAddrToTable(const std::string& addr);
    void DumpStatToTable(const TabletNode& stat);

    // garbage clean
    void EnableTabletNodeGcTimer();
    void DisableTabletNodeGcTimer();
    void TabletNodeGarbageClean();
    void DoTabletNodeGarbageClean();
    void DoTabletNodeGarbageCleanPhase2(bool is_success);
    void CollectDeadTabletsFiles();
    void CollectSingleDeadTablet(const std::string& tablename, uint64_t tabletnum);
    void DeleteObsoleteFiles();
    void ProcessQueryCallbackForGc(QueryResponse* response);

private:
    mutable Mutex m_status_mutex;
    MasterStatus m_status;
    std::string m_local_addr;

    mutable Mutex m_tabletnode_mutex;
    bool m_restored;
    scoped_ptr<TabletManager> m_tablet_manager;
    scoped_ptr<TabletNodeManager> m_tabletnode_manager;
    scoped_ptr<Scheduler> m_scheduler;
    scoped_ptr<MasterZkAdapterBase> m_zk_adapter;

    Mutex m_mutex;
    int64_t m_release_cache_timer_id;
    int64_t m_query_tabletnode_timer_id;
    int64_t m_load_balance_timer_id;
    Counter m_this_sequence_id;

    scoped_ptr<ThreadPool> m_thread_pool;
    AutoResetEvent m_query_event;

    mutable Mutex m_meta_task_mutex;
    std::queue<MetaTask*> m_meta_task_queue;

    mutable Mutex m_tabletnode_timer_mutex;
    std::map<std::string, int64_t> m_tabletnode_timer_id_map;

    mutable Mutex m_tablet_mutex;

    // stat table
    bool m_is_stat_table;
    std::map<std::string, int64_t> m_ts_stat_update_time;
    mutable Mutex m_stat_table_mutex;
    TableImpl* m_stat_table;

    // tabletnode garbage clean
    // first: live tablet, second: dead tablet
    mutable Mutex m_gc_rw_mutex;
    typedef std::pair<std::set<uint64_t>, std::set<uint64_t> > GcTabletSet;
    std::map<std::string, GcTabletSet> m_gc_tablets;
    typedef std::vector<std::set<uint64_t> > GcFileSet;
    std::map<std::string, GcFileSet> m_gc_live_files;
    std::set<std::string> m_gc_tabletnodes;
    int64_t m_gc_timer_id;
    bool m_gc_query_enable;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_MASTER_IMPL_H_
