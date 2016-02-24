// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_MASTER_IMPL_H_
#define TERA_MASTER_MASTER_IMPL_H_

#include <stdint.h>
#include <semaphore.h>
#include <string>
#include <vector>

#include "common/event.h"
#include "common/base/scoped_ptr.h"
#include "common/mutex.h"
#include "common/thread_pool.h"
#include "gflags/gflags.h"

#include "master/gc_strategy.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "master/user_manager.h"
#include "proto/master_rpc.pb.h"
#include "proto/table_meta.pb.h"
#include "sdk/client_impl.h"
#include "sdk/table_impl.h"

DECLARE_int32(tera_master_impl_retry_times);

namespace tera {

class LoadTabletRequest;
class LoadTabletResponse;
class UnloadTabletRequest;
class UnloadTabletResponse;
class SplitTabletRequest;
class SplitTabletResponse;
class MergeTabletRequest;
class MergeTabletResponse;
class QueryRequest;
class QueryResponse;
class WriteTabletRequest;
class WriteTabletResponse;
class ScanTabletRequest;
class ScanTabletResponse;

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

    void GetRollback(const RollbackRequest* request,
                  RollbackResponse* response,
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

    void UpdateCheck(const UpdateCheckRequest* request,
                     UpdateCheckResponse* response,
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

    void ShowTablesBrief(const ShowTablesRequest* request,
                         ShowTablesResponse* response,
                         google::protobuf::Closure* done);

    void ShowTabletNodes(const ShowTabletNodesRequest* request,
                         ShowTabletNodesResponse* response,
                         google::protobuf::Closure* done);

    void RenameTable(const RenameTableRequest* request,
                     RenameTableResponse* response,
                     google::protobuf::Closure* done);

    void CmdCtrl(const CmdCtrlRequest* request,
                 CmdCtrlResponse* response);
    void OperateUser(const OperateUserRequest* request,
                     OperateUserResponse* response,
                     google::protobuf::Closure* done);

    void RefreshTabletNodeList(const std::map<std::string, std::string>& ts_node_list);

    bool SetMasterStatus(const MasterStatus& new_status,
                         MasterStatus* old_status = NULL);
    MasterStatus GetMasterStatus();

    void EnableQueryTabletNodeTimer();
    void DisableQueryTabletNodeTimer();

    bool GetMetaTabletAddr(std::string* addr);
    void TryLoadTablet(TabletPtr tablet, std::string addr = "");

    std::string ProfilingLog();

private:
    typedef Closure<void, SnapshotRequest*, SnapshotResponse*, bool, int> SnapshotClosure;
    typedef Closure<void, SnapshotRollbackRequest*, SnapshotRollbackResponse*, bool, int> RollbackClosure;
    typedef Closure<void, ReleaseSnapshotRequest*, ReleaseSnapshotResponse*, bool, int> DelSnapshotClosure;
    typedef Closure<void, QueryRequest*, QueryResponse*, bool, int> QueryClosure;
    typedef Closure<void, UpdateRequest*, UpdateResponse*, bool, int> UpdateClosure;
    typedef Closure<void, LoadTabletRequest*, LoadTabletResponse*, bool, int> LoadClosure;
    typedef Closure<void, UnloadTabletRequest*, UnloadTabletResponse*, bool, int> UnloadClosure;
    typedef Closure<void, SplitTabletRequest*, SplitTabletResponse*, bool, int> SplitClosure;
    typedef Closure<void, WriteTabletRequest*, WriteTabletResponse*, bool, int> WriteClosure;
    typedef Closure<void, ScanTabletRequest*, ScanTabletResponse*, bool, int> ScanClosure;
    typedef boost::function<void (std::string*, std::string*)> ToMetaFunc;

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
        std::vector<ToMetaFunc> m_meta_entries;
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

    struct RollbackTask {
        const RollbackRequest* request;
        RollbackResponse* response;
        google::protobuf::Closure* done;
        TablePtr table;
        std::vector<TabletPtr> tablets;
        std::vector<uint64_t> rollback_points;
        int task_num;
        int finish_num;
        mutable Mutex mutex;
        bool aborted;
    };

    void SafeModeCmdCtrl(const CmdCtrlRequest* request,
                         CmdCtrlResponse* response);
    void ReloadConfig(CmdCtrlResponse* response);
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
    bool TrySplitTablet(TabletPtr tablet);
    bool TryMergeTablet(TabletPtr tablet);
    void TryMoveTablet(TabletPtr tablet, const std::string& server_addr = "");

    void TryReleaseCache(bool enbaled_debug = false);
    void ReleaseCacheWrapper();
    void EnableReleaseCacheTimer();
    void DisableReleaseCacheTimer();
    void EnableLoadBalance();
    void DisableLoadBalance();

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

    void ScheduleLoadBalance();
    void LoadBalance();
    uint32_t LoadBalance(Scheduler* scheduler,
                         uint32_t max_move_num, uint32_t max_round_num,
                         std::vector<TabletNodePtr>& tabletnode_list,
                         std::vector<TabletPtr>& tablet_list,
                         const std::string& table_name = "");
    bool TabletNodeLoadBalance(TabletNodePtr tabletnode, Scheduler* scheduler,
                               const std::vector<TabletPtr>& tablet_list,
                               const std::string& table_name = "");

    void GetSnapshotAsync(TabletPtr tablet, int64_t snapshot_id, int32_t timeout,
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
    void RollbackAsync(TabletPtr tablet, uint64_t snapshot_id, int32_t timeout,
                          RollbackClosure* done);
    void RollbackCallback(int32_t tablet_id, RollbackTask* task,
                          SnapshotRollbackRequest* master_request,
                          SnapshotRollbackResponse* master_response,
                          bool failed, int error_code);
    void AddRollbackCallback(TablePtr table,
                             std::vector<TabletPtr> tablets,
                             int32_t retry_times,
                             const RollbackRequest* rpc_request,
                             RollbackResponse* rpc_response,
                             google::protobuf::Closure* rpc_done,
                             WriteTabletRequest* request,
                             WriteTabletResponse* response,
                             bool failed, int error_code);

    void ScheduleQueryTabletNode();
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

    void MergeTabletAsync(TabletPtr tablet_p1, TabletPtr tablet_p2);
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

    void BatchWriteMetaTableAsync(ToMetaFunc meta_entry, bool is_delete, WriteClosure* done);
    void BatchWriteMetaTableAsync(std::vector<ToMetaFunc> meta_entries,
                                  bool is_delete, WriteClosure* done);
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
    void AddUserInfoToMetaCallback(UserPtr user_ptr,
                                   int32_t retry_times,
                                   const OperateUserRequest* rpc_request,
                                   OperateUserResponse* rpc_response,
                                   google::protobuf::Closure* rpc_done,
                                   WriteTabletRequest* request,
                                   WriteTabletResponse* response,
                                   bool rpc_failed, int error_code);
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
                                            const TableSchema* schema,
                                            UpdateTableResponse* rpc_response,
                                            google::protobuf::Closure* rpc_done,
                                            WriteTabletRequest* request,
                                            WriteTabletResponse* response,
                                            bool failed, int error_code);
    void UpdateTableRecordForRenameCallback(TablePtr table, int32_t retry_times,
                                            RenameTableResponse* rpc_response,
                                            google::protobuf::Closure* rpc_done,
                                            std::string old_alias,
                                            std::string new_alias,
                                            WriteTabletRequest* request,
                                            WriteTabletResponse* response,
                                            bool failed, int error_code
                                            );
    void UpdateTabletRecordCallback(TabletPtr tablet, int32_t retry_times,
                                    WriteTabletRequest* request,
                                    WriteTabletResponse* response,
                                    bool failed, int error_code);
    void UpdateMetaForLoadCallback(TabletPtr tablet, int32_t retry_times,
                                    WriteTabletRequest* request,
                                    WriteTabletResponse* response,
                                    bool failed, int error_code);
    void DeleteTableCallback(TablePtr table,
                             std::vector<TabletPtr> tablets,
                             int32_t retry_times,
                             DeleteTableResponse* rpc_response,
                             google::protobuf::Closure* rpc_done,
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

    void UpdateSchemaCallback(std::string table_name,
                              std::string tablet_path,
                              std::string start_key,
                              std::string end_key,
                              int32_t retry_times,
                              UpdateRequest* request,
                              UpdateResponse* response,
                              bool rpc_failed, int status_code);
    void NoticeTabletNodeSchemaUpdatedAsync(TabletPtr tablet,
                                            UpdateClosure* done);
    void NoticeTabletNodeSchemaUpdated(TablePtr table);
    void NoticeTabletNodeSchemaUpdated(TabletPtr tablet);

    // load metabale to master memory
    bool LoadMetaTable(const std::string& meta_tablet_addr,
                       StatusCode* ret_status);
    bool LoadMetaTableFromFile(const std::string& filename,
                               StatusCode* ret_status = NULL);
    bool ReadFromStream(std::ifstream& ifs,
                        std::string* key,
                        std::string* value);

    // load metatable on a tabletserver
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

    void SuspendMetaOperation(TablePtr table, const std::vector<TabletPtr>& tablets,
                              bool is_delete, WriteClosure* done);
    void SuspendMetaOperation(ToMetaFunc meta_entry,
                              bool is_delete, WriteClosure* done);
    void SuspendMetaOperation(std::vector<ToMetaFunc> meta_entries,
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
    void ScheduleTabletNodeGc();
    void DoTabletNodeGc();
    void DoTabletNodeGcPhase2();

    bool IsRootUser(const std::string& token);

    bool CheckUserPermissionOnTable(const std::string& token, TablePtr table);

    template <typename Request>
    bool HasPermissionOnTable(const Request* request, TablePtr table);

    template <typename Request, typename Response, typename Callback>
    bool HasPermissionOrReturn(const Request* request, Response* response,
                               Callback* done, TablePtr table, const char* operate);

    void FillAlias(const std::string& key, const std::string& value);
    void RefreshTableCounter();
private:
    mutable Mutex m_status_mutex;
    MasterStatus m_status;
    std::string m_local_addr;

    mutable Mutex m_tabletnode_mutex;
    bool m_restored;
    boost::shared_ptr<TabletManager> m_tablet_manager;
    boost::shared_ptr<TabletNodeManager> m_tabletnode_manager;
    boost::shared_ptr<UserManager> m_user_manager;
    scoped_ptr<MasterZkAdapterBase> m_zk_adapter;
    scoped_ptr<Scheduler> m_size_scheduler;
    scoped_ptr<Scheduler> m_load_scheduler;

    Mutex m_mutex;
    int64_t m_release_cache_timer_id;
    Counter m_this_sequence_id;

    bool m_query_enabled;
    scoped_ptr<ThreadPool> m_query_thread_pool;
    int64_t m_start_query_time;
    int64_t m_query_tabletnode_timer_id;
    Counter m_query_pending_count;

    bool m_load_balance_enabled;
    int64_t m_load_balance_timer_id;

    scoped_ptr<ThreadPool> m_thread_pool;
    AutoResetEvent m_query_event;

    mutable Mutex m_meta_task_mutex;
    std::queue<MetaTask*> m_meta_task_queue;

    mutable Mutex m_tabletnode_timer_mutex;
    std::map<std::string, int64_t> m_tabletnode_timer_id_map;

    mutable Mutex m_tablet_mutex;

    TabletPtr m_meta_tablet;

    // stat table
    bool m_is_stat_table;
    std::map<std::string, int64_t> m_ts_stat_update_time;
    mutable Mutex m_stat_table_mutex;
    TableImpl* m_stat_table;

    // tabletnode garbage clean
    bool m_gc_enabled;
    int64_t m_gc_timer_id;
    bool m_gc_query_enable;
    boost::shared_ptr<GcStrategy> gc_strategy;
    std::map<std::string, std::string> m_alias;
    mutable Mutex m_alias_mutex;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_MASTER_IMPL_H_
