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

#include "master/abnormal_node_mgr.h"
#include "master/availability.h"
#include "master/master_state_machine.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"
#include "master/user_manager.h"
#include "proto/master_rpc.pb.h"
#include "proto/table_meta.pb.h"
#include "sdk/client_impl.h"
#include "sdk/stat_table.h"
#include "sdk/table_impl.h"

#include "access/access_entry.h"
#include "access/access_builder.h"

#include "quota/master_quota_entry.h"

#include "tablet_state_machine.h"
#include "procedure_executor.h"

DECLARE_int32(tera_master_impl_retry_times);
DECLARE_bool(tera_acl_enabled);

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
class TeraMasterEnv;

class MasterImpl {
  friend class TeraMasterEnv;

 public:
  MasterImpl(const std::shared_ptr<auth::AccessEntry>& access_entry,
             const std::shared_ptr<quota::MasterQuotaEntry>& quota_entry);
  virtual ~MasterImpl();

  bool Init();

  bool Restore(const std::map<std::string, std::string>& tabletnode_list);

  void CreateTable(const CreateTableRequest* request, CreateTableResponse* response,
                   google::protobuf::Closure* done);

  void DeleteTable(const DeleteTableRequest* request, DeleteTableResponse* response,
                   google::protobuf::Closure* done);

  void DisableTable(const DisableTableRequest* request, DisableTableResponse* response,
                    google::protobuf::Closure* done);

  void EnableTable(const EnableTableRequest* request, EnableTableResponse* response,
                   google::protobuf::Closure* done);

  void UpdateTable(const UpdateTableRequest* request, UpdateTableResponse* response,
                   google::protobuf::Closure* done);

  void UpdateCheck(const UpdateCheckRequest* request, UpdateCheckResponse* response,
                   google::protobuf::Closure* done);

  void SearchTable(const SearchTableRequest* request, SearchTableResponse* response,
                   google::protobuf::Closure* done);

  void ShowTables(const ShowTablesRequest* request, ShowTablesResponse* response,
                  google::protobuf::Closure* done);

  void ShowTablesBrief(const ShowTablesRequest* request, ShowTablesResponse* response,
                       google::protobuf::Closure* done);

  void ShowTabletNodes(const ShowTabletNodesRequest* request, ShowTabletNodesResponse* response,
                       google::protobuf::Closure* done);

  void CmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void OperateUser(const OperateUserRequest* request, OperateUserResponse* response,
                   google::protobuf::Closure* done);

  void RefreshTabletNodeList(const std::map<std::string, std::string>& ts_node_list);

  bool DoStateTransition(const MasterEvent, MasterStatus* old_status);

  bool DoStateTransition(const MasterEvent event);

  bool IsInSafeMode();

  MasterStatus GetMasterStatus();
  std::shared_ptr<auth::AccessEntry> GetAccessEntry() { return access_entry_; }

  void EnableQueryTabletNodeTimer();
  void DisableQueryTabletNodeTimer();

  bool GetMetaTabletAddr(std::string* addr);

  bool TryKickTabletNode(TabletNodePtr node);

  bool TryKickTabletNode(const std::string& tabletnode_addr);

  std::string ProfilingLog();

  bool IsRootUser(const std::string& token);

  template <typename Request>
  bool HasPermission(const Request* request, TablePtr table, const char* operate) {
    if (!FLAGS_tera_acl_enabled || IsRootUser(request->user_token()) ||
        ((table->GetSchema().admin_group() == "") && (table->GetSchema().admin() == "")) ||
        (request->has_user_token() && CheckUserPermissionOnTable(request->user_token(), table))) {
      return true;
    } else {
      std::string token = request->has_user_token() ? request->user_token() : "";
      LOG(WARNING) << "[acl]" << user_manager_->TokenToUserName(token) << ":" << token << "fail to "
                   << operate;
      return false;
    }
  }

 private:
  typedef std::function<void(QueryRequest*, QueryResponse*, bool, int)> QueryClosure;
  typedef std::function<void(UpdateRequest*, UpdateResponse*, bool, int)> UpdateClosure;

  void SafeModeCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void ReloadConfig(CmdCtrlResponse* response);
  void KickTabletNodeCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void TabletCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void TableCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void MetaCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void DfsHardLimitCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);
  void ProcedureLimitCmdCtrl(const CmdCtrlRequest* request, CmdCtrlResponse* response);

  bool LoadTabletSync(const TabletMeta& meta, const TableSchema& schema, StatusCode* status);
  bool UnloadTabletSync(const std::string& table_name, const std::string& key_start,
                        const std::string& key_end, const std::string& server_addr,
                        StatusCode* status);

  void TryReleaseCache(bool enbaled_debug = false);
  void ReleaseCacheWrapper();
  void EnableReleaseCacheTimer();
  void DisableReleaseCacheTimer();
  void EnableLoadBalance();
  void DisableLoadBalance();

  void InitAsync();

  bool CreateAndLoadTable(const std::string& table_name, bool compress, StoreMedium store,
                          StatusCode* status);

  bool RemoveTablet(const TabletMeta& meta, StatusCode* status);

  void ScheduleLoadBalance();
  void LoadBalance();
  uint32_t LoadBalance(Scheduler* scheduler, uint32_t max_move_num, uint32_t max_round_num,
                       std::vector<TabletNodePtr>& tabletnode_list,
                       std::vector<TabletPtr>& tablet_list, const std::string& table_name = "");
  bool TabletNodeLoadBalance(TabletNodePtr tabletnode, Scheduler* scheduler,
                             const std::vector<TabletPtr>& tablet_list,
                             const std::string& table_name = "");

  void ScheduleQueryTabletNode();
  void QueryTabletNode();
  void QueryTabletNodeAsync(std::string addr, int32_t timeout, bool is_gc, QueryClosure done);

  void QueryTabletNodeCallback(std::string addr, QueryRequest* req, QueryResponse* res, bool failed,
                               int error_code);
  void CollectTabletInfoCallback(std::string addr, std::vector<TabletMeta>* tablet_list,
                                 sem_t* finish_counter, Mutex* mutex, QueryRequest* request,
                                 QueryResponse* response, bool failed, int error_code);
  void RetryCollectTabletInfo(std::string addr, std::vector<TabletMeta>* tablet_list,
                              sem_t* finish_counter, Mutex* mutex);

  void AddUserInfoToMetaCallback(UserPtr user_ptr, const OperateUserRequest* rpc_request,
                                 OperateUserResponse* rpc_response,
                                 google::protobuf::Closure* rpc_done, bool succ);

  void UpdateTableRecordForEnableCallback(TablePtr table, EnableTableResponse* rpc_response,
                                          google::protobuf::Closure* rpc_done, bool succ);

  void UpdateTableRecordForUpdateCallback(TablePtr table, UpdateTableResponse* rpc_response,
                                          google::protobuf::Closure* rpc_done, bool succ);

  void DisableAllTablets(TablePtr table);

  void UpdateSchemaCallback(std::string table_name, std::string tablet_path, std::string start_key,
                            std::string end_key, int32_t retry_times, UpdateRequest* request,
                            UpdateResponse* response, bool rpc_failed, int status_code);
  void NoticeTabletNodeSchemaUpdatedAsync(TabletPtr tablet, UpdateClosure done);
  void NoticeTabletNodeSchemaUpdated(TablePtr table);
  void NoticeTabletNodeSchemaUpdated(TabletPtr tablet);

  // load metabale to master memory
  bool LoadMetaTable(const std::string& meta_tablet_addr, StatusCode* ret_status);
  bool LoadMetaTableFromFile(const std::string& filename, StatusCode* ret_status = NULL);
  bool ReadFromStream(std::ifstream& ifs, std::string* key, std::string* value);

  // load metatable on a tabletserver
  bool LoadMetaTablet(std::string* server_addr);
  void UnloadMetaTablet(const std::string& server_addr);
  void RestartTabletNode(const std::string& addr, const std::string& uuid);

  void AddTabletNode(const std::string& tabletnode_addr, const std::string& tabletnode_id);
  void DeleteTabletNode(const std::string& tabletnode_addr, const std::string& uuid);

  void MoveTabletOnDeadTabletNode(const std::vector<TabletPtr>& tablet_list,
                                  TabletNodePtr dead_node);

  bool TryEnterSafeMode();

  void TryLeaveSafeMode();
  bool EnterSafeMode(const MasterEvent event, StatusCode* status = NULL);
  bool LeaveSafeMode(const MasterEvent event, StatusCode* status = NULL);

  void SetSafeModeTTLTask(int64_t delay_minute);
  void CancelSafeModeTTLTask();

  void TryMovePendingTablet(TabletPtr tablet);
  double LiveNodeTabletRatio();
  void LoadAllDeadNodeTablets();

  void CollectAllTabletInfo(const std::map<std::string, std::string>& tabletnode_list,
                            std::vector<TabletMeta>* tablet_list);
  bool RestoreMetaTablet(const std::vector<TabletMeta>& tablet_list);

  void RestoreUserTablet(const std::vector<TabletMeta>& report_tablet_list);

  // garbage clean
  void EnableGcTrashCleanTimer();
  void DisableGcTrashCleanTimer();
  void ScheduleGcTrashClean();
  void DoGcTrashClean();
  void EnableTabletNodeGcTimer();
  void DisableTabletNodeGcTimer();
  void ScheduleTabletNodeGc();
  void DoTabletNodeGc();
  void DoTabletNodeGcPhase2();

  bool CheckUserPermissionOnTable(const std::string& token, TablePtr table);

  void RefreshTableCounter();

  void DoAvailableCheck();
  void ScheduleAvailableCheck();
  void EnableAvailabilityCheck();
  void DeleteTablet(TabletPtr tablet);
  void CopyTableMetaToUser(TablePtr table, TableMeta* meta_ptr);

  void ScheduleDelayAddNode();
  void DoDelayAddNode();

  bool TryLoadTablet(TabletPtr tablet, TabletNodePtr node = TabletNodePtr(nullptr));

  bool TryUnloadTablet(TabletPtr tablet);

  bool TryMoveTablet(TabletPtr tablet, TabletNodePtr node = TabletNodePtr(nullptr));

  bool TryMergeTablet(TabletPtr tablet);

  bool TrySplitTablet(TabletPtr tablet, std::string split_key = "");

 private:
  mutable Mutex status_mutex_;
  MasterStateMachine state_machine_;

  // MasterStatus status_;
  std::string local_addr_;

  std::shared_ptr<ThreadPool> thread_pool_;

  mutable Mutex tabletnode_mutex_;
  bool restored_;
  std::shared_ptr<TabletManager> tablet_manager_;
  std::shared_ptr<TabletNodeManager> tabletnode_manager_;
  std::shared_ptr<UserManager> user_manager_;
  std::shared_ptr<MasterZkAdapterBase> zk_adapter_;
  std::shared_ptr<Scheduler> size_scheduler_;
  std::shared_ptr<Scheduler> load_scheduler_;

  Mutex mutex_;
  int64_t release_cache_timer_id_;
  Counter this_sequence_id_;

  bool query_enabled_;
  scoped_ptr<ThreadPool> query_thread_pool_;
  int64_t start_query_time_;
  int64_t query_tabletnode_timer_id_;
  Counter query_pending_count_;
  Counter update_auth_pending_count_;
  Counter update_quota_pending_count_;

  bool load_balance_scheduled_;
  bool load_balance_enabled_;

  mutable Mutex tabletnode_timer_mutex_;
  std::map<std::string, int64_t> tabletnode_timer_id_map_;

  mutable Mutex tablet_mutex_;

  MetaTabletPtr meta_tablet_;

  std::mutex kick_mutex_;

  // stat table
  std::shared_ptr<tera::sdk::StatTable> stat_table_;

  // tabletnode garbage clean
  bool gc_trash_clean_enabled_;
  int64_t gc_trash_clean_timer_id_;
  bool gc_enabled_;
  int64_t gc_timer_id_;
  bool gc_query_enable_;

  std::shared_ptr<ProcedureExecutor> executor_;
  std::shared_ptr<TabletAvailability> tablet_availability_;

  std::shared_ptr<auth::AccessEntry> access_entry_;
  std::shared_ptr<auth::AccessBuilder> access_builder_;
  std::shared_ptr<quota::MasterQuotaEntry> quota_entry_;
  std::unique_ptr<AbnormalNodeMgr> abnormal_node_mgr_;
  int64_t running_guard_timestamp_ = 0;
  int64_t safemode_ttl_taskid_ = -1;
};

}  // namespace master
}  // namespace tera

#endif  // TERA_MASTER_MASTER_IMPL_H_
