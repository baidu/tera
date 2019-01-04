// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_TABLETNODE_TABLETNODE_IMPL_H_
#define TERA_TABLETNODE_TABLETNODE_IMPL_H_

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>

#include "common/base/scoped_ptr.h"
#include "common/event.h"
#include "common/metric/collector_report_publisher.h"
#include "common/metric/metric_counter.h"
#include "common/thread_pool.h"

#include "io/tablet_io.h"
#include "proto/master_rpc.pb.h"
#include "proto/tabletnode.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "sdk/stat_table.h"
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
    std::shared_ptr<Counter> row_done_counter;

    const WriteTabletRequest* request;
    WriteTabletResponse* response;
    google::protobuf::Closure* done;
    WriteRpcTimer* timer;

    WriteTabletTask(const WriteTabletRequest* req, WriteTabletResponse* resp,
                    google::protobuf::Closure* d, WriteRpcTimer* t, std::shared_ptr<Counter> c)
        : row_done_counter(c), request(req), response(resp), done(d), timer(t) {}
  };

  TabletNodeImpl();
  ~TabletNodeImpl();

  bool Init();

  bool Exit();

  void GarbageCollect();
  void PersistentCacheGarbageCollect(const std::set<std::string>& inherited_files,
                                     const std::set<std::string>& active_tablets);

  StatusCode QueryTabletStatus(const std::string& table_name, const std::string& key_start,
                               const std::string& key_end);

  void LoadTablet(const LoadTabletRequest* request, LoadTabletResponse* response);

  bool UnloadTablet(const std::string& tablet_name, const std::string& start,
                    const std::string& end, StatusCode* status);

  void UnloadTablet(const UnloadTabletRequest* request, UnloadTabletResponse* response);

  void CompactTablet(const CompactTabletRequest* request, CompactTabletResponse* response,
                     google::protobuf::Closure* done);

  void Update(const UpdateRequest* request, UpdateResponse* response,
              google::protobuf::Closure* done);

  void ReadTablet(int64_t start_micros, const ReadTabletRequest* request,
                  ReadTabletResponse* response, google::protobuf::Closure* done,
                  ThreadPool* read_thread_pool);

  void WriteTablet(const WriteTabletRequest* request, WriteTabletResponse* response,
                   google::protobuf::Closure* done, WriteRpcTimer* timer = NULL);

  void ScanTablet(const ScanTabletRequest* request, ScanTabletResponse* response,
                  google::protobuf::Closure* done);

  void CmdCtrl(const TsCmdCtrlRequest* request, TsCmdCtrlResponse* response,
               google::protobuf::Closure* done);

  void Query(const QueryRequest* request, QueryResponse* response, google::protobuf::Closure* done);

  void ComputeSplitKey(const SplitTabletRequest* request, SplitTabletResponse* response,
                       google::protobuf::Closure* done);

  void EnterSafeMode();
  void LeaveSafeMode();
  void ExitService();

  void SetTabletNodeStatus(const TabletNodeStatus& status);
  TabletNodeStatus GetTabletNodeStatus();

  void SetRootTabletAddr(const std::string& root_tablet_addr);

  void SetSessionId(const std::string& session_id);
  std::string GetSessionId();

  TabletNodeSysInfo& GetSysInfo();

  void RefreshAndDumpSysInfo();

  void GetBackgroundErrors(std::vector<tera::TabletBackgroundErrorInfo>* background_errors);

  void TryReleaseMallocCache();

  void RefreshLevelSize();

 private:
  // call this when fail to write TabletIO
  void WriteTabletFail(WriteTabletTask* tablet_task, StatusCode status);

  // write callback for TabletIO::Write()
  void WriteTabletCallback(WriteTabletTask* tablet_task,
                           std::vector<const RowMutationSequence*>* row_mutation_vec,
                           std::vector<StatusCode>* status_vec);

  bool CheckInKeyRange(const KeyList& key_list, const std::string& key_start,
                       const std::string& key_end);
  bool CheckInKeyRange(const KeyValueList& pair_list, const std::string& key_start,
                       const std::string& key_end);
  bool CheckInKeyRange(const RowMutationList& row_list, const std::string& key_start,
                       const std::string& key_end);
  bool CheckInKeyRange(const RowReaderList& reader_list, const std::string& key_start,
                       const std::string& key_end);

  bool InitCacheSystem();
  void InitDfsReadThreadLimiter();

  void ReleaseMallocCache();
  void EnableReleaseMallocCacheTimer(int32_t expand_factor = 1);
  void DisableReleaseMallocCacheTimer();

  void RefreshTabletsStatus();

  void GetInheritedLiveFiles(std::vector<TabletInheritedFileInfo>* inherited);
  void GetInheritedLiveFiles(std::vector<InheritedLiveFiles>& inherited);

  void GarbageCollectInPath(const std::string& path, leveldb::Env* env,
                            const std::set<std::string>& inherited_files,
                            const std::set<std::string>& active_tablets);

  bool ApplySchema(const UpdateRequest* request);

  void UnloadTabletProc(io::TabletIO* tablet_io, Counter* worker_count);

 private:
  mutable Mutex status_mutex_;
  TabletNodeStatus status_;
  Mutex mutex_;
  bool running_;

  std::shared_ptr<TabletManager> tablet_manager_;
  scoped_ptr<TabletNodeZkAdapterBase> zk_adapter_;

  uint64_t this_sequence_id_;
  std::string local_addr_;
  std::string root_tablet_addr_;
  std::string session_id_;
  int64_t release_cache_timer_id_;

  TabletNodeSysInfo sysinfo_;
  std::vector<MetricCounter> level_size_;

  // do some tablets health check with a timer
  std::thread tablet_healthcheck_thread_;
  // Exit() called should set this event
  common::AutoResetEvent exit_event_;

  scoped_ptr<ThreadPool> thread_pool_;

  leveldb::Logger* ldb_logger_;
  leveldb::Cache* ldb_block_cache_;
  leveldb::Cache* m_memory_cache;
  leveldb::TableCache* ldb_table_cache_;

  // metric for caches
  struct CacheMetrics {
    tera::AutoCollectorRegister block_cache_hitrate_;
    tera::AutoCollectorRegister block_cache_entries_;
    tera::AutoCollectorRegister block_cache_charge_;

    tera::AutoCollectorRegister table_cache_hitrate_;
    tera::AutoCollectorRegister table_cache_entries_;
    tera::AutoCollectorRegister table_cache_charge_;

    CacheMetrics(leveldb::Cache* block_cache, leveldb::TableCache* table_cache);
  };

  scoped_ptr<CacheMetrics> cache_metrics_;
  scoped_ptr<tera::AutoCollectorRegister> snappy_ratio_metric_;
  // persistent cache's garbage files will be delayed for one gc period before remove.
  std::unordered_set<std::string> delayed_gc_files_;
};

class ReadTabletTask : public std::enable_shared_from_this<ReadTabletTask> {
 public:
  using RowResults = std::vector<std::shared_ptr<RowResult>>;
  ReadTabletTask(int64_t start_micros, std::shared_ptr<TabletManager> tablet_manager,
                 const ReadTabletRequest* request, ReadTabletResponse* response,
                 google::protobuf::Closure* done, ThreadPool* read_thread_pool);

  void StartRead();

 private:
  struct ShardRequest {
    int64_t offset;
    int64_t row_num;
    RowResults* row_results;
    ShardRequest(int64_t off, int64_t r_num, RowResults* r_results)
        : offset(off), row_num(r_num), row_results(r_results) {}
  };

  void DoRead(std::shared_ptr<ShardRequest> shard_req);
  void FinishShardRequest(const std::shared_ptr<ShardRequest>& shard_req);

 private:
  std::shared_ptr<TabletManager> tablet_manager_;
  const ReadTabletRequest* request_;
  ReadTabletResponse* response_;
  google::protobuf::Closure* done_;
  ThreadPool* read_thread_pool_;

  Counter finished_;
  Counter read_success_num_;
  std::atomic<bool> has_timeout_{false};

  int64_t end_time_ms_;
  uint64_t snapshot_id_;
  int32_t total_row_num_;

  std::vector<RowResults> row_results_list_;
};

}  // namespace tabletnode
}  // namespace tera

#endif  // TERA_TABLETNODE_TABLETNODE_IMPL_H_
