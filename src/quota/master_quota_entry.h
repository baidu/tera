// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#pragma once

#include <deque>
#include <memory>
#include <unordered_map>
#include <vector>

#include "access/helpers/version_recorder.h"
#include "common/base/bounded_queue.h"
#include "common/metric/metric_counter.h"
#include "common/mutex.h"
#include "proto/master_rpc.pb.h"
#include "proto/quota.pb.h"
#include "proto/tabletnode_rpc.pb.h"
#include "quota/helpers/master_quota_helper.h"
#include "quota/helpers/quota_utils.h"
#include "master/tabletnode_manager.h"

namespace tera {

namespace master {
class TabletManager;
}

namespace quota {

enum class QuotaUpdateStatus { WaitUpdate, Updating, FinishUpdated };

// map<table, TableQuota>
using TableQuotaList = std::unordered_map<std::string, std::unique_ptr<TableQuota>>;

// map<ts_addr, map<table, TableQuota>>
using TsTableQuotaList = std::unordered_map<std::string, TableQuotaList>;

// map<ts_addr, map<table, TableQuota>>
using TsDfsQuotaList = std::unordered_map<std::string, uint64_t>;

class MasterQuotaEntry {
 public:
  MasterQuotaEntry()
      : quota_update_status_(QuotaUpdateStatus::FinishUpdated),
        flow_control_slowdown_ratio_(-1),
        cluster_dfs_write_bytes_quota_{
            "dfs_write_bytes_threshold", {tera::Subscriber::SubscriberType::LATEST}, false},
        cluster_dfs_qps_quota_{
            "dfs_qps_threshold", {tera::Subscriber::SubscriberType::LATEST}, false},
        cluster_dfs_qps_queue_(kDfsQueueBoundSize),
        cluster_dfs_write_bytes_queue_(kDfsQueueBoundSize),
        dfs_write_bytes_hard_limit_(-1),
        dfs_read_bytes_hard_limit_(-1) {
    version_recorder_.IncVersion();
  }
  virtual ~MasterQuotaEntry() {}

  MasterQuotaEntry(MasterQuotaEntry&) = delete;
  MasterQuotaEntry& operator=(const MasterQuotaEntry&) = delete;

  // master
  void SetTabletManager(const std::shared_ptr<master::TabletManager>& tablet_manager);
  void SetTabletNodeManager(const std::shared_ptr<master::TabletNodeManager>& tabletnode_manager);

  // used by quota adjust by manual
  bool AddRecord(const std::string& key, const std::string& value);
  bool DelRecord(const std::string& key);

  void BuildReq(QueryRequest* request, const std::string& ts_addr);
  bool IsSameVersion(uint64_t version) { return version_recorder_.IsSameVersion(version); }

  // Aim to make sure NEED to sync version from master to ts or NOT
  // If pass true then means NEED to sync, false then NOT NEED to sync.
  void SyncVersion(bool updated) { version_recorder_.SetNeedUpdate(updated); }

  // Caculate delta table_quotas, used by split/merge/move
  void CaculateDeltaQuota(const std::string& table_name);

  // Only clear delta list after query sync success all ts,
  // and quota_update_status_ still keep Updating.
  // If not QuotaUpdateStatus::Updating, it means delta list modified in query dispatch,
  // so need to re-dispatch again.
  bool ClearDeltaQuota();

  void RefreshClusterFlowControlStatus();

  bool GetTableQuota(const std::string& table_name, TableQuota* table_quota);

  void ShowQuotaInfo(ShowQuotaResponse* response, bool brief_show);

  void SetDfsWriteSizeQuota(int64_t quota) { cluster_dfs_write_bytes_quota_.Set(quota); }
  void SetDfsQpsQuota(int64_t quota) { cluster_dfs_qps_quota_.Set(quota); }

  void SetDfsWriteThroughputHardLimit(int64_t quota) { dfs_write_bytes_hard_limit_.store(quota); }
  void SetDfsReadThroughputHardLimit(int64_t quota) { dfs_read_bytes_hard_limit_.store(quota); }
  int64_t GetDfsWriteThroughputHardLimit() { return dfs_write_bytes_hard_limit_.load(); }
  int64_t GetDfsReadThroughputHardLimit() { return dfs_read_bytes_hard_limit_.load(); }
  void RefreshDfsHardLimit();

 private:
  void AddDeltaQuota(const std::string& table_name, TsTableQuotaList* delta_ts_table_quotas_list);
  // Clear ts table quota setting while table drop
  void AddDeltaDropQuota(const std::string& table_name,
                         TsTableQuotaList* delta_ts_table_quotas_list);
  void SwitchWaitingUpdateStatus();
  void PrepareUpdate();
  std::string DebugPrintDeltaTableList();
  std::string DebugPrintTableQuotaList();

  void SetSlowdownWriteRatio(double slowdown_write_ratio) {
    flow_control_slowdown_ratio_.store(slowdown_write_ratio);
  }

  void ResetSlowdownWriteRatio() { flow_control_slowdown_ratio_.store(-1); }

  bool SlowdownModeTriggered() const { return flow_control_slowdown_ratio_ > 0; }

  void UpdateDfsWriteBytesQueue();
  void UpdateDfsQpsQueue();
  void RefreshDfsWriteThroughputHardLimit(const std::vector<master::TabletNodePtr>&);
  void RefreshDfsReadThroughputHardLimit(const std::vector<master::TabletNodePtr>&);

  int64_t GetClusterDfsWriteSize(const std::vector<master::TabletNodePtr>&);
  int64_t GetClusterDfsReadSize(const std::vector<master::TabletNodePtr>&);

 private:
  TableQuotaList table_quotas_list_;

  // Only clear in all update success
  TsTableQuotaList delta_ts_table_quotas_list_;
  QuotaUpdateStatus quota_update_status_;

  mutable Mutex mutex_;
  std::shared_ptr<master::TabletManager> tablet_manager_;
  std::shared_ptr<master::TabletNodeManager> tabletnode_manager_;

  auth::VersionRecorder version_recorder_;

  std::atomic<double> flow_control_slowdown_ratio_;
  // Dfs quota, which will trigger user write slowdown when exceed.
  MetricCounter cluster_dfs_write_bytes_quota_;
  MetricCounter cluster_dfs_qps_quota_;

  common::BoundedQueue<int64_t> cluster_dfs_qps_queue_;
  common::BoundedQueue<int64_t> cluster_dfs_write_bytes_queue_;
  static constexpr size_t kDfsQueueBoundSize = 10;

  TsDfsQuotaList dfs_write_throughput_hard_limit_list_;
  TsDfsQuotaList dfs_read_throughput_hard_limit_list_;

  // Dfs read/write hard limit. It's set by tera client manually and just keep in memory.
  // For solving dfs write/read throughput snowslide online.
  // -1 means not enabled.
  std::atomic<int64_t> dfs_write_bytes_hard_limit_;
  std::atomic<int64_t> dfs_read_bytes_hard_limit_;
};
}  // namespace quota
}  // namespace tera