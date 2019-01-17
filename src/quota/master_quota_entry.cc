// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <cmath>
#include <glog/logging.h>
#include <limits>
#include <numeric>
#include <sstream>

#include "common/metric/metric_counter.h"
#include "master/tablet_manager.h"
#include "quota/master_quota_entry.h"

namespace tera {
namespace quota {

tera::MetricCounter flow_control_mode{
    "tera_master_enter_flow_control_mode", {tera::SubscriberType::LATEST}, false};

void MasterQuotaEntry::SwitchWaitingUpdateStatus() {
  quota_update_status_ = QuotaUpdateStatus::WaitUpdate;
  PrepareUpdate();
}

void MasterQuotaEntry::PrepareUpdate() {
  version_recorder_.IncVersion();
  version_recorder_.SetNeedUpdate(true);
}

bool MasterQuotaEntry::AddRecord(const std::string& key, const std::string& value) {
  std::string table_name = MasterQuotaHelper::GetTableNameFromMetaKey(key);
  if ("" == table_name) {
    VLOG(25) << "wrong quota key[" << key << "] format, value[" << value << "]";
    return false;
  }

  std::unique_ptr<TableQuota> table_quota(MasterQuotaHelper::NewTableQuotaFromMetaValue(value));
  if (!table_quota) {
    VLOG(25) << "failed to get table_quota in meta value";
    return false;
  }
  {
    MutexLock l(&mutex_);
    if (!tablet_manager_) {
      return false;
    }
    bool found_delta = false;
    auto it = table_quotas_list_.find(table_name);
    if (it == table_quotas_list_.end()) {
      table_quotas_list_.emplace(table_name, std::move(table_quota));
      VLOG(25) << "quota insert table_quotas_list_ table[" << table_name << "]";
      found_delta = true;
    } else {
      // set update if different, and inc vesion
      if (MasterQuotaHelper::MergeTableQuota(*table_quota, table_quotas_list_[table_name].get())) {
        VLOG(25) << "quota merge table_quotas_list_ table[" << table_name << "]";
        found_delta = true;
      }
    }
    if (found_delta) {
      AddDeltaQuota(table_name, &delta_ts_table_quotas_list_);
      SwitchWaitingUpdateStatus();
      VLOG(25) << "AddRecord cause quota update, version = " << version_recorder_.GetVersion();
    }
    VLOG(25) << DebugPrintTableQuotaList();
    VLOG(25) << DebugPrintDeltaTableList();
  }
  return true;
}

bool MasterQuotaEntry::DelRecord(const std::string& table_name) {
  // Delete table quota while table drop
  MutexLock l(&mutex_);
  auto it = table_quotas_list_.find(table_name);
  if (it != table_quotas_list_.end()) {
    table_quotas_list_.erase(it);
    AddDeltaDropQuota(table_name, &delta_ts_table_quotas_list_);
    SwitchWaitingUpdateStatus();
    VLOG(25) << "DelRecord cause quota update, version = " << version_recorder_.GetVersion();
    return true;
  }
  return false;
}

void MasterQuotaEntry::BuildReq(QueryRequest* request, const std::string& ts_addr) {
  MutexLock l(&mutex_);
  if (version_recorder_.NeedUpdate()) {
    auto ts_addr_it = delta_ts_table_quotas_list_.find(ts_addr);
    if (ts_addr_it != delta_ts_table_quotas_list_.end()) {
      for (auto table_quota_it = ts_addr_it->second.begin();
           table_quota_it != ts_addr_it->second.end(); ++table_quota_it) {
        TableQuota* table_quota = request->add_table_quotas();
        table_quota->CopyFrom(*(table_quota_it->second));
      }
    }
    if (SlowdownModeTriggered()) {
      request->set_slowdown_write_ratio(flow_control_slowdown_ratio_.load());
    }

    auto write_hard_limit_iter = dfs_write_throughput_hard_limit_list_.find(ts_addr);
    if (write_hard_limit_iter != dfs_write_throughput_hard_limit_list_.end()) {
      request->set_dfs_write_throughput_hard_limit(write_hard_limit_iter->second);
    }

    auto read_hard_limit_iter = dfs_read_throughput_hard_limit_list_.find(ts_addr);
    if (read_hard_limit_iter != dfs_read_throughput_hard_limit_list_.end()) {
      request->set_dfs_read_throughput_hard_limit(read_hard_limit_iter->second);
    }

    request->set_quota_version(version_recorder_.GetVersion());
    VLOG(25) << "BuildReq for Quota, version = " << version_recorder_.GetVersion();
    quota_update_status_ = QuotaUpdateStatus::Updating;
  }
}

void MasterQuotaEntry::ShowQuotaInfo(ShowQuotaResponse* response, bool brief_show) {
  MutexLock l(&mutex_);
  if (brief_show) {
    // show TableQuota
    for (auto it = table_quotas_list_.begin(); it != table_quotas_list_.end(); ++it) {
      TableQuota* table_quota = response->add_table_quota_list();
      table_quota->CopyFrom(*(it->second));
    }
  } else {
    // show TsQuota
    TsTableQuotaList delta_ts_table_quotas_list;
    for (auto it = table_quotas_list_.begin(); it != table_quotas_list_.end(); ++it) {
      AddDeltaQuota(it->first, &delta_ts_table_quotas_list);
    }
    for (auto ts_addr_it = delta_ts_table_quotas_list.begin();
         ts_addr_it != delta_ts_table_quotas_list.end(); ++ts_addr_it) {
      TsQuota* ts_quota = response->add_ts_quota_list();
      ts_quota->set_ts_addr(ts_addr_it->first);
      for (auto table_quota_it = ts_addr_it->second.begin();
           table_quota_it != ts_addr_it->second.end(); ++table_quota_it) {
        TableQuota* table_quota = ts_quota->add_table_quotas();
        table_quota->CopyFrom(*table_quota_it->second);
      }
    }
  }
}

void MasterQuotaEntry::SetTabletManager(
    const std::shared_ptr<master::TabletManager>& tablet_manager) {
  MutexLock l(&mutex_);
  tablet_manager_ = tablet_manager;
}

void MasterQuotaEntry::CaculateDeltaQuota(const std::string& table_name) {
  MutexLock l(&mutex_);
  auto it = table_quotas_list_.find(table_name);
  if (it == table_quotas_list_.end()) {
    return;
  }
  AddDeltaQuota(table_name, &delta_ts_table_quotas_list_);
  SwitchWaitingUpdateStatus();
  VLOG(25) << "CaculateDeltaQuota cause quota update, version = " << version_recorder_.GetVersion();
}

bool MasterQuotaEntry::ClearDeltaQuota() {
  MutexLock l(&mutex_);
  if (quota_update_status_ == QuotaUpdateStatus::Updating) {
    quota_update_status_ = QuotaUpdateStatus::FinishUpdated;
    delta_ts_table_quotas_list_.clear();
    return true;
  }
  return false;
}

bool MasterQuotaEntry::GetTableQuota(const std::string& table_name, TableQuota* table_quota) {
  MutexLock l(&mutex_);
  auto iter = table_quotas_list_.find(table_name);
  if (iter == table_quotas_list_.end()) {
    return false;
  }
  table_quota->CopyFrom(*iter->second);
  return true;
}

void MasterQuotaEntry::AddDeltaDropQuota(const std::string& table_name,
                                         TsTableQuotaList* delta_ts_table_quotas_list) {
  mutex_.AssertHeld();
  master::TablePtr table_ptr;
  if (!tablet_manager_ || !tablet_manager_->FindTable(table_name, &table_ptr) || !table_ptr) {
    VLOG(25) << "quota AddDeltaQuota couldn't find table[" << table_name << "] in tablet_manager_";
    return;
  }
  std::map<std::string, int64_t> ts_addr_tablets_count;
  table_ptr->GetTsAddrTabletsCount(&ts_addr_tablets_count);
  for (auto it = ts_addr_tablets_count.begin(); it != ts_addr_tablets_count.end(); ++it) {
    std::unique_ptr<TableQuota> new_tablet_quota(new TableQuota);
    new_tablet_quota->set_table_name(table_name);
    new_tablet_quota->set_type(TableQuota::kDelQuota);
    TableQuotaList& table_quota_list = (*delta_ts_table_quotas_list)[it->first];
    table_quota_list[table_name].reset(new_tablet_quota.release());
    VLOG(7) << "del quota " << it->first << " for table " << table_name;
  }
}

void MasterQuotaEntry::AddDeltaQuota(const std::string& table_name,
                                     TsTableQuotaList* delta_ts_table_quotas_list) {
  mutex_.AssertHeld();
  int64_t tablets_count = 1;
  master::TablePtr table_ptr;
  if (!tablet_manager_ || !tablet_manager_->FindTable(table_name, &table_ptr) || !table_ptr) {
    VLOG(25) << "quota AddDeltaQuota couldn't find table[" << table_name << "] in tablet_manager_";
    return;
  }
  tablets_count = table_ptr->GetTabletsCount();
  VLOG(25) << "quota AddDeltaQuota table[" << table_name << "] tablets_count[" << tablets_count
           << "]";

  std::unique_ptr<TableQuota> tablet_quota(new TableQuota);
  tablet_quota->CopyFrom(*table_quotas_list_[table_name]);
  int quota_infos_size = tablet_quota->quota_infos_size();
  for (int quota_info_index = 0; quota_info_index < quota_infos_size; ++quota_info_index) {
    QuotaInfo* quota_info = tablet_quota->mutable_quota_infos(quota_info_index);
    if (quota_info->limit() > 0) {
      quota_info->set_limit(std::ceil(quota_info->limit() / static_cast<double>(tablets_count)));
    }
  }

  // Caclulate the sum quota in ts addr
  // 1. get tables count in ts addr
  // 2. multiply quota for each table's tablets_count in addr
  // 3. set result to delta_list which prepare for request build

  // First step : ts_addr => tablets_count
  std::map<std::string, int64_t> ts_addr_tablets_count;
  table_ptr->GetTsAddrTabletsCount(&ts_addr_tablets_count);

  for (auto it = ts_addr_tablets_count.begin(); it != ts_addr_tablets_count.end(); ++it) {
    std::unique_ptr<TableQuota> new_tablet_quota(new TableQuota);
    new_tablet_quota->CopyFrom(*tablet_quota);
    for (int quota_info_index = 0; quota_info_index < quota_infos_size; ++quota_info_index) {
      QuotaInfo* quota_info = new_tablet_quota->mutable_quota_infos(quota_info_index);
      if (quota_info->limit() > 0) {
        // Second step : get limit for each Ts
        quota_info->set_limit(quota_info->limit() * it->second);
      }
    }

    VLOG(25) << "quota AddDeltaQuota ts addr = " << it->first << ", table = " << table_name
             << ", tablets count = " << it->second;

    // Third step : set delta_list
    TableQuotaList& table_quota_list = (*delta_ts_table_quotas_list)[it->first];
    table_quota_list[table_name].reset(new_tablet_quota.release());
  }

  std::vector<std::string> all_ts_addr;
  tabletnode_manager_->GetAllTabletNodeAddr(&all_ts_addr);
  for (auto it = all_ts_addr.cbegin(); it != all_ts_addr.cend(); ++it) {
    auto delta_it = delta_ts_table_quotas_list->find(*it);
    if (delta_it == delta_ts_table_quotas_list->end()) {
      std::unique_ptr<TableQuota> new_tablet_quota(new TableQuota);
      new_tablet_quota->CopyFrom(*tablet_quota);
      for (int quota_info_index = 0; quota_info_index < quota_infos_size; ++quota_info_index) {
        QuotaInfo* quota_info = new_tablet_quota->mutable_quota_infos(quota_info_index);
        quota_info->set_limit(-1);
        quota_info->set_period(-1);
      }
      TableQuotaList& table_quota_list = (*delta_ts_table_quotas_list)[*it];
      table_quota_list[table_name].reset(new_tablet_quota.release());
      VLOG(7) << "clear quota " << *it << " no table " << table_name;
    } else {
      VLOG(7) << "reset quota " << *it << " for table " << table_name;
    }
  }
}

std::string MasterQuotaEntry::DebugPrintTableQuotaList() {
  std::ostringstream table_quota_list_info;
  table_quota_list_info
      << "-------------------------Globla TableQuota Infos start------------------------\n";
  for (auto it = table_quotas_list_.begin(); it != table_quotas_list_.end(); ++it) {
    table_quota_list_info << "############ table[" << it->first << "] ############\n";
    table_quota_list_info << QuotaUtils::DebugPrintTableQuota(*(it->second));
    table_quota_list_info << "################################################\n";
  }
  table_quota_list_info
      << "-------------------------Globla TableQuota Infos end--------------------------\n";
  return table_quota_list_info.str();
}

std::string MasterQuotaEntry::DebugPrintDeltaTableList() {
  std::ostringstream delta_table_list_info;
  delta_table_list_info
      << "-----------------------DeltaTableQuota per Ts Infos start---------------------------\n";
  for (auto ts_addr_it = delta_ts_table_quotas_list_.begin();
       ts_addr_it != delta_ts_table_quotas_list_.end(); ++ts_addr_it) {
    for (auto table_quota_it = ts_addr_it->second.begin();
         table_quota_it != ts_addr_it->second.end(); ++table_quota_it) {
      delta_table_list_info << "########## ts[" << ts_addr_it->first << "] table["
                            << table_quota_it->first << "] ##########\n";
      delta_table_list_info << QuotaUtils::DebugPrintTableQuota(*(table_quota_it->second));
      delta_table_list_info << "#########################################################\n";
    }
  }
  delta_table_list_info
      << "-----------------------DeltaTableQuota per Ts Infos end-----------------------------\n";
  return delta_table_list_info.str();
}

void MasterQuotaEntry::SetTabletNodeManager(
    const std::shared_ptr<master::TabletNodeManager>& tabletnode_manager) {
  MutexLock l(&mutex_);
  tabletnode_manager_ = tabletnode_manager;
}

void MasterQuotaEntry::RefreshClusterFlowControlStatus() {
  MutexLock l(&mutex_);
  if (!tabletnode_manager_) {
    return;
  }
  auto cluster_dfs_write_bytes_quota = cluster_dfs_write_bytes_quota_.Get();
  auto cluster_dfs_qps_quota = cluster_dfs_qps_quota_.Get();
  double slowdown_write_ratio{std::numeric_limits<double>::max()};
  // Check dfs write through-put triggered flow control
  if (cluster_dfs_write_bytes_quota >= 0) {
    UpdateDfsWriteBytesQueue();
    auto average_dfs_write_bytes = cluster_dfs_write_bytes_queue_.Average();
    slowdown_write_ratio = (double)cluster_dfs_write_bytes_quota / (average_dfs_write_bytes + 1);
    if (average_dfs_write_bytes >= cluster_dfs_write_bytes_quota) {
      LOG(WARNING) << "Dfs throughput trigger flow control mode, current: "
                   << average_dfs_write_bytes << " quota: " << cluster_dfs_write_bytes_quota
                   << " ratio: " << slowdown_write_ratio;
    }
  }

  // Check dfs master's qps triggered flow control
  if (cluster_dfs_qps_quota >= 0) {
    UpdateDfsQpsQueue();
    auto average_dfs_qps = cluster_dfs_qps_queue_.Average();
    slowdown_write_ratio =
        std::min((double)cluster_dfs_qps_quota / (average_dfs_qps + 1), slowdown_write_ratio);
    if (average_dfs_qps >= cluster_dfs_qps_quota) {
      LOG(WARNING) << "Dfs qps trigger flow control mode, current: " << average_dfs_qps
                   << " quota: " << cluster_dfs_qps_quota << " ratio: " << slowdown_write_ratio;
    }
  }

  if (slowdown_write_ratio < 1) {
    if (!SlowdownModeTriggered()) {
      flow_control_mode.Set(1);
      LOG(WARNING) << "Enter flow control mode, slow-down cluster write.";
    }
    LOG(WARNING) << "Set flow control slow down ratio to: " << slowdown_write_ratio;
    SetSlowdownWriteRatio(slowdown_write_ratio);
    PrepareUpdate();
    VLOG(25) << "RefreshClusterFlowControlStatus cause quota update, version = "
             << version_recorder_.GetVersion();
  } else {
    if (SlowdownModeTriggered()) {
      LOG(WARNING) << "Leave flow control mode.";
      flow_control_mode.Set(0);
      ResetSlowdownWriteRatio();
    }
  }
}

void MasterQuotaEntry::UpdateDfsWriteBytesQueue() {
  mutex_.AssertHeld();
  std::vector<master::TabletNodePtr> tabletnodes;
  tabletnode_manager_->GetAllTabletNodeInfo(&tabletnodes);
  auto current_dfs_write_size = GetClusterDfsWriteSize(tabletnodes);
  cluster_dfs_write_bytes_queue_.Push(current_dfs_write_size);
}

void MasterQuotaEntry::UpdateDfsQpsQueue() {
  mutex_.AssertHeld();
  std::vector<master::TabletNodePtr> tabletnodes;
  tabletnode_manager_->GetAllTabletNodeInfo(&tabletnodes);
  auto current_dfs_qps = std::accumulate(std::begin(tabletnodes), std::end(tabletnodes), (int64_t)0,
                                         [](int64_t val, const master::TabletNodePtr& ptr) {
                                           if (ptr->info_.has_dfs_master_qps()) {
                                             val += ptr->info_.dfs_master_qps();
                                           }
                                           return val;
                                         });
  cluster_dfs_qps_queue_.Push(current_dfs_qps);
}

void MasterQuotaEntry::RefreshDfsHardLimit() {
  MutexLock _(&mutex_);
  if (!tabletnode_manager_) {
    return;
  }
  std::vector<master::TabletNodePtr> tabletnodes;
  tabletnode_manager_->GetAllTabletNodeInfo(&tabletnodes);
  if (tabletnodes.empty()) {
    return;
  }
  RefreshDfsWriteThroughputHardLimit(tabletnodes);
  RefreshDfsReadThroughputHardLimit(tabletnodes);
}

void MasterQuotaEntry::RefreshDfsWriteThroughputHardLimit(
    const std::vector<master::TabletNodePtr>& nodes) {
  mutex_.AssertHeld();
  auto write_hard_limit = GetDfsWriteThroughputHardLimit();
  if (write_hard_limit >= 0) {
    // Limit strategy:
    // We share half of the hard limit value equally on each ts to guarantee their basic needs.
    // And the remaining half of the limit will be set to each ts based on their dfs_write history.
    // The reason of this stragety is to keep each ts has some dfs write quota, and maximize the
    // cluster's dfs write size meanwhile.
    write_hard_limit /= 2;
    auto ts_num = nodes.size();
    auto total_write_size = GetClusterDfsWriteSize(nodes);
    auto base_limit = write_hard_limit / ts_num;
    TsDfsQuotaList new_list;
    for (const auto& node : nodes) {
      new_list[node->addr_] = base_limit;
      if (node->info_.has_dfs_io_w()) {
        new_list[node->addr_] +=
            (double)node->info_.dfs_io_w() / (total_write_size + 1) * write_hard_limit;
      }
    }
    std::swap(dfs_write_throughput_hard_limit_list_, new_list);
    PrepareUpdate();
    VLOG(25) << "RefreshDfsWriteThroughputHardLimit cause quota update, version = "
             << version_recorder_.GetVersion();
  } else {
    dfs_write_throughput_hard_limit_list_.clear();
  }
}

void MasterQuotaEntry::RefreshDfsReadThroughputHardLimit(
    const std::vector<master::TabletNodePtr>& nodes) {
  auto read_hard_limit = GetDfsReadThroughputHardLimit();
  if (read_hard_limit >= 0) {
    // Same stragety as RefreshDfsWriteThroughputHardLimit does.
    read_hard_limit /= 2;
    auto ts_num = nodes.size();
    auto total_read_size = GetClusterDfsReadSize(nodes);
    auto base_limit = read_hard_limit / ts_num;
    TsDfsQuotaList new_list;
    for (const auto& node : nodes) {
      new_list[node->addr_] = base_limit;
      if (node->info_.has_dfs_io_r()) {
        new_list[node->addr_] +=
            (double)node->info_.dfs_io_r() / (total_read_size + 1) * read_hard_limit;
      }
    }
    std::swap(dfs_read_throughput_hard_limit_list_, new_list);
    PrepareUpdate();
    VLOG(25) << "RefreshDfsReadThroughputHardLimit cause quota update, version = "
             << version_recorder_.GetVersion();
  } else {
    dfs_read_throughput_hard_limit_list_.clear();
  }
}

int64_t MasterQuotaEntry::GetClusterDfsWriteSize(const std::vector<master::TabletNodePtr>& nodes) {
  return accumulate(std::begin(nodes), std::end(nodes), (int64_t)0,
                    [](int64_t val, const master::TabletNodePtr& ptr) {
                      if (ptr->info_.has_dfs_io_w()) {
                        val += ptr->info_.dfs_io_w();
                      }
                      return val;
                    });
}

int64_t MasterQuotaEntry::GetClusterDfsReadSize(const std::vector<master::TabletNodePtr>& nodes) {
  return accumulate(std::begin(nodes), std::end(nodes), (int64_t)0,
                    [](int64_t val, const master::TabletNodePtr& ptr) {
                      if (ptr->info_.has_dfs_io_r()) {
                        val += ptr->info_.dfs_io_r();
                      }
                      return val;
                    });
}
}  // namespace quota
}  // namespace tera
