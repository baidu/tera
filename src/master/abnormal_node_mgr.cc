// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>

#include "common/timer.h"
#include "master/abnormal_node_mgr.h"

namespace tera {

namespace master {

AbnormalNodeMgr::AbnormalNodeMgr() {}

AbnormalNodeMgr::~AbnormalNodeMgr() {}

void AbnormalNodeMgr::RecordNodeDelete(const std::string& addr, const int64_t delete_time) {
  if (FLAGS_abnormal_node_trigger_count < 1) {
    return;
  }
  VLOG(30) << "append node delete time, node: " << addr << ", time: " << get_time_str(delete_time);

  MutexLock lock(&mutex_);
  NodeAbnormalInfo& info = nodes_abnormal_infos_[addr];
  info.deleted_times.emplace_back(delete_time);
  if (info.deleted_times.size() > static_cast<size_t>(FLAGS_abnormal_node_trigger_count)) {
    info.deleted_times.erase(info.deleted_times.begin());
  }

  if (DeleteTooFrequent(info.deleted_times)) {
    int64_t last_delete_time = info.deleted_times[info.deleted_times.size() - 1];
    // avoiding overflow, 30 is large enough
    int64_t recovery_wait_time = FLAGS_abnormal_node_auto_recovery_period_s
                                 << (info.abnormal_count > 30 ? 30 : info.abnormal_count);
    if (recovery_wait_time > 24 * 3600) {  // no more than 24 hours
      recovery_wait_time = 24 * 3600;
    }
    info.recovery_time = last_delete_time + recovery_wait_time;

    ++info.abnormal_count;
    info.deleted_times.clear();

    VLOG(30) << "delete too frequent(delete " << FLAGS_abnormal_node_trigger_count << " times in "
             << FLAGS_abnormal_node_check_period_s << "s), abnormal count: " << info.abnormal_count
             << ", recovery_time: " << get_time_str(info.recovery_time);
  }

  if (delay_add_nodes_.find(addr) != delay_add_nodes_.end()) {
    delay_add_nodes_.erase(addr);
    abnormal_nodes_count_.Set(delay_add_nodes_.size());
    VLOG(30) << "cancel delay add node, addr: " << addr;
  }
}

bool AbnormalNodeMgr::IsAbnormalNode(const std::string& addr, const std::string& uuid) {
  MutexLock lock(&mutex_);
  if (nodes_abnormal_infos_.find(addr) == nodes_abnormal_infos_.end()) {
    return false;
  }

  NodeAbnormalInfo& info = nodes_abnormal_infos_[addr];
  if (get_micros() / 1000000 >= info.recovery_time) {
    return false;
  } else {
    DelayAddNode(addr, uuid);
    return true;
  }
}

std::string AbnormalNodeMgr::GetNodeInfo(const std::string& addr) {
  MutexLock lock(&mutex_);
  std::string ret = "";
  if (nodes_abnormal_infos_.find(addr) != nodes_abnormal_infos_.end()) {
    ret = ret + "abnormal node: " + addr + ", abnormal count: " +
          std::to_string(nodes_abnormal_infos_[addr].abnormal_count) + ", recovery time: " +
          get_time_str(nodes_abnormal_infos_[addr].recovery_time);
  }
  return ret;
}

void AbnormalNodeMgr::ConsumeRecoveredNodes(std::unordered_map<std::string, std::string>* nodes) {
  MutexLock lock(&mutex_);
  for (const auto& node : delay_add_nodes_) {
    if (get_micros() / 1000000 >= node.second.recovery_time) {
      nodes->emplace(node.first, node.second.uuid);
    }
  }

  for (const auto& node : *nodes) {
    delay_add_nodes_.erase(node.first);
  }
  abnormal_nodes_count_.Set(delay_add_nodes_.size());
}

void AbnormalNodeMgr::GetDelayAddNodes(std::unordered_map<std::string, std::string>* nodes) {
  MutexLock lock(&mutex_);
  for (const auto& node : delay_add_nodes_) {
    nodes->emplace(node.first, node.second.uuid);
  }
}

void AbnormalNodeMgr::DelayAddNode(const std::string& addr, const std::string& uuid) {
  VLOG(30) << "delay add node, addr: " << addr << ", uuid: " << uuid;
  mutex_.AssertHeld();
  DelayAddNodeInfo& info = delay_add_nodes_[addr];
  info.uuid = uuid;
  assert(nodes_abnormal_infos_.find(addr) != nodes_abnormal_infos_.end());
  info.recovery_time = nodes_abnormal_infos_[addr].recovery_time;
  abnormal_nodes_count_.Set(delay_add_nodes_.size());
}

bool AbnormalNodeMgr::DeleteTooFrequent(const std::vector<int64_t>& times) {
  if (FLAGS_abnormal_node_trigger_count < 2) {
    return false;
  }
  assert(times.size() <= static_cast<size_t>(FLAGS_abnormal_node_trigger_count));
  if (times.size() < static_cast<size_t>(FLAGS_abnormal_node_trigger_count)) {
    return false;
  }

  if (times[FLAGS_abnormal_node_trigger_count - 1] - times[0] <=
      FLAGS_abnormal_node_check_period_s) {
    return true;
  } else {
    return false;
  }
}

}  // namespace master

}  // namespace tera
