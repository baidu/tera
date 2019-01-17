#pragma once

// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>

#include "common/metric/metric_counter.h"
#include "common/mutex.h"

DECLARE_int64(abnormal_node_check_period_s);
DECLARE_int32(abnormal_node_trigger_count);
DECLARE_int64(abnormal_node_auto_recovery_period_s);

namespace tera {

namespace master {

class AbnormalNodeMgr {
 public:
  AbnormalNodeMgr();
  ~AbnormalNodeMgr();

  void RecordNodeDelete(const std::string& addr, const int64_t delete_time);

  bool IsAbnormalNode(const std::string& addr, const std::string& uuid);

  std::string GetNodeInfo(const std::string& addr);

  void ConsumeRecoveredNodes(std::unordered_map<std::string, std::string>* nodes);

  void GetDelayAddNodes(std::unordered_map<std::string, std::string>* nodes);

 private:
  struct NodeAbnormalInfo {
    std::vector<int64_t> deleted_times;
    int64_t abnormal_count;
    int64_t recovery_time;

    NodeAbnormalInfo() : abnormal_count(0), recovery_time(0) {}
  };

  std::unordered_map<std::string, NodeAbnormalInfo> nodes_abnormal_infos_;

  struct DelayAddNodeInfo {
    std::string uuid;
    int64_t recovery_time;
  };

  std::unordered_map<std::string, DelayAddNodeInfo> delay_add_nodes_;

  mutable Mutex mutex_;
  MetricCounter abnormal_nodes_count_{
      "tera_master_abnormal_nodes_count", {SubscriberType::LATEST}, false};

 private:
  void DelayAddNode(const std::string& addr, const std::string& uuid);

  bool DeleteTooFrequent(const std::vector<int64_t>& times);
};

}  // namespace master

}  // namespace tera
