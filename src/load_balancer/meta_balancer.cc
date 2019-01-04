// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/meta_balancer.h"

#include <algorithm>
#include <limits>

#include "glog/logging.h"
#include "load_balancer/random.h"
#include "common/timer.h"

namespace tera {
namespace load_balancer {

using tera::master::TabletNodePtr;
using tera::master::TabletPtr;

static uint32_t s_max_compute_step = 100;

MetaBalancer::MetaBalancer(const LBOptions& options) : lb_options_(options) {
  action_generators_.emplace_back(new MetaIsolateActionGenerator());
}

MetaBalancer::~MetaBalancer() {}

bool MetaBalancer::BalanceCluster(const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
                                  std::vector<Plan>* plans) {
  return BalanceCluster("", lb_nodes, plans);
}

bool MetaBalancer::BalanceCluster(const std::string& table_name,
                                  const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
                                  std::vector<Plan>* plans) {
  if (lb_nodes.size() <= 1 || plans == nullptr) {
    return false;
  }

  VLOG(5) << "[lb] balance meta node begin";

  std::shared_ptr<Cluster> cluster =
      std::make_shared<Cluster>(lb_nodes, lb_options_, false /*skip_meta_node*/);

  if (lb_options_.debug_mode_enabled) {
    cluster->DebugCluster();
  }

  if (!NeedBalance(cluster)) {
    return true;
  }

  uint32_t step = 0;
  uint32_t valid_action_count = 0;
  while (true) {
    ++step;
    if (step > s_max_compute_step) {
      break;
    }
    if (cluster->tablets_per_node_[cluster->meta_table_node_index_].size() == 1) {
      break;
    }

    std::shared_ptr<Action> action(NextAction(cluster));
    VLOG(20) << "[lb] action:" << action->ToString();

    if (cluster->ValidAction(action)) {
      ++valid_action_count;
    } else {
      continue;
    }

    cluster->DoAction(action);

    if (lb_options_.debug_mode_enabled) {
      cluster->DebugCluster();
    }

    if (valid_action_count >= lb_options_.meta_balance_max_move_num) {
      break;
    }
  }

  CreatePlans(cluster, plans);

  return true;
}

bool MetaBalancer::NeedBalance(const std::shared_ptr<Cluster>& cluster) {
  if (!lb_options_.meta_table_isolate_enabled) {
    VLOG(10) << "[lb] meta isolate is closed, no need to balance";
    return false;
  }

  if (cluster->tablet_node_num_ == 0) {
    LOG(INFO) << "[lb] empty cluster , no need to balance";
    return false;
  }

  if (cluster->tablets_per_node_[cluster->meta_table_node_index_].size() == 1) {
    LOG(INFO) << "[lb] meta node is isolated, no need to balance";
    return false;
  }

  uint32_t heavy_pending_node_num = cluster->HeavyPendingNodeNum();
  uint32_t bad_node_num = cluster->abnormal_nodes_index_.size() + heavy_pending_node_num;
  double bad_node_percent =
      static_cast<double>(bad_node_num) / static_cast<double>(cluster->tablet_node_num_);
  if (bad_node_percent >= lb_options_.bad_node_safemode_percent) {
    LOG(INFO) << "[lb] bad node num: " << bad_node_num
              << ", total node num: " << cluster->tablet_node_num_
              << ", bad node safemode percent: " << lb_options_.bad_node_safemode_percent
              << ", too many bad nodes, skip balance";
    return false;
  }

  return true;
}

Action* MetaBalancer::NextAction(const std::shared_ptr<Cluster>& cluster) {
  uint32_t rand = Random::Rand(0, action_generators_.size());
  return action_generators_[rand]->Generate(cluster);
}

void MetaBalancer::CreatePlans(const std::shared_ptr<Cluster>& cluster, std::vector<Plan>* plans) {
  for (uint32_t i = 0; i < cluster->tablet_index_to_node_index_.size(); ++i) {
    uint32_t initial_node_index = cluster->initial_tablet_index_to_node_index_[i];
    uint32_t new_node_index = cluster->tablet_index_to_node_index_[i];

    if (initial_node_index != new_node_index) {
      // tablet has been moved to another tablet node
      Plan plan(cluster->tablets_[i]->tablet_ptr,
                cluster->nodes_[initial_node_index]->tablet_node_ptr,
                cluster->nodes_[new_node_index]->tablet_node_ptr);
      plans->emplace_back(plan);
    }
  }
}

std::string MetaBalancer::GetName() { return "MetaBalancer"; }

}  // namespace load_balancer
}  // namespace tera
