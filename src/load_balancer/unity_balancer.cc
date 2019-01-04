// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/unity_balancer.h"

#include <algorithm>
#include <limits>

#include "glog/logging.h"
#include "load_balancer/random.h"
#include "common/timer.h"

namespace tera {
namespace load_balancer {

using tera::master::TabletNodePtr;
using tera::master::TabletPtr;

UnityBalancer::UnityBalancer(const LBOptions& options) : lb_options_(options) {
  if (lb_options_.move_count_cost_weight > 0) {
    cost_functions_.emplace_back(new MoveCountCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.tablet_count_cost_weight > 0) {
    cost_functions_.emplace_back(new TabletCountCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new TabletCountActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.size_cost_weight > 0) {
    cost_functions_.emplace_back(new SizeCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new SizeActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.flash_size_cost_weight > 0) {
    cost_functions_.emplace_back(new FlashSizeCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new FlashSizeActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.read_load_cost_weight > 0) {
    cost_functions_.emplace_back(new ReadLoadCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new ReadLoadActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.write_load_cost_weight > 0) {
    cost_functions_.emplace_back(new WriteLoadCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new WriteLoadActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.scan_load_cost_weight > 0) {
    cost_functions_.emplace_back(new ScanLoadCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new ScanLoadActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
  if (lb_options_.lread_cost_weight > 0) {
    cost_functions_.emplace_back(new LReadCostFunction(options));
    VLOG(20) << "[lb] " << cost_functions_[cost_functions_.size() - 1]->Name() << " enabled";
    action_generators_.emplace_back(new LReadActionGenerator());
    VLOG(20) << "[lb] " << action_generators_[action_generators_.size() - 1]->Name() << " enabled";
  }
}

UnityBalancer::~UnityBalancer() {}

bool UnityBalancer::BalanceCluster(const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
                                   std::vector<Plan>* plans) {
  return BalanceCluster("", lb_nodes, plans);
}

bool UnityBalancer::BalanceCluster(const std::string& table_name,
                                   const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
                                   std::vector<Plan>* plans) {
  if (lb_nodes.size() <= 1 || plans == nullptr) {
    return false;
  }

  VLOG(5) << "[lb] BalanceCluster for table:" << table_name << " begin";

  bool skip_meta_node = lb_options_.meta_table_isolate_enabled ? true : false;
  std::shared_ptr<Cluster> cluster =
      std::make_shared<Cluster>(lb_nodes, lb_options_, skip_meta_node);

  if (lb_options_.debug_mode_enabled) {
    cluster->DebugCluster();
  }

  InitCostFunctions(cluster);

  if (!NeedBalance(cluster)) {
    return true;
  }

  uint64_t max_steps = std::min(
      lb_options_.max_compute_steps,
      static_cast<uint64_t>(lb_options_.max_compute_steps_per_tablet * cluster->tablet_num_));
  double init_cost = ComputeCost(std::numeric_limits<double>::max());
  double current_cost = init_cost;

  VLOG(5) << "[lb] compute begin, max_steps:" << max_steps << " init total cost:" << init_cost;

  int64_t start_time_ns = get_micros();
  int64_t cost_time_ms = 0;
  uint64_t step = 0;
  uint32_t success_step = 0;
  for (step = 0; step < max_steps; ++step) {
    std::shared_ptr<Action> action(NextAction(cluster));
    VLOG(20) << "[lb] step:" << step << " action:" << action->ToString();

    if (!cluster->ValidAction(action)) {
      continue;
    }

    cluster->DoAction(action);

    if (lb_options_.debug_mode_enabled) {
      cluster->DebugCluster();
    }

    double new_cost = ComputeCost(current_cost);
    if (new_cost < current_cost) {
      VLOG(10) << "[lb] step " << step << " got lower cost " << new_cost << " by "
               << action->GetGeneratorName();
      current_cost = new_cost;
      ++success_step;
    } else {
      std::shared_ptr<Action> undo_action(action->UndoAction());
      VLOG(20) << "[lb] undo action:" << undo_action->ToString();
      cluster->DoAction(undo_action);

      if (lb_options_.debug_mode_enabled) {
        cluster->DebugCluster();
      }
    }

    if (success_step >= lb_options_.tablet_max_move_num) {
      VLOG(5) << "[lb] stop computing since success_step reach to "
                 "tablet_max_move_num:" << lb_options_.tablet_max_move_num;
      break;
    }

    cost_time_ms = (get_micros() - start_time_ns) / 1000;
    if (static_cast<uint64_t>(cost_time_ms) > lb_options_.max_compute_time_ms) {
      VLOG(5) << "[lb] stop computing since time reach to max_compute_time_ms:"
              << lb_options_.max_compute_time_ms;
      break;
    }
  }

  VLOG(5) << "[lb] compute end, compute time(ms):" << cost_time_ms << " compute steps:" << step
          << " init total cost:" << init_cost << " new total cost:" << current_cost;

  if (current_cost < init_cost) {
    CreatePlans(cluster, plans);
    VLOG(5) << "[lb] balance plan size:" << plans->size();
  } else {
    VLOG(5) << "[lb] no better balance plan";
  }

  VLOG(5) << "[lb] BalanceCluster for table:" << table_name << " end";

  return true;
}

bool UnityBalancer::NeedBalance(const std::shared_ptr<Cluster>& cluster) {
  if (cluster->tablet_node_num_ <= 1) {
    LOG(INFO) << "[lb] no enough nodes to balance";
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

  if (heavy_pending_node_num > 0) {
    LOG(INFO) << "[lb] cluster has " << heavy_pending_node_num
              << " heavy pending nodes, need balance";
    return true;
  }

  double total_cost = 0.0;
  double total_weight = 0.0;

  for (const auto& cost_func : cost_functions_) {
    double weight = cost_func->GetWeight();
    if (weight <= 0) {
      continue;
    }
    double cost = cost_func->Cost();
    VLOG(5) << "[lb] init cost of " << cost_func->Name() << ": " << cost << " * " << weight;

    total_weight += weight;
    total_cost += cost * weight;
  }
  double cost = total_weight == 0 ? 0 : total_cost / total_weight;

  VLOG(5) << "[lb] NeedBalance compute, total_cost:" << total_cost
          << " total_weight:" << total_weight << " cost:" << cost
          << " min_cost_need_balance:" << lb_options_.min_cost_need_balance;

  if (total_cost <= 0 || total_weight <= 0 || cost < lb_options_.min_cost_need_balance) {
    LOG(INFO) << "[lb] cluster is well balanced, no need to balance";
    return false;
  } else {
    return true;
  }
}

void UnityBalancer::InitCostFunctions(const std::shared_ptr<Cluster>& cluster) {
  for (const auto& cost_func : cost_functions_) {
    cost_func->Init(cluster);
  }
}

double UnityBalancer::ComputeCost(double previous_cost) {
  VLOG(20) << "[lb] ComputeCost begin, previous total cost:" << previous_cost;
  double total_cost = 0.0;

  for (const auto& cost_func : cost_functions_) {
    double weight = cost_func->GetWeight();
    if (weight <= 0) {
      continue;
    }
    double cost = cost_func->Cost();
    total_cost += cost * weight;
    VLOG(20) << "[lb] " << cost_func->Name() << " cost:" << cost << " weight:" << weight;
    if (total_cost > previous_cost) {
      break;
    }
  }

  VLOG(20) << "[lb] ComputeCost end, new total cost:" << total_cost;
  return total_cost;
}

Action* UnityBalancer::NextAction(const std::shared_ptr<Cluster>& cluster) {
  uint32_t rand = Random::Rand(0, action_generators_.size());
  return action_generators_[rand]->Generate(cluster);
}

void UnityBalancer::CreatePlans(const std::shared_ptr<Cluster>& cluster, std::vector<Plan>* plans) {
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

std::string UnityBalancer::GetName() { return "UnityBalancer"; }

}  // namespace load_balancer
}  // namespace tera
