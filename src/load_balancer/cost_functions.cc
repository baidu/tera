// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/cost_functions.h"

#include <algorithm>
#include <vector>

namespace tera {
namespace load_balancer {

MoveCountCostFunction::MoveCountCostFunction(const LBOptions& options)
    : CostFunction(options, "MoveCountCostFunction"),
      kExpensiveCost(1000000),
      tablet_max_move_num_(options.tablet_max_move_num) {
  SetWeight(options.move_count_cost_weight);
}

MoveCountCostFunction::~MoveCountCostFunction() {}

double MoveCountCostFunction::Cost() {
  double cost = cluster_->tablet_moved_num_;
  if (cost > static_cast<double>(tablet_max_move_num_)) {
    // return an expensive cost
    VLOG(20) << "[lb] reach max move num limit: " << tablet_max_move_num_;
    return kExpensiveCost;
  }

  return Scale(0, std::max(cluster_->tablet_num_, tablet_max_move_num_), cost);
}

TabletCountCostFunction::TabletCountCostFunction(const LBOptions& options)
    : CostFunction(options, "TabletCountCostFunction") {
  SetWeight(options.tablet_count_cost_weight);
}

TabletCountCostFunction::~TabletCountCostFunction() {}

double TabletCountCostFunction::Cost() {
  std::vector<double> tablet_nums_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    tablet_nums_per_node.emplace_back(cluster_->tablets_per_node_[i].size());
  }

  return ScaleFromArray(tablet_nums_per_node);
}

SizeCostFunction::SizeCostFunction(const LBOptions& options)
    : CostFunction(options, "SizeCostFunction") {
  SetWeight(options.size_cost_weight);
}

SizeCostFunction::~SizeCostFunction() {}

double SizeCostFunction::Cost() {
  std::vector<double> size_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    size_per_node.emplace_back(cluster_->size_per_node_[i]);
  }

  return ScaleFromArray(size_per_node);
}

FlashSizeCostFunction::FlashSizeCostFunction(const LBOptions& options)
    : CostFunction(options, "FlashSizeCostFunction") {
  SetWeight(options.flash_size_cost_weight);
}

FlashSizeCostFunction::~FlashSizeCostFunction() {}

double FlashSizeCostFunction::Cost() {
  std::vector<double> flash_size_percent_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    uint64_t node_flash_capacity = cluster_->nodes_[i]->tablet_node_ptr->GetPersistentCacheSize();
    if (node_flash_capacity == 0) {
      // skip the node which does not has ssd
      continue;
    }
    assert(node_flash_capacity > 0);
    flash_size_percent_per_node.emplace_back(100.0 * cluster_->flash_size_per_node_[i] /
                                             node_flash_capacity);
  }

  return ScaleFromArray(flash_size_percent_per_node);
}

ReadLoadCostFunction::ReadLoadCostFunction(const LBOptions& options)
    : CostFunction(options, "ReadLoadCostFunction") {
  SetWeight(options.read_load_cost_weight);
}

ReadLoadCostFunction::~ReadLoadCostFunction() {}

double ReadLoadCostFunction::Cost() {
  std::vector<double> read_load_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    read_load_per_node.emplace_back(cluster_->read_load_per_node_[i]);
  }

  return ScaleFromArray(read_load_per_node);
}

WriteLoadCostFunction::WriteLoadCostFunction(const LBOptions& options)
    : CostFunction(options, "WriteLoadCostFunction") {
  SetWeight(options.write_load_cost_weight);
}

WriteLoadCostFunction::~WriteLoadCostFunction() {}

double WriteLoadCostFunction::Cost() {
  std::vector<double> write_load_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    write_load_per_node.emplace_back(cluster_->write_load_per_node_[i]);
  }

  return ScaleFromArray(write_load_per_node);
}

ScanLoadCostFunction::ScanLoadCostFunction(const LBOptions& options)
    : CostFunction(options, "ScanLoadCostFunction") {
  SetWeight(options.scan_load_cost_weight);
}

ScanLoadCostFunction::~ScanLoadCostFunction() {}

double ScanLoadCostFunction::Cost() {
  std::vector<double> scan_load_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    scan_load_per_node.emplace_back(cluster_->scan_load_per_node_[i]);
  }

  return ScaleFromArray(scan_load_per_node);
}

LReadCostFunction::LReadCostFunction(const LBOptions& options)
    : CostFunction(options, "LReadCostFunction") {
  SetWeight(options.lread_cost_weight);
}

LReadCostFunction::~LReadCostFunction() {}

double LReadCostFunction::Cost() {
  std::vector<double> lread_per_node;
  for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
    lread_per_node.emplace_back(cluster_->lread_per_node_[i]);
  }

  return ScaleFromArray(lread_per_node);
}

}  // namespace load_balancer
}  // namespace tera
