// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/cost_functions.h"

#include <algorithm>
#include <vector>

namespace tera {
namespace load_balancer {

MoveCountCostFunction::MoveCountCostFunction (const LBOptions& options) :
        CostFunction(options, "MoveCountCostFunction"),
        kExpensiveCost(1000000),
        tablet_max_move_num_(options.tablet_max_move_num),
        tablet_max_move_percent_(options.tablet_max_move_percent) {
    SetWeight(options.move_count_cost_weight);
}

MoveCountCostFunction::~MoveCountCostFunction() {
}

double MoveCountCostFunction::Cost() {
    uint32_t max_move_num = std::max(tablet_max_move_num_, static_cast<uint32_t>(cluster_->tablet_num_ * tablet_max_move_percent_));
    double cost = cluster_->tablet_moved_num_;
    if (cost > static_cast<double>(max_move_num)) {
        // return an expensive cost
        VLOG(20) << "[lb] reach max move num limit, max_move_num:" << max_move_num;
        return kExpensiveCost;
    }

    return Scale(0, std::min(cluster_->tablet_num_, max_move_num), cost);
}

MoveFrequencyCostFunction::MoveFrequencyCostFunction(const LBOptions& options) :
        CostFunction(options, "MoveFrequencyCostFunction"),
        kExpensiveCost(100000) {
    SetWeight(options.move_frequency_cost_weight);
}

MoveFrequencyCostFunction::~MoveFrequencyCostFunction() {
}

double MoveFrequencyCostFunction::Cost() {
    if (cluster_->tablets_moved_too_frequently_.size() > 0) {
        // there are tablets moved too frequently, return an expensive cost
        VLOG(20) << "[lb] there are " << cluster_->tablets_moved_too_frequently_.size()
                << " tablets moved too frequently";
        return kExpensiveCost;
    } else {
        return 0;
    }
}

AbnormalNodeCostFunction::AbnormalNodeCostFunction(const LBOptions& options) :
        CostFunction(options, "AbnormalNodeCostFunction"),
        kExpensiveCost(100000) {
    SetWeight(options.abnormal_node_cost_weight);
}

AbnormalNodeCostFunction::~AbnormalNodeCostFunction() {
}

double AbnormalNodeCostFunction::Cost() {
    if (cluster_->tablets_moved_to_abnormal_nodes_.size() > 0) {
        // there are tablets moved to abnormal nodes, return an expensive cost
        VLOG(20) << "[lb] there are " << cluster_->tablets_moved_to_abnormal_nodes_.size()
                << " tablets moved to abnormal nodes";
        return kExpensiveCost;
    } else {
        return 0;
    }
}

ReadPendingNodeCostFunction::ReadPendingNodeCostFunction(const LBOptions& options) :
        CostFunction(options, "ReadPendingNodeCostFunction"),
        kExpensiveCost(10000) {
    SetWeight(options.read_pending_node_cost_weight);
}

ReadPendingNodeCostFunction::~ReadPendingNodeCostFunction() {
}

double ReadPendingNodeCostFunction::Cost() {
    if (cluster_->tablets_moved_to_read_pending_nodes_.size() > 0) {
        // there are tablets moved to read pending nodes, return an expensive cost
        VLOG(20) << "[lb] there are " << cluster_->tablets_moved_to_read_pending_nodes_.size()
                << " tablets moved to read pending nodes";
        return kExpensiveCost;
    } else {
        return 0;
    }
}

WritePendingNodeCostFunction::WritePendingNodeCostFunction(const LBOptions& options) :
        CostFunction(options, "WritePendingNodeCostFunction"),
        kExpensiveCost(10000) {
    SetWeight(options.write_pending_node_cost_weight);
}

WritePendingNodeCostFunction::~WritePendingNodeCostFunction() {
}

double WritePendingNodeCostFunction::Cost() {
    if (cluster_->tablets_moved_to_write_pending_nodes_.size() > 0) {
        // there are tablets moved to write pending nodes, return an expensive cost
        VLOG(20) << "[lb] there are " << cluster_->tablets_moved_to_write_pending_nodes_.size()
                << " tablets moved to write pending nodes";
        return kExpensiveCost;
    } else {
        return 0;
    }
}

ScanPendingNodeCostFunction::ScanPendingNodeCostFunction(const LBOptions& options) :
        CostFunction(options, "ScanPendingNodeCostFunction"),
        kExpensiveCost(10000) {
    SetWeight(options.scan_pending_node_cost_weight);
}

ScanPendingNodeCostFunction::~ScanPendingNodeCostFunction() {
}

double ScanPendingNodeCostFunction::Cost() {
    if (cluster_->tablets_moved_to_scan_pending_nodes_.size() > 0) {
        // there are tablets moved to scan pending nodes, return an expensive cost
        VLOG(20) << "[lb] there are " << cluster_->tablets_moved_to_scan_pending_nodes_.size()
                << " tablets moved to scan pending nodes";
        return kExpensiveCost;
    } else {
        return 0;
    }
}

TabletCountCostFunction::TabletCountCostFunction (const LBOptions& options) :
        CostFunction(options, "TabletCountCostFunction") {
    SetWeight(options.tablet_count_cost_weight);
}

TabletCountCostFunction::~TabletCountCostFunction() {
}

double TabletCountCostFunction::Cost() {
    std::vector<double> tablet_nums_per_node;
    for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
        tablet_nums_per_node.emplace_back(cluster_->tablets_per_node_[i].size());
    }

    return ScaleFromArray(tablet_nums_per_node);
}

SizeCostFunction::SizeCostFunction (const LBOptions& options) :
        CostFunction(options, "SizeCostFunction") {
    SetWeight(options.size_cost_weight);
}

SizeCostFunction::~SizeCostFunction() {
}

double SizeCostFunction::Cost() {
    std::vector<double> size_per_node;
    for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
        size_per_node.emplace_back(cluster_->size_per_node_[i]);
    }

    return ScaleFromArray(size_per_node);
}

ReadLoadCostFunction::ReadLoadCostFunction (const LBOptions& options) :
        CostFunction(options, "ReadLoadCostFunction") {
    SetWeight(options.read_load_cost_weight);
}

ReadLoadCostFunction::~ReadLoadCostFunction() {
}

double ReadLoadCostFunction::Cost() {
    std::vector<double> read_load_per_node;
    for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
        read_load_per_node.emplace_back(cluster_->read_load_per_node_[i]);
    }

    return ScaleFromArray(read_load_per_node);
}

WriteLoadCostFunction::WriteLoadCostFunction (const LBOptions& options) :
        CostFunction(options, "WriteLoadCostFunction") {
    SetWeight(options.write_load_cost_weight);
}

WriteLoadCostFunction::~WriteLoadCostFunction() {
}

double WriteLoadCostFunction::Cost() {
    std::vector<double> write_load_per_node;
    for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
        write_load_per_node.emplace_back(cluster_->write_load_per_node_[i]);
    }

    return ScaleFromArray(write_load_per_node);
}

ScanLoadCostFunction::ScanLoadCostFunction (const LBOptions& options) :
        CostFunction(options, "ScanLoadCostFunction") {
    SetWeight(options.scan_load_cost_weight);
}

ScanLoadCostFunction::~ScanLoadCostFunction() {
}

double ScanLoadCostFunction::Cost() {
    std::vector<double> scan_load_per_node;
    for (uint32_t i = 0; i < cluster_->tablet_node_num_; ++i) {
        scan_load_per_node.emplace_back(cluster_->scan_load_per_node_[i]);
    }

    return ScaleFromArray(scan_load_per_node);
}

} // namespace load_balancer
} // namespace tera
