// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_OPTIONS_H_
#define TERA_LOAD_BALANCER_OPTIONS_H_

#include <string>

namespace tera {
namespace load_balancer {

struct LBOptions {
    // calculate
    uint64_t max_compute_steps;
    uint32_t max_compute_steps_per_tablet;
    uint64_t max_compute_time_ms;
    double min_cost_need_balance;
    double bad_node_safemode_percent;

    // MoveCountCostFunction
    double move_count_cost_weight;
    uint32_t tablet_max_move_num;

    // MoveFrequencyCostFunction
    double move_frequency_cost_weight;
    uint32_t tablet_move_too_frequently_threshold_s;

    // AbnormalNodeCostFunction
    double abnormal_node_cost_weight;
    // if not ready tablets's ratio is hither than this value,
    // the node in considered abnormal
    double abnormal_node_ratio;

    // ReadPendingNodeCostFunction
    double read_pending_node_cost_weight;

    // WritePendingNodeCostFunction
    double write_pending_node_cost_weight;

    // ScanPendingNodeCostFunction
    double scan_pending_node_cost_weight;

    // CountCostFunction
    double tablet_count_cost_weight;

    // SizeCostFunction
    double size_cost_weight;

    // LoadCostFunction
    double read_load_cost_weight;
    double write_load_cost_weight;
    double scan_load_cost_weight;

    double read_pending_factor;
    double write_pending_factor;
    double scan_pending_factor;

    // meta table
    bool meta_table_isolate_enabled;
    std::string meta_table_name;
    std::string meta_table_node_addr;

    // debug
    bool debug_mode_enabled;

    LBOptions() :
            max_compute_steps(1000000),
            max_compute_steps_per_tablet(1000),
            max_compute_time_ms(30 * 1000),
            min_cost_need_balance(0.05),
            bad_node_safemode_percent(0.5),

            move_count_cost_weight(10),
            tablet_max_move_num(1),

            move_frequency_cost_weight(10),
            tablet_move_too_frequently_threshold_s(600),

            abnormal_node_cost_weight(10),
            abnormal_node_ratio(0.5),

            read_pending_node_cost_weight(10),
            write_pending_node_cost_weight(10),
            scan_pending_node_cost_weight(10),

            tablet_count_cost_weight(100),
            size_cost_weight(100),
            read_load_cost_weight(20),
            write_load_cost_weight(20),
            scan_load_cost_weight(20),

            read_pending_factor(100),
            write_pending_factor(100),
            scan_pending_factor(100),

            meta_table_isolate_enabled(true),
            meta_table_name("meta_table"),
            meta_table_node_addr(""),

            debug_mode_enabled(false) {
    }
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_OPTIONS_H_
