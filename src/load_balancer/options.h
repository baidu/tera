// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_OPTIONS_H_
#define TERA_LOAD_BALANCER_OPTIONS_H_

#include <string>

namespace tera {
namespace load_balancer {

struct LBOptions {
  uint64_t max_compute_steps;
  uint32_t max_compute_steps_per_tablet;
  uint64_t max_compute_time_ms;
  double min_cost_need_balance;
  double bad_node_safemode_percent;

  double move_count_cost_weight;
  uint32_t meta_balance_max_move_num;
  uint32_t tablet_max_move_num;

  uint32_t tablet_move_too_frequently_threshold_s;

  /*
   * if not ready tablets's ratio is higher than this value,
   * the node is considered as an abnormal node
   */
  double abnormal_node_ratio;

  double tablet_count_cost_weight;
  double size_cost_weight;
  double flash_size_cost_weight;

  double read_load_cost_weight;
  double write_load_cost_weight;
  double scan_load_cost_weight;
  double lread_cost_weight;
  double heavy_read_pending_threshold;
  double heavy_write_pending_threshold;
  double heavy_scan_pending_threshold;
  double heavy_lread_threshold;

  double read_pending_factor;
  double write_pending_factor;
  double scan_pending_factor;

  bool meta_table_isolate_enabled;
  std::string meta_table_name;
  std::string meta_table_node_addr;

  bool debug_mode_enabled;

  LBOptions()
      : max_compute_steps(1000000),
        max_compute_steps_per_tablet(1000),
        max_compute_time_ms(30 * 1000),
        min_cost_need_balance(0.02),
        bad_node_safemode_percent(0.5),

        move_count_cost_weight(1),
        meta_balance_max_move_num(1),
        tablet_max_move_num(1),

        tablet_move_too_frequently_threshold_s(600),
        abnormal_node_ratio(0.5),

        tablet_count_cost_weight(100),
        size_cost_weight(200),
        flash_size_cost_weight(0),

        read_load_cost_weight(10),
        write_load_cost_weight(10),
        scan_load_cost_weight(5),
        lread_cost_weight(10),
        heavy_read_pending_threshold(1000),
        heavy_write_pending_threshold(1000),
        heavy_scan_pending_threshold(1000),
        heavy_lread_threshold(1000000),

        read_pending_factor(1),
        write_pending_factor(1),
        scan_pending_factor(1),

        meta_table_isolate_enabled(true),
        meta_table_name("meta_table"),
        meta_table_node_addr(""),

        debug_mode_enabled(false) {}
};

}  // namespace load_balancer
}  // namespace tera

#endif  // TERA_LOAD_BALANCER_OPTIONS_H_
