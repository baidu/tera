// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/base/stdint.h"
#include "gflags/gflags.h"

DEFINE_string(tera_lb_server_addr, "0.0.0.0", "default load balancer rpc server addr");
DEFINE_string(tera_lb_server_port, "31000", "default load balancer rpc server port");
DEFINE_int32(tera_lb_server_thread_num, 1, "default load balancer rpc server thread pool num");
DEFINE_int32(tera_lb_impl_thread_num, 1, "default load balancer impl thread pool num");

DEFINE_string(tera_lb_meta_table_name, "meta_table", "the meta table name");
DEFINE_bool(tera_lb_meta_isolate_enabled, true, "enable master to reserve a tabletnode for meta");

DEFINE_bool(tera_lb_by_table, false, "balance by table one by one");
DEFINE_int32(tera_lb_meta_balance_period_s, 180, "default meta load balance period(s)");
DEFINE_int32(tera_lb_meta_balance_max_move_num, 1, "default max move num for meta balance(s)");
DEFINE_int32(tera_lb_load_balance_period_s, 60, "default load balance period(s)");
DEFINE_int32(tera_lb_max_compute_steps, 1000000,
             "default max compute steps for one balance procedure");
DEFINE_int32(tera_lb_max_compute_steps_per_tablet, 1000,
             "default max compute steps per tablet for one balance procedure");
DEFINE_int32(tera_lb_max_compute_time_ms, 30000,
             "default max compute time(ms) for one balance procedure");
DEFINE_double(tera_lb_min_cost_need_balance, 0.02, "min cost needed for balance");
DEFINE_double(tera_lb_bad_node_safemode_percent, 0.5,
              "if bad node num percent is higher than this, skip balance");

DEFINE_double(tera_lb_move_count_cost_weight, 1, "move cost weight");
DEFINE_int32(tera_lb_tablet_max_move_num, 2,
             "default tablet max move num for one balance procedure");

DEFINE_int32(tera_lb_tablet_move_too_frequently_threshold_s, 3600,
             "if move a tablet in this threshold time(s) again, it's been "
             "moved too frequently");
DEFINE_double(tera_lb_abnormal_node_ratio, 0.5, "abnormal node ratio");

DEFINE_double(tera_lb_tablet_count_cost_weight, 100, "tablet count cost weight");
DEFINE_double(tera_lb_size_cost_weight, 200, "size cost weight");
DEFINE_double(tera_lb_flash_size_cost_weight, 0, "flash size cost weight");

DEFINE_double(tera_lb_read_load_cost_weight, 10, "read load cost weight");
DEFINE_double(tera_lb_write_load_cost_weight, 10, "write load cost weight");
DEFINE_double(tera_lb_scan_load_cost_weight, 1, "scan load cost weight");
DEFINE_double(tera_lb_lread_cost_weight, 5, "lread cost weight");
DEFINE_double(tera_lb_heavy_read_pending_threshold, 1000, "heavy read pending threshold");
DEFINE_double(tera_lb_heavy_write_pending_threshold, 1000, "heavy write pending threshold");
DEFINE_double(tera_lb_heavy_scan_pending_threshold, 1000, "heavy scan pending threshold");
DEFINE_double(tera_lb_heavy_lread_threshold, 1000000, "heavy lread threshold");

DEFINE_double(tera_lb_read_pending_factor, 1, "read pending factor");
DEFINE_double(tera_lb_write_pending_factor, 1, "write pending factor");
DEFINE_double(tera_lb_scan_pending_factor, 1, "scan pending factor");

DEFINE_bool(tera_lb_debug_mode_enabled, false, "debug mode");
