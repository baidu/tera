// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "ts_write_flow_controller.h"
#include <glog/logging.h>

namespace tera {
// Ts write flow controller in singleton

TsWriteFlowController &TsWriteFlowController::Instance() {
  static TsWriteFlowController write_flow_controller;
  return write_flow_controller;
}

void TsWriteFlowController::SetSlowdownMode(double ratio) {
  std::lock_guard<std::mutex> _(mu_);
  uint64_t total_write_bytes = std::accumulate(
      std::begin(write_throughput_queue_), std::end(write_throughput_queue_), (uint64_t)0,
      [](uint64_t val, const TimeValuePair &pr) { return val + pr.second; });
  auto write_quota =
      static_cast<uint64_t>(total_write_bytes * ratio / write_throughput_queue_.size());
  if (!flow_controller_.InFlowControlMode()) {
    LOG(WARNING) << "Enter slow-down write mode.";
  }

  LOG(WARNING) << "Set write flow limit to " << write_quota
               << " bytes, total_write_bytes: " << total_write_bytes << " ratio: " << ratio;
  flow_controller_.EnterFlowControlMode(write_quota);
  current_write_flow_limit_.Set(write_quota);
}

void TsWriteFlowController::ResetSlowdownMode() {
  std::lock_guard<std::mutex> _(mu_);
  if (flow_controller_.InFlowControlMode()) {
    LOG(WARNING) << "Exit slow-down write mode.";
    current_write_flow_limit_.Set(-1);
  }
  flow_controller_.LeaveFlowControlMode();
}
}
