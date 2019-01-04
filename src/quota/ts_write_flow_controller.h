// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once
#include "flow_controller.h"

namespace tera {
class TsWriteFlowController {
 private:
  // 10 min
  static constexpr uint64_t kHoldWriteThroughputSeconds = 600;

 public:
  using TimeValuePair = std::pair<uint64_t, uint64_t>;
  static TsWriteFlowController& Instance();

  void Append(uint64_t timestamp_ms, uint64_t write_throughput) {
    std::lock_guard<std::mutex> _(mu_);
    write_throughput_queue_.emplace_back(timestamp_ms, write_throughput);
    DropExpiredValue();
  }

  void SetSlowdownMode(double ratio);

  void ResetSlowdownMode();

  bool TryWrite(uint64_t size) { return flow_controller_.TryConsume(size); }

  bool InSlowdownMode() { return flow_controller_.InFlowControlMode(); }

 private:
  TsWriteFlowController()
      : flow_controller_{0, 1000},
        current_write_flow_limit_{
            "ts_write_flow_limit", {tera::Subscriber::SubscriberType::LATEST}, false} {
    current_write_flow_limit_.Set(-1);
  }

  // Protected by mu_
  void DropExpiredValue() {
    if (write_throughput_queue_.empty()) {
      return;
    }

    auto last_enqueue_ts = write_throughput_queue_.back().first;
    while (last_enqueue_ts - write_throughput_queue_.front().first >=
           kHoldWriteThroughputSeconds * 1000) {
      write_throughput_queue_.pop_front();
    }
  }

 private:
  std::mutex mu_;
  std::deque<TimeValuePair> write_throughput_queue_;
  FlowController flow_controller_;
  MetricCounter current_write_flow_limit_;
};
}