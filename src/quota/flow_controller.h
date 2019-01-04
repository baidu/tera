// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#pragma once
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <numeric>
#include <thread>
#include "common/event.h"
#include "common/metric/metric_counter.h"

namespace tera {

class FlowLimiter {
 public:
  explicit FlowLimiter(uint64_t limit) : availiable_quota_{limit}, limit_{limit} {}

  void ResetQuota() {
    std::lock_guard<std::mutex> _(mu_);
    availiable_quota_ = (limit_);
    cv_.notify_all();
  }

  void SetLimit(uint64_t limit) { limit_ = limit; }

  bool TryConsume(uint64_t size) {
    std::lock_guard<std::mutex> _(mu_);
    if (availiable_quota_ >= size) {
      availiable_quota_ -= size;
      return true;
    }
    return false;
  }

  void BlockingConsume(uint64_t size) {
    std::unique_lock<std::mutex> _(mu_);
    if (availiable_quota_ < size) {
      cv_.wait(_, [this, size] { return availiable_quota_ >= size; });
      assert(availiable_quota_ >= size);
    }
    availiable_quota_ -= size;
  }

 private:
  uint64_t availiable_quota_;
  std::atomic<uint64_t> limit_;
  std::mutex mu_;
  std::condition_variable cv_;
};

class FlowController {
 private:
  enum class FlowControlStatus { kFlowControlMode, kNormal };

 public:
  FlowController(const FlowController&) = delete;
  void operator=(const FlowController&) = delete;

  explicit FlowController(uint64_t limit, uint64_t reset_interval_ms) : limiter_(limit) {
    t_ = std::thread{[this, reset_interval_ms]() {
      while (!stop_event_.TimeWait(reset_interval_ms)) {
        limiter_.ResetQuota();
      }
    }};
  };

  virtual ~FlowController() {
    stop_event_.Set();
    t_.join();
  }

  void EnterFlowControlMode(uint64_t value) {
    std::lock_guard<std::mutex> _{mu_};
    status_.store(FlowControlStatus::kFlowControlMode);
    limiter_.SetLimit(value);
  }

  void LeaveFlowControlMode() {
    std::lock_guard<std::mutex> _{mu_};
    status_.store(FlowControlStatus::kNormal);
    limiter_.SetLimit(std::numeric_limits<uint64_t>::max());
  }

  bool InFlowControlMode() { return status_.load() == FlowControlStatus::kFlowControlMode; }

  void BlockingConsume(uint64_t bytes) {
    if (InFlowControlMode()) {
      limiter_.BlockingConsume(bytes);
    }
  }

  bool TryConsume(uint64_t bytes) {
    if (InFlowControlMode()) {
      return limiter_.TryConsume(bytes);
    }
    return true;
  }

 private:
  std::thread t_;
  common::AutoResetEvent stop_event_;
  std::mutex mu_;
  FlowLimiter limiter_;
  std::atomic<FlowControlStatus> status_{FlowControlStatus::kNormal};
};

inline FlowController& DfsWriteThroughputHardLimiter() {
  static FlowController dfs_flow_controller{0, 1000};
  return dfs_flow_controller;
}

inline FlowController& DfsReadThroughputHardLimiter() {
  static FlowController dfs_flow_controller{0, 1000};
  return dfs_flow_controller;
}
}
