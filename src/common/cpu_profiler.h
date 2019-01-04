// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_CPU_PROFILER_H
#define TERA_CPU_PROFILER_H

#include <atomic>
#include <thread>
#include <string>
#include <mutex>
#include <condition_variable>

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {

class CpuProfiler {
 public:
  /**
   * @brief Init CpuProfiler and the detect thread will start
  **/
  explicit CpuProfiler(const std::string& profiler_file = "CPU");

  ~CpuProfiler();

  CpuProfiler& SetEnable(bool enable) {
    if (enable) {
      LOG(INFO) << "[Cpu Profiler] Cpu Profiler Enabled";
    } else {
      LOG(INFO) << "[Cpu Profiler] Cpu Profiler Disabled";
    }

    {
      std::unique_lock<std::mutex> lock(lock_);
      enable_ = enable;
    }
    cv_.notify_one();
    return *this;
  }

  CpuProfiler& SetInterval(int second) {
    {
      std::unique_lock<std::mutex> lock(lock_);
      interval_ = std::chrono::seconds(second);
    }
    cv_.notify_one();
    return *this;
  }

 private:
  void run();

 private:
  std::atomic<bool> exit_;
  bool enable_{false};
  std::chrono::seconds interval_{10};
  // Never Changed, So we can use profiler_file_.c_str() in safe.
  const std::string profiler_file_;
  std::mutex lock_;
  std::condition_variable cv_;
  std::thread thread_;
};

}  // namespace tera

#endif  // TERA_CPU_PROFILER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
