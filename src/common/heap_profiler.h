// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_HEAP_PROFILER_H
#define TERA_HEAP_PROFILER_H

#include <atomic>
#include <thread>
#include <mutex>
#include <string>
#include <condition_variable>
#include <cstdlib>

#include <gflags/gflags.h>
#include <glog/logging.h>

namespace tera {

class HeapProfiler {
 public:
  /**
   * @brief Init HeapProfiler and the detect thread will start
  **/
  explicit HeapProfiler(const std::string& profiler_file = "HEAP");
  /**
   * @brief: the heap profiler will stop after descontrucor called
   *
  **/
  ~HeapProfiler();
  HeapProfiler& SetEnable(bool enable);

  HeapProfiler& SetInterval(int second) {
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

#endif  // TERA_HEAP_PROFILER

/* vim: set ts=4 sw=4 sts=4 tw=100 */
