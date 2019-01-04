// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_SDK_SDK_PERF_H_
#define TERA_SDK_SDK_PERF_H_

#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "common/metric/metric_counter.h"
#include "common/metric/collector_report.h"
#include "common/this_thread.h"
#include "tera.h"

DECLARE_int32(tera_sdk_perf_collect_interval);

namespace tera {
namespace sdk {

class PerfCollecter {
 public:
  PerfCollecter() : stopped_(false) {}
  ~PerfCollecter() {}

  void Run() { thread_ = std::thread{&PerfCollecter::ScheduleCollect, this}; }

  void Stop() {
    stopped_ = true;
    thread_.join();
  }

 private:
  void ScheduleCollect() {
    while (!stopped_) {
      CollectorReportPublisher::GetInstance().Refresh();
      DumpLog();
      ThisThread::Sleep(FLAGS_tera_sdk_perf_collect_interval);
    }
  }

  void DumpLog();

 private:
  std::thread thread_;
  bool stopped_;
};

}  // namespace sdk
}  // namespace tera

#endif  // TERA_SDK_SDK_PERF_H_
