// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
#pragma once

#include "common/metric/collector_report_publisher.h"
#include "common/metric/collector.h"

namespace tera {

enum class TcmallocMetricType {
  kInUse,
  kCentralCacheFreeList,
  kThreadCacheFreeList,
};

class TcmallocCollector : public Collector {
 public:
  explicit TcmallocCollector(const TcmallocMetricType& type) : type_(type) {}
  int64_t Collect() override;

 private:
  TcmallocMetricType type_;
  int64_t val_ = 0;
  int64_t last_check_ms_ = 0;
};

inline void RegisterTcmallocCollectors() {
  auto& instance = CollectorReportPublisher::GetInstance();

  instance.AddCollector(
      MetricId("tcmalloc_allocated_bytes"),
      std::unique_ptr<Collector>(new TcmallocCollector(TcmallocMetricType::kInUse)),
      {SubscriberType::LATEST});
  instance.AddCollector(
      MetricId("tcmalloc_central_free_list_bytes"),
      std::unique_ptr<Collector>(new TcmallocCollector(TcmallocMetricType::kCentralCacheFreeList)),
      {SubscriberType::LATEST});
  instance.AddCollector(
      MetricId("tcmalloc_thread_free_list_bytes"),
      std::unique_ptr<Collector>(new TcmallocCollector(TcmallocMetricType::kThreadCacheFreeList)),
      {SubscriberType::LATEST});
}
}  // namespace tera
