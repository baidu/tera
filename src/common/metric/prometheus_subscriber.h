#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <memory>
#include <deque>
#include <utility>
#include <algorithm>
#include <numeric>
#include <mutex>
#include <string>

#include "common/metric/subscriber.h"

namespace tera {

using TimeValueQueue = std::deque<TimeValuePair>;

class PrometheusSubscriber : public Subscriber {
 public:
  PrometheusSubscriber(const MetricId& metric_id, SubscriberType type = SubscriberType::LATEST)
      : tera_queue_ptr_(new TimeValueQueue),
        last_collect_ts_(0),
        has_inited_(false),
        type_(type),
        metric_id_(metric_id) {}

  ~PrometheusSubscriber() override {}
  ReportItem Collect() override;
  void OnUpdate(const std::shared_ptr<CollectorReport>) override;

  std::string GetTypeName() override;

  const MetricId& GetMetricId() override { return metric_id_; }

 private:
  void Append(int64_t time_stamp, int64_t current_value);
  void DropExpiredValue();
  int64_t GetSpecificValue(std::shared_ptr<TimeValueQueue>);

  int64_t GetMax(std::shared_ptr<TimeValueQueue> tera_queue_ptr) {
    return std::max_element(tera_queue_ptr->begin(), tera_queue_ptr->end(),
                            [](const TimeValuePair& x, const TimeValuePair& y) {
                              return x.second < y.second;
                            })->second;
  }

  int64_t GetMin(std::shared_ptr<TimeValueQueue> tera_queue_ptr) {
    return std::min_element(tera_queue_ptr->begin(), tera_queue_ptr->end(),
                            [](const TimeValuePair& x, const TimeValuePair& y) {
                              return x.second < y.second;
                            })->second;
  }

  int64_t GetLatest(std::shared_ptr<TimeValueQueue> tera_queue_ptr) {
    return tera_queue_ptr->back().second;
  }

  int64_t GetSum(std::shared_ptr<TimeValueQueue> tera_queue_ptr) {
    return std::accumulate(
        tera_queue_ptr->begin(), tera_queue_ptr->end(), (int64_t)0,
        [](const int64_t val, const TimeValuePair& x) { return val + x.second; });
  }

  std::mutex mtx_;
  // queue of tera timestamp-value
  std::shared_ptr<TimeValueQueue> tera_queue_ptr_;
  // timestamp of prometheus_queue_ptr_'s last enqueue operation
  int64_t last_collect_ts_;
  // Is this class inited?
  bool has_inited_;
  // subscriber type
  const SubscriberType type_;
  MetricId metric_id_;
};
}