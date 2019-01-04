// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "common/metric/prometheus_subscriber.h"
#include "common/metric/collector_report.h"

DEFINE_int64(tera_metric_hold_max_time, 300000,
             "interval of prometheus collectors push a value to hold_queue in ms");
DEFINE_bool(tera_prometheus_subscriber_dump_log, false,
            "Whether to dump prometheus subscriber log");

namespace tera {

void PrometheusSubscriber::OnUpdate(std::shared_ptr<CollectorReport> report) {
  int64_t value = report->FindMetricValue(metric_id_);
  Append(report->timestamp_ms, value);
}

ReportItem PrometheusSubscriber::Collect() {
  ReportItem ret;
  std::shared_ptr<TimeValueQueue> tera_queue_ptr;
  int64_t last_collect_ts;

  {
    std::lock_guard<std::mutex> lock_mtx(mtx_);
    if (tera_queue_ptr_->empty()) {
      LOG_IF(WARNING, FLAGS_tera_prometheus_subscriber_dump_log)
          << "[PROMETHEUS SUBSCRIBER] Empty Tera Queue";
      return ret;
    }

    last_collect_ts = last_collect_ts_;
    last_collect_ts_ = tera_queue_ptr_->back().first;
    tera_queue_ptr = tera_queue_ptr_;
    tera_queue_ptr_.reset(new TimeValueQueue);
  }

  int64_t value = GetSpecificValue(tera_queue_ptr);

  if (type_ == SubscriberType::QPS || type_ == SubscriberType::THROUGHPUT) {
    int64_t time_interval = tera_queue_ptr->back().first - last_collect_ts;
    value = (time_interval != 0 ? value * 1000 / time_interval : 0);
  }

  ret.SetTimeValue({tera_queue_ptr->back().first, value});
  ret.SetType(GetTypeName());

  return ret;
}

void PrometheusSubscriber::Append(int64_t time_stamp, int64_t current_value) {
  std::lock_guard<std::mutex> mtx_lock(mtx_);
  tera_queue_ptr_->emplace_back(time_stamp, current_value);
  LOG_IF(WARNING, FLAGS_tera_prometheus_subscriber_dump_log)
      << "[PROMETHEUS APPEND] " << metric_id_.GetName() << "\tValue: " << current_value
      << "\tQueue Size:" << tera_queue_ptr_->size();
  if (has_inited_) {
    DropExpiredValue();
  } else {
    last_collect_ts_ = time_stamp;
    has_inited_ = true;
  }
}

std::string PrometheusSubscriber::GetTypeName() {
  switch (type_) {
    case SubscriberType::LATEST:
      return "Latest";

    case SubscriberType::MAX:
      return "Max";

    case SubscriberType::MIN:
      return "Min";

    case SubscriberType::SUM:
      return "Sum";

    case SubscriberType::QPS:
      return "Qps";

    case SubscriberType::THROUGHPUT:
      return "ThroughPut";

    default:
      LOG(ERROR) << "Unknown collector type: ";
      abort();
  }
  // Never reach here
  return "";
}

void PrometheusSubscriber::DropExpiredValue() {
  if (tera_queue_ptr_->empty()) {
    return;
  }

  auto last_enqueue_ts = tera_queue_ptr_->back().first;
  int64_t drop_cnt = 0;
  while (last_enqueue_ts - tera_queue_ptr_->front().first >= FLAGS_tera_metric_hold_max_time) {
    VLOG(30) << "[PROMETHEUS SUBSCRIBER] drop last_enqueue_ts: " << last_enqueue_ts
             << "first_ts: " << tera_queue_ptr_->front().first << "name: " << metric_id_.GetName();

    ++drop_cnt;
    last_collect_ts_ = tera_queue_ptr_->front().first;
    tera_queue_ptr_->pop_front();
  }

  if (drop_cnt != 0) {
    VLOG(30) << "[PROMETHEUS SUBSCRIBER] drop " << drop_cnt << "values";
  }
}

int64_t PrometheusSubscriber::GetSpecificValue(std::shared_ptr<TimeValueQueue> tera_queue_ptr) {
  switch (type_) {
    case SubscriberType::LATEST:
      return GetLatest(tera_queue_ptr);

    case SubscriberType::MAX:
      return GetMax(tera_queue_ptr);

    case SubscriberType::MIN:
      return GetMin(tera_queue_ptr);

    // Both of SUM, Qps, and THROUGHPUT use GetSum here
    case SubscriberType::SUM:
    case SubscriberType::QPS:
    case SubscriberType::THROUGHPUT:
      return GetSum(tera_queue_ptr);

    default:
      LOG(ERROR) << "Unknown collector type";
      abort();
  }
  // Never reach here
  return -1;
}
}
