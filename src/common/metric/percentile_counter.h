#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include "common/metric/collector.h"
#include "common/metric/subscriber.h"
#include "common/metric/collector_report_publisher.h"
#include "leveldb/util/histogram.h"

namespace tera {

class PercentileCounter;

class PercentileCollector : public Collector {
 public:
  virtual ~PercentileCollector() {}
  // return a instant value of the metric for tera to dump log and other usage
  PercentileCollector(PercentileCounter* pc) : pc_(pc){};

  inline virtual int64_t Collect() override;

 private:
  PercentileCounter* pc_;
};

class PercentileCounter {
 public:
  // create a metric with name and label
  // label_str format: k1:v1,k2:v2,...
  // can build by LabelStringBuilder().Append("k1",
  // "v1").Append("k2","v2").ToString();
  PercentileCounter(const std::string& name, const std::string& label_str, double percentile,
                    SubscriberTypeList type_list = {SubscriberType::LATEST})
      : percentile_(percentile), registered_(false) {
    // parse metric id
    MetricId::ParseFromStringWithThrow(name, label_str, &metric_id_);
    // legal label str format, do register
    registered_ = CollectorReportPublisher::GetInstance().AddCollector(
        metric_id_, std::unique_ptr<Collector>(new PercentileCollector(this)), type_list);
  }

  PercentileCounter(const std::string& name, double percentile,
                    SubscriberTypeList type_list = {SubscriberType::LATEST})
      : percentile_(percentile), registered_(false) {
    // parse metric id
    MetricId::ParseFromStringWithThrow(name, "", &metric_id_);
    // legal label str format, do register
    registered_ = CollectorReportPublisher::GetInstance().AddCollector(
        metric_id_, std::unique_ptr<Collector>(new PercentileCollector(this)), type_list);
  }

  virtual ~PercentileCounter() {
    if (registered_) {
      // do unregister
      CollectorReportPublisher::GetInstance().DeleteCollector(metric_id_);
    }
  }

  bool IsRegistered() const { return registered_; }

  int64_t Get() {
    double percentile_value = hist_.Percentile(percentile_);
    if (isnan(percentile_value)) {
      return -1;
    }
    return (int64_t)percentile_value;
  }

  void Clear() { hist_.Clear(); }

  void Append(int64_t v) { hist_.Add((double)v); }

  // Never copyied
  PercentileCounter(const PercentileCounter&) = delete;
  PercentileCounter& operator=(const PercentileCounter&) = delete;

 private:
  double percentile_;
  bool registered_;
  MetricId metric_id_;
  leveldb::Histogram hist_;
};

int64_t PercentileCollector::Collect() {
  int64_t val = (int64_t)pc_->Get();
  pc_->Clear();
  return val;
}
}