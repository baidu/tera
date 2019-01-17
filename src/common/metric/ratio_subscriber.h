#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include "common/metric/subscriber.h"
#include <cassert>
#include <cmath>

namespace tera {
class RatioSubscriber : public Subscriber {
 public:
  RatioSubscriber(const MetricId& metric_id, std::unique_ptr<Subscriber>&& subscriber1,
                  std::unique_ptr<Subscriber>&& subscriber2)
      : metric_id_(metric_id),
        subscriber1_(std::move(subscriber1)),
        subscriber2_(std::move(subscriber2)) {
    type_name_ = "Ratio: (" + subscriber1_->GetMetricId().GetName() + ":" +
                 subscriber1_->GetTypeName() + " / " + subscriber2_->GetMetricId().GetName() + ":" +
                 subscriber2_->GetTypeName() + ")";
  }

  virtual std::string GetTypeName() override { return type_name_; }

  virtual void OnUpdate(const std::shared_ptr<CollectorReport> report_ptr) override {
    subscriber1_->OnUpdate(report_ptr);
    subscriber2_->OnUpdate(report_ptr);
  }

  virtual ReportItem Collect() override {
    ReportItem ret;
    auto subscriber1_ret = subscriber1_->Collect();
    auto subscriber2_ret = subscriber2_->Collect();
    // timestamp should be equal;
    assert(subscriber1_ret.Time() == subscriber2_ret.Time());
    double ratio = (double)subscriber1_ret.Value() / subscriber2_ret.Value();
    ret.SetTimeValue({subscriber1_ret.Time(), (isnan(ratio) ? -1 : static_cast<int64_t>(ratio))});
    ret.SetType(GetTypeName());
    return ret;
  }

  const MetricId& GetMetricId() override { return metric_id_; }

  virtual ~RatioSubscriber() override {}

 private:
  MetricId metric_id_;
  std::unique_ptr<Subscriber> subscriber1_;
  std::unique_ptr<Subscriber> subscriber2_;
  std::string type_name_;
};
}
