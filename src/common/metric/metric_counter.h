#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdexcept>
#include <string>

#include "common/metric/collector_report_publisher.h"
#include "common/metric/counter_collector.h"
#include "common/counter.h"

namespace tera{
class MetricCounter : public Counter {
public:
    // create a metric with empty label
    explicit MetricCounter(const std::string& name,
                           SubscriberTypeList type_list = {SubscriberType::LATEST},
                           bool is_periodic = true):
            Counter(),
            registered_(false),
            metric_id_(name),
            type_list_(type_list),
            is_periodic_(is_periodic) {
        if (name.empty()) {
            // throw a exception and make process exit with coredump
            throw std::invalid_argument("metric name is empty");
        }
        registered_ = CollectorReportPublisher::GetInstance().AddCollector(
            metric_id_,
            std::unique_ptr<Collector>(new CounterCollector(this, is_periodic_)),
            type_list_);
    }

    // create a metric with name and label
    // label_str format: k1:v1,k2:v2,...
    // can build by LabelStringBuilder().Append("k1", "v1").Append("k2","v2").ToString();
    MetricCounter(const std::string& name,
                  const std::string& label_str,
                  SubscriberTypeList type_list = {SubscriberType::LATEST},
                  bool is_periodic = true):
            Counter(),
            registered_(false),
            type_list_(type_list),
            is_periodic_(is_periodic) {
        // parse metric id
        MetricId::ParseFromStringWithThrow(name, label_str, &metric_id_);
        // legal label str format, do register
        registered_ = CollectorReportPublisher::GetInstance().AddCollector(
            metric_id_,
            std::unique_ptr<Collector>(new CounterCollector(this, is_periodic_)),
            type_list);
    }

    MetricCounter(MetricCounter&& counter) {
        // parse metric id
        if (counter.registered_) {
            CollectorReportPublisher::GetInstance().DeleteCollector(counter.metric_id_);
        }
        registered_ = counter.registered_;
        metric_id_ = counter.metric_id_;
        is_periodic_ = counter.is_periodic_;
        type_list_ = counter.type_list_;
        Set(counter.Get());
        counter.registered_ = false;
        registered_ = CollectorReportPublisher::GetInstance().AddCollector(
            metric_id_,
            std::unique_ptr<Collector>(new CounterCollector(this, is_periodic_)),
            type_list_);
    }

    virtual ~MetricCounter() {
        if (registered_) {
            // do unregister
            CollectorReportPublisher::GetInstance().DeleteCollector(metric_id_);
        }
    }

    bool IsRegistered() const {
        return registered_;
    }

    //Never copyied
    MetricCounter(const MetricCounter&) = delete;
    MetricCounter& operator=(const MetricCounter&) = delete;

private:
    bool registered_;
    MetricId metric_id_;
    SubscriberTypeList type_list_;
    bool is_periodic_;
};
}
