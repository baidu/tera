#pragma once
// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <memory>
#include <string>
#include <unordered_map> 
#include "common/metric/metric_id.h"

namespace tera {

using TimeValuePair = std::pair<int64_t, int64_t>;

class CollectorReport;

struct ReportItem {
    TimeValuePair time_value_pair;
    std::string type;
    ReportItem(TimeValuePair tvp = {-1, -1}, const std::string& t = ""):
        time_value_pair(tvp),
        type(t) { }

    int64_t Value() const {
        return time_value_pair.second;
    }

    int64_t Time() const {
        return time_value_pair.first;
    }

    void SetTimeValue(const TimeValuePair& tvp) {
        time_value_pair = tvp;
    }

    void SetType(const std::string& tp) {
        type = tp;
    }

    std::string Type() const {
        return type;
    }
};

class Subscriber {
public:
    enum class SubscriberType {
        LATEST,
        MAX,
        MIN,
        QPS,
        SUM,
        THROUGHPUT
    };
    virtual ~Subscriber() {}
    // return a pair of <timestamp, aggregate value> to Prometheus 
    virtual ReportItem Collect() = 0;
    // Update subscriber, depends to subscriber type
    // Called in CollectorReportPublisher::Report()
    virtual void OnUpdate(const std::shared_ptr<CollectorReport>) = 0;
    virtual std::string GetTypeName() = 0;
    virtual const MetricId& GetMetricId() = 0;
};

using SubscriberType = Subscriber::SubscriberType;
using SubscriberReport = std::unordered_multimap<MetricId, ReportItem>;
}