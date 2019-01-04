#pragma once
// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
#include <memory>
#include <string>
#include <unordered_map>

#include "common/metric/metric_id.h"
#include "common/mutex.h"
#include "common/metric/collector.h"
#include "common/metric/subscriber.h"

namespace tera {

using CollectorReportMap = std::unordered_map<MetricId, int64_t>;

struct CollectorReport {
  int64_t timestamp_ms;  // timestamp of the report
  int64_t interval_ms;   // time interval since last report

  // metric_id to metric snapshot
  CollectorReport() : timestamp_ms(get_millis()) {}

  // find methods, return 0 if not found
  int64_t FindMetricValue(const MetricId& metric_id) const {
    auto iter = report.find(metric_id);
    return iter == report.end() ? 0 : iter->second;
  };

  int64_t FindMetricValue(const std::string& metric_name) const {
    return FindMetricValue(MetricId(metric_name));
  }

  int64_t FindMetricValue(const std::string& metric_name, const std::string& label_str) const {
    MetricId metric_id;
    if (!MetricId::ParseFromString(metric_name, label_str, &metric_id)) {
      return 0;
    } else {
      return FindMetricValue(metric_id);
    }
  }

  CollectorReportMap report;
};
}  // end namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
