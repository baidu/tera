// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_METRIC_METRICS_H_
#define TERA_COMMON_METRIC_METRICS_H_

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <initializer_list>

#include "common/metric/metric_id.h"
#include "common/metric/collector_report.h"
#include "common/metric/collector.h"
#include "common/metric/subscriber.h"

namespace tera {
// Base class for metric value collector

using SubscriberTypeList = std::initializer_list<SubscriberType>;

class CollectorReportPublisher {
 private:
  // set private for singleton
  CollectorReportPublisher();
  ~CollectorReportPublisher();

  // disallow copy
  CollectorReportPublisher(const CollectorReportPublisher&) = delete;
  CollectorReportPublisher& operator=(const CollectorReportPublisher&) = delete;

 public:
  static CollectorReportPublisher& GetInstance();

  void Refresh();
  /// report the instant values of collectors
  std::shared_ptr<CollectorReport> GetCollectorReport();
  std::shared_ptr<SubscriberReport> GetSubscriberReport();

  /// Add a collector with a given metric_id
  /// collector should be a right value reference of std::unique_ptr<Collector>
  /// return true if register success,
  /// retrun false if argument is invalid or metric_id name has been registered
  /// already.
  bool AddCollector(const MetricId& metric_id, std::unique_ptr<Collector>&& metric_collector,
                    SubscriberTypeList type_list = {SubscriberType::LATEST});

  /// weather a collector has been Added
  bool HasCollector(const MetricId& metric_id) const;
  /// Delete a collector
  bool DeleteCollector(const MetricId& metric_id);

  /// Add a subscriber to a given metricId.
  /// Different type of subscribers can be registered to a same metricId.
  bool AddSubscriber(std::unique_ptr<Subscriber>&& subscriber);
  /// Delete a subscriber
  bool DeleteSubscriber(const MetricId& metric_id);
  void DeleteSubscribers();

 private:
  void NotifySubscribers();
  void AddHardwareCollectors();

 private:
  mutable std::recursive_mutex mutex_;

  using CollectorMap = std::unordered_map<MetricId, std::unique_ptr<Collector>>;

  using SubscriberMap = std::unordered_multimap<MetricId, std::unique_ptr<Subscriber>>;
  CollectorMap collectors_;
  SubscriberMap subscribers_;

  int64_t last_report_timestamp_;

  std::shared_ptr<CollectorReport> last_collector_report_;
};

class AutoCollectorRegister {
 public:
  AutoCollectorRegister(const MetricId& id, std::unique_ptr<Collector>&& collector,
                        SubscriberTypeList type_list = {SubscriberType::LATEST})
      : registered_(false), id_(id) {
    registered_ =
        CollectorReportPublisher::GetInstance().AddCollector(id_, std::move(collector), type_list);
  }

  // create a metric with empty label
  AutoCollectorRegister(const std::string& name, std::unique_ptr<Collector>&& collector,
                        SubscriberTypeList type_list = {SubscriberType::LATEST})
      : registered_(false), id_(name) {
    if (name.empty()) {
      throw std::invalid_argument("name");
    }
    registered_ =
        CollectorReportPublisher::GetInstance().AddCollector(id_, std::move(collector), type_list);
  }

  // create a metric with name and label
  // label_str format: k1:v1,k2:v2,...
  // can build by LabelStringBuilder().Append("k1",
  // "v1").Append("k2","v2").ToString();
  AutoCollectorRegister(const std::string& name, const std::string& label_str,
                        std::unique_ptr<Collector>&& collector,
                        SubscriberTypeList type_list = {SubscriberType::LATEST})
      : registered_(false) {
    // parse metric id
    MetricId::ParseFromStringWithThrow(name, label_str, &id_);
    registered_ =
        CollectorReportPublisher::GetInstance().AddCollector(id_, std::move(collector), type_list);
  }

  ~AutoCollectorRegister() {
    if (registered_) {
      CollectorReportPublisher::GetInstance().DeleteCollector(id_);
    }
  }

  const MetricId& GetId() const { return id_; }

  bool IsRegistered() const { return registered_; }

 private:
  bool registered_;
  MetricId id_;
};

class AutoSubscriberRegister {
 public:
  AutoSubscriberRegister(std::unique_ptr<Subscriber>&& subscriber_ptr) : registered_(false) {
    if (subscriber_ptr) {
      metric_id_ = subscriber_ptr->GetMetricId();
      registered_ =
          CollectorReportPublisher::GetInstance().AddSubscriber(std::move(subscriber_ptr));
    }
  }
  ~AutoSubscriberRegister() {
    if (registered_) {
      CollectorReportPublisher::GetInstance().DeleteSubscriber(metric_id_);
    }
  }

 private:
  bool registered_;
  MetricId metric_id_;
};
}  // end namespace tera

#endif  // TERA_COMMON_METRIC_METRICS_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
