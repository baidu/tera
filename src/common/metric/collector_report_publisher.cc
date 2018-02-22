// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. 

#include "common/metric/collector_report_publisher.h" 

#include "glog/logging.h"

#include "common/metric/hardware_collectors.h"
#include "common/timer.h"
#include "common/metric/collector.h"
#include "common/metric/prometheus_subscriber.h"
 
namespace tera {

CollectorReportPublisher& CollectorReportPublisher::GetInstance() {
    static CollectorReportPublisher instance;
    return instance;
}

CollectorReportPublisher::CollectorReportPublisher():
    last_report_timestamp_(get_millis()),
    last_collector_report_(new CollectorReport) {
    AddHardwareCollectors();
}

CollectorReportPublisher::~CollectorReportPublisher() {}

std::shared_ptr<SubscriberReport> CollectorReportPublisher::GetSubscriberReport() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    std::shared_ptr<SubscriberReport> new_report(new SubscriberReport());
    int64_t start_ts = get_millis();
    // do collect
    for (auto& subscriber_pair : subscribers_) {
        const MetricId& metric_id = subscriber_pair.first;
        new_report->insert(std::make_pair(metric_id, subscriber_pair.second->Collect()));
    }

    int64_t end_ts = get_millis();
    VLOG(12) << "[Metric] Get Subscriber Summary Cost: " << (end_ts - start_ts) << " ms.";
    return new_report;
}

std::shared_ptr<CollectorReport> CollectorReportPublisher::GetCollectorReport() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return last_collector_report_;
}

void CollectorReportPublisher::Refresh() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    std::shared_ptr<CollectorReport> new_report(new CollectorReport());
    int64_t start_ts = new_report->timestamp_ms;
    new_report->interval_ms = new_report->timestamp_ms - last_report_timestamp_;

    // do collect
    for (auto& metric_pair : collectors_) {
        const MetricId& metric_id = metric_pair.first;
        int64_t value = metric_pair.second->Collect();
        new_report->report[metric_id] = value;
    }

    last_report_timestamp_ = start_ts;
    int64_t end_ts = get_millis();
    VLOG(12) << "[Metric] Refresh Collectors Cost: " << (end_ts - start_ts) << " ms.";
    last_collector_report_ = new_report;
    NotifySubscribers();
}

bool CollectorReportPublisher::AddCollector(const MetricId& metric_id, 
                                          std::unique_ptr<Collector>&& metric_collector,
                                          SubscriberTypeList type_list) {
    if (!metric_id.IsValid() || !metric_collector) {
        return false;
    }
    
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto insert_ret = collectors_.insert(std::make_pair(metric_id, std::move(metric_collector)));
    if (!insert_ret.second) {
        return false;
    } 

    for (auto type : type_list) {
        if (!AddSubscriber(std::unique_ptr<Subscriber>(new PrometheusSubscriber(metric_id, type)))) {
            LOG(ERROR) << "[METRIC] Add Subscriber For " << metric_id.ToString() << " Failed!";
        }
    }

    return true;
}

bool CollectorReportPublisher::AddSubscriber(std::unique_ptr<Subscriber>&& prometheus_subscriber_ptr) {
    if (!prometheus_subscriber_ptr || 
        !prometheus_subscriber_ptr->GetMetricId().IsValid()) {
        // invalid arguments
        return false;
    }

    std::lock_guard<std::recursive_mutex> lock(mutex_);
    subscribers_.insert(std::make_pair(prometheus_subscriber_ptr->GetMetricId(), 
                                       std::move(prometheus_subscriber_ptr)));

    return true;
}

void CollectorReportPublisher::NotifySubscribers() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& subscriber_pair : subscribers_) {
        subscriber_pair.second->OnUpdate(last_collector_report_);
    }
}

bool CollectorReportPublisher::HasCollector(const MetricId& metric_id) const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return collectors_.find(metric_id) != collectors_.end();
}

bool CollectorReportPublisher::DeleteCollector(const MetricId& metric_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    DeleteSubscriber(metric_id);
    return collectors_.erase(metric_id) > 0;
}

bool CollectorReportPublisher::DeleteSubscriber(const MetricId& metric_id) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return subscribers_.erase(metric_id) > 0;
}

void CollectorReportPublisher::DeleteSubscribers() {
    subscribers_.clear();
}

void CollectorReportPublisher::AddHardwareCollectors() {
    // register hardware metrics
    AddCollector(MetricId(kInstCpuMetricName), std::unique_ptr<Collector>(new CpuUsageCollector()));
    AddCollector(MetricId(kInstMemMetricName), std::unique_ptr<Collector>(new MemUsageCollector()));
    
    AddCollector(MetricId(kInstNetRXMetricName), 
                      std::unique_ptr<Collector>(new NetUsageCollector(RECEIVE)), 
                      {SubscriberType::MAX});

    AddCollector(MetricId(kInstNetTXMetricName), 
                      std::unique_ptr<Collector>(new NetUsageCollector(TRANSMIT)), 
                      {SubscriberType::MAX});
}
} // end namespace tera
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

