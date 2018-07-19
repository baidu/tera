// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLET_AVAILABILITY_H_
#define TERA_MASTER_TABLET_AVAILABILITY_H_

#include <string>
#include <map>

#include "master/tablet_manager.h"

#include "common/mutex.h"
#include "common/metric/metric_counter.h"

namespace tera {
namespace master {

struct TimeStatistic {
    int64_t total;
    int64_t start_ts;
    int64_t notready_num;
    int64_t reready_num;
    TimeStatistic() : total(0), start_ts(0), notready_num(0), reready_num(0) {}
};
class TabletAvailability {
public:
    TabletAvailability(std::shared_ptr<TabletManager> t);
    void LogAvailability();
    void AddNotReadyTablet(const std::string& path,
                           const TabletMeta::TabletStatus& tablet_status);
    void EraseNotReadyTablet(const std::string& id);

private:

    enum class TabletErrorStatus {
        kNotReady = 1,
        kFatal = 2,
        kError = 3,
        kWarning = 4
    };

    Mutex mutex_;
    std::shared_ptr<TabletManager> tablet_manager_;

    std::map<std::string, int64_t> tablets_;
    std::map<std::string, MetricCounter> not_ready_tablet_metrics_;
    MetricCounter ready_time_percent{"tera_master_tablet_ready_time_percent",
                                     {SubscriberType::LATEST},
                                     false};

    int64_t start_ts_;
    std::map<std::string, TimeStatistic> tablets_hist_cost_;
    const std::string metric_name_{"tera_master_tablet_availability"};
};

} // master
} // tera

#endif // TERA_MASTER_TABLET_AVAILABILITY_H_
