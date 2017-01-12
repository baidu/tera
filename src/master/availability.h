// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_TABLET_AVAILABILITY_H_
#define TERA_MASTER_TABLET_AVAILABILITY_H_

#include <string>

#include "master/tablet_manager.h"

#include "common/mutex.h"

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
    void AddNotReadyTablet(const std::string& id);
    void EraseNotReadyTablet(const std::string& id);

private:
    Mutex mutex_;
    std::shared_ptr<TabletManager> tablet_manager_;
    std::map<std::string, int64_t> tablets_;

    int64_t start_ts_;
    std::map<std::string, TimeStatistic> tablets_hist_cost_;
};

} // master
} // tera

#endif // TERA_MASTER_TABLET_AVAILABILITY_H_
