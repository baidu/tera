// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/availability.h"

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/timer.h"

DECLARE_int64(tera_master_not_available_threshold);
DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);

namespace tera {
namespace master {

void TabletAvailability::AddNotReadyTablet(const std::string& path) {
    MutexLock lock(&mutex_);
    tablets_.insert(std::pair<std::string, int64_t>(path, get_micros()));
}

void TabletAvailability::EraseNotReadyTablet(const std::string& path) {
    MutexLock lock(&mutex_);
    tablets_.erase(path);
}

static std::string GetNameFromPath(const std::string& path) {
    if (path == FLAGS_tera_master_meta_table_path) {
        return FLAGS_tera_master_meta_table_name;
    }
    std::vector<std::string> t;
    SplitString(path, "/", &t); // table_name/tablet00...001
    return t[0];
}

double TabletAvailability::GetAvailability() {
    MutexLock lock(&mutex_);
    int64_t not_avai_count = 0;
    int64_t start = ::common::timer::get_micros();
    std::map<std::string, int64_t>::iterator it;
    for (it = tablets_.begin(); it != tablets_.end(); ++it) {
        std::string table_name = GetNameFromPath(it->first);
        TablePtr table;
        if (!tablet_manager_->FindTable(table_name, &table)) {
            LOG(ERROR) << "[availability] unknown table:" << table_name;
            continue;
        }
        if ((table->GetStatus() == kTableEnable)
            && (start - it->second > FLAGS_tera_master_not_available_threshold * 1000 * 1000)) {
            VLOG(12) << "[availability] not available:" << it->first;
            not_avai_count++;
        }
    }
    int64_t cost = ::common::timer::get_micros() - start;
    int64_t all_tablets = tablet_manager_->GetAllTabletsCount();
    LOG(INFO) << "[availability][current-status] (not-available/not-ready/all-tablets: "
        << not_avai_count << "/" << tablets_.size() << "/" << all_tablets << ")"
        << " cost time:" << cost/1000 << " ms";
    return 1 - not_avai_count/(double)all_tablets;
}

} // master
} // tera
