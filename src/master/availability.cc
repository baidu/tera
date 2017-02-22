// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/availability.h"

#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/timer.h"
#include "utils/string_util.h"

DECLARE_bool(tera_master_availability_show_details_enabled);
DECLARE_int64(tera_master_availability_error_threshold);
DECLARE_int64(tera_master_availability_fatal_threshold);
DECLARE_int64(tera_master_availability_warning_threshold);
DECLARE_int64(tera_master_not_available_threshold);
DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);

namespace tera {
namespace master {

TabletAvailability::TabletAvailability(boost::shared_ptr<TabletManager> t)
    : tablet_manager_(t) {
    start_ts_ = get_micros();
}

void TabletAvailability::AddNotReadyTablet(const std::string& path) {
    MutexLock lock(&mutex_);
    int64_t ts = get_micros();
    tablets_.insert(std::pair<std::string, int64_t>(path, ts));

    tablets_hist_cost_[path].start_ts = ts;
    tablets_hist_cost_[path].notready_num++;
}

void TabletAvailability::EraseNotReadyTablet(const std::string& path) {
    MutexLock lock(&mutex_);
    tablets_.erase(path);

    int64_t ts = get_micros();
    if (tablets_hist_cost_[path].start_ts > 0) {
        tablets_hist_cost_[path].total += ts - tablets_hist_cost_[path].start_ts;
    } else {
        tablets_hist_cost_[path].total += ts - start_ts_;
    }
    tablets_hist_cost_[path].start_ts = 0;
    tablets_hist_cost_[path].reready_num++;
}

static std::string GetNameFromPath(const std::string& path) {
    if (path == FLAGS_tera_master_meta_table_path) {
        return FLAGS_tera_master_meta_table_name;
    }
    std::vector<std::string> t;
    SplitString(path, "/", &t); // table_name/tablet00...001
    return t[0];
}

void TabletAvailability::LogAvailability() {
    MutexLock lock(&mutex_);
    int64_t not_avai_count = 0;
    int64_t not_avai_warning = 0;
    int64_t not_avai_error = 0;
    int64_t not_avai_fatal = 0;
    int64_t start = ::common::timer::get_micros();
    std::map<std::string, int64_t>::iterator it;
    for (it = tablets_.begin(); it != tablets_.end(); ++it) {
        std::string table_name = GetNameFromPath(it->first);
        TablePtr table;
        if (!tablet_manager_->FindTable(table_name, &table)) {
            LOG(ERROR) << "[availability] unknown table:" << table_name;
            continue;
        }
        if (table->GetStatus() != kTableEnable) {
            continue;
        }

        if ((start - it->second) > FLAGS_tera_master_not_available_threshold * 1000 * 1000LL) {
            VLOG(12) << "[availability] not available:" << it->first;
            not_avai_count++;
        }
        if ((start - it->second) > FLAGS_tera_master_availability_fatal_threshold * 1000 * 1000LL) {
            not_avai_fatal++;
            if (FLAGS_tera_master_availability_show_details_enabled) {
                LOG(INFO) << "[availability] fatal-tablet:" << it->first;
            }
        } else if ((start - it->second) > FLAGS_tera_master_availability_error_threshold * 1000 * 1000LL) {
            not_avai_error++;
            if (FLAGS_tera_master_availability_show_details_enabled) {
                LOG(INFO) << "[availability] error-tablet:" << it->first;
            }
        } else if ((start - it->second) > FLAGS_tera_master_availability_warning_threshold * 1000 * 1000LL) {
            not_avai_warning++;
        }
    }

    int64_t all_tablets = tablet_manager_->GetAllTabletsCount();
    LOG(INFO) << "[availability][current-status] fatal=" << not_avai_fatal
        << " f-ratio=" << RoundNumberToNDecimalPlaces((double)not_avai_fatal/all_tablets, 6)
        << ", error=" << not_avai_error
        << " e-ratio=" << RoundNumberToNDecimalPlaces((double)not_avai_error/all_tablets, 6)
        << ", warn=" << not_avai_warning
        << " w-ratio=" << RoundNumberToNDecimalPlaces((double)not_avai_warning/all_tablets, 6);

    LOG(INFO) << "[availability][current-status] (not-available/not-ready/all-tablets: "
        << not_avai_count << "/" << tablets_.size() << "/" << all_tablets << ")"
        << " available tablets percentage: " << 1 - not_avai_count/(double)all_tablets;

    int64_t total_time = 0, all_time = start - start_ts_;
    start_ts_ = start;
    int64_t nr_notready_tablets = tablets_hist_cost_.size();
    int64_t total_notready = 0, total_reready = 0;
    std::map<std::string, TimeStatistic>::iterator stat_it;
    for (stat_it = tablets_hist_cost_.begin();
         stat_it != tablets_hist_cost_.end();) {
        if (stat_it->second.start_ts > 0) {
            stat_it->second.total += start - stat_it->second.start_ts;
        }

        total_time += stat_it->second.total;
        total_notready += stat_it->second.notready_num;
        total_reready += stat_it->second.reready_num;

        if (stat_it->second.start_ts > 0) {
            stat_it->second.total = 0;
            stat_it->second.start_ts = start;
            stat_it->second.notready_num = 1;
            stat_it->second.reready_num = 0;
            ++stat_it;
        } else {
            tablets_hist_cost_.erase(stat_it++);
        }
    }
    LOG(INFO) << "[availability][tablet_staticstic] time_interval: " << all_time / 1000
      << ", notready_time: " << total_time / 1000
      << ", total_time: " << (all_time * all_tablets) / 1000
      << ", ready_time_percent: " << RoundNumberToNDecimalPlaces(1.0 - (double)total_time / (all_time * all_tablets + 1), 6)
      << ", notready_tablets: " << nr_notready_tablets
      << ", total_tabltes: " << all_tablets
      << ", ready_tablets_percent: " << RoundNumberToNDecimalPlaces(1.0 - (double)nr_notready_tablets / (all_tablets + 1), 6)
      << ", notready_count: " << total_notready
      << ", reready_count: " << total_reready;

    int64_t cost = ::common::timer::get_micros() - start;
    LOG(INFO) << "[availability] cost time:" << cost/1000 << " ms";
}

} // master
} // tera
