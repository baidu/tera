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

DEFINE_bool(tera_master_availability_show_details_enabled, false, "whether show details of not-ready tablets"); 
DEFINE_int64(tera_master_availability_error_threshold, 600, "10 minutes, the threshold (in s) of error availability");   
DEFINE_int64(tera_master_availability_fatal_threshold, 3600, "1 hour, the threshold (in s) of fatal availability"); 
DEFINE_int64(tera_master_availability_warning_threshold, 60, "1 minute, the threshold (in s) of warning availability"); 
DEFINE_int64(tera_master_not_available_threshold, 0, "the threshold (in s) of not available");
DECLARE_string(tera_master_meta_table_name);
DECLARE_string(tera_master_meta_table_path);

namespace tera {
namespace master {

static std::string GetNameFromPath(const std::string& path) {
    if (path == FLAGS_tera_master_meta_table_path) {
        return FLAGS_tera_master_meta_table_name;
    }
    std::vector<std::string> t;
    SplitString(path, "/", &t); // table_name/tablet00...001
    if (!t.empty()) {
        return t[0];
    } else {
        return "";
    }
}


TabletAvailability::TabletAvailability(std::shared_ptr<TabletManager> t)
    : tablet_manager_(t) {
    start_ts_ = get_micros();
}

void TabletAvailability::AddNotReadyTablet(const std::string& path,
                                           const TabletMeta::TabletStatus& tablet_status) {
    if (tablet_status == TabletMeta::kTabletReady || tablet_status == TabletMeta::kTabletDisable) {
        return;
    }

    MutexLock lock(&mutex_);
    int64_t ts = get_micros();
    tablets_.insert(std::pair<std::string, int64_t>(path, ts));
    auto iter = not_ready_tablet_metrics_.emplace(
        path,
        MetricCounter{
            metric_name_,
            "table:" + GetNameFromPath(path) + ",tablet:" + path,
            {SubscriberType::LATEST},
            false
        });

    if (iter.second) {
        VLOG(12) << "[Add NotReady To Metric]: " << static_cast<int64_t>(TabletErrorStatus::kNotReady);
        iter.first->second.Set(static_cast<int64_t>(TabletErrorStatus::kNotReady));
    } else {
        VLOG(12) << "[Add NotReady To Metric Failed]: " << static_cast<int64_t>(TabletErrorStatus::kNotReady);
    }

    if (tablets_hist_cost_[path].start_ts > 0) {
        VLOG(10) << "notready again " << path;
        return;
    }

    tablets_hist_cost_[path].start_ts = ts;
    tablets_hist_cost_[path].notready_num++;
    VLOG(10) << "addnotready " << path
        << ", total_cost " << tablets_hist_cost_[path].total
        << ", start_ts " << tablets_hist_cost_[path].start_ts
        << ", notready " << tablets_hist_cost_[path].notready_num
        << ", reready " << tablets_hist_cost_[path].reready_num;
}

void TabletAvailability::EraseNotReadyTablet(const std::string& path) {
    MutexLock lock(&mutex_);
    tablets_.erase(path);
    not_ready_tablet_metrics_.erase(path);

    if (tablets_hist_cost_.find(path) == tablets_hist_cost_.end() ||
        tablets_hist_cost_[path].start_ts == 0) {
        VLOG(10) << "reready again " << path;
        return;
    }

    int64_t ts = get_micros();
    if (tablets_hist_cost_[path].start_ts > 0) {
        tablets_hist_cost_[path].total += ts - tablets_hist_cost_[path].start_ts;
    }
    tablets_hist_cost_[path].start_ts = 0;
    tablets_hist_cost_[path].reready_num++;
    VLOG(10) << "delnotready " << path
        << ", total_cost " << tablets_hist_cost_[path].total
        << ", start_ts " << tablets_hist_cost_[path].start_ts
        << ", notready " << tablets_hist_cost_[path].notready_num
        << ", reready " << tablets_hist_cost_[path].reready_num;
}

void TabletAvailability::LogAvailability() {
    int64_t not_avai_count = 0;
    int64_t not_avai_warning = 0;
    int64_t not_avai_error = 0;
    int64_t not_avai_fatal = 0;
    int64_t start = get_micros();
    std::map<std::string, int64_t> tablets_snapshot;
    std::map<std::string, int64_t>::iterator it;
    std::set<std::string> ignore_tables;
    {
        MutexLock lock(&mutex_);
        tablets_snapshot = tablets_;
    }
    for (it = tablets_snapshot.begin(); it != tablets_snapshot.end(); ++it) {
        std::string table_name = GetNameFromPath(it->first);
        TablePtr table;
        if (!tablet_manager_->FindTable(table_name, &table)) {
            LOG(ERROR) << "[availability] unknown table:" << table_name;
            ignore_tables.insert(it->first);
            continue;
        }
        if (table->GetStatus() != kTableEnable) {
            ignore_tables.insert(it->first);
        }
    }
    int64_t all_tablets = tablet_manager_->GetAllTabletsCount();
    
    MutexLock lock(&mutex_);
    for (it = tablets_.begin(); it != tablets_.end(); ++it) {
        if (ignore_tables.find(it->first) != ignore_tables.end() ) {
            continue;
        }

        auto metric_iter = not_ready_tablet_metrics_.find(it->first);
        assert(metric_iter != not_ready_tablet_metrics_.end());

        if ((start - it->second) > FLAGS_tera_master_not_available_threshold * 1000 * 1000LL) {
            VLOG(12) << "[availability] not available:" << it->first;
            not_avai_count++;
        }
        if ((start - it->second) > FLAGS_tera_master_availability_fatal_threshold * 1000 * 1000LL) {
            not_avai_fatal++;
            metric_iter->second.Set(static_cast<int64_t>(TabletErrorStatus::kFatal));
            if (FLAGS_tera_master_availability_show_details_enabled) {
                LOG(INFO) << "[availability] fatal-tablet:" << it->first;
            }
        } else if ((start - it->second) > FLAGS_tera_master_availability_error_threshold * 1000 * 1000LL) {
            not_avai_error++;
            metric_iter->second.Set(static_cast<int64_t>(TabletErrorStatus::kError));
            if (FLAGS_tera_master_availability_show_details_enabled) {
                LOG(INFO) << "[availability] error-tablet:" << it->first;
            }
        } else if ((start - it->second) > FLAGS_tera_master_availability_warning_threshold * 1000 * 1000LL) {
            not_avai_warning++;
            metric_iter->second.Set(static_cast<int64_t>(TabletErrorStatus::kWarning));
        }
    }

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
    int64_t nr_notready_tablets = tablets_hist_cost_.size();
    double time_percent = 1.0 - (double)total_time / (all_time * all_tablets + 1);
    ready_time_percent.Set(static_cast<int64_t>(time_percent * 10000));

    LOG(INFO) << "[availability][tablet_staticstic] time_interval: " << all_time / 1000
      << ", notready_time: " << total_time / 1000
      << ", total_time: " << (all_time * all_tablets) / 1000
      << ", ready_time_percent: " << RoundNumberToNDecimalPlaces(1.0 - (double)total_time / (all_time * all_tablets + 1), 6)
      << ", notready_tablets: " << nr_notready_tablets
      << ", total_tabltes: " << all_tablets
      << ", ready_tablets_percent: " << RoundNumberToNDecimalPlaces(1.0 - (double)nr_notready_tablets / (all_tablets + 1), 6)
      << ", notready_count: " << total_notready
      << ", reready_count: " << total_reready;

    int64_t cost = get_micros() - start;
    LOG(INFO) << "[availability] cost time:" << cost/1000 << " ms";
}

} // master
} // tera
