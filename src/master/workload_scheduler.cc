// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/workload_scheduler.h"

#include "glog/logging.h"
#include "master/tablet_manager.h"

DECLARE_double(tera_master_load_balance_size_ratio_trigger);
DECLARE_int32(tera_master_load_balance_ts_load_threshold);
DECLARE_int32(tera_master_load_balance_scan_weight);

namespace tera {
namespace master {

class Comparator {
public:
    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    virtual int Compare(const TabletNodePtr& a, const TabletNodePtr& b,
                        const std::string& table_name) = 0;

};

class WorkloadLess {
public:
    bool operator() (const TabletNodePtr& a, const TabletNodePtr& b) {
        return comparator_->Compare(a, b, table_name_) < 0;
    }
    WorkloadLess(Comparator* comparator, const std::string& table_name = "")
        : comparator_(comparator), table_name_(table_name) {}
private:
    Comparator* comparator_;
    std::string table_name_;
};

class WorkloadGreater {
public:
    bool operator() (const TabletNodePtr& a, const TabletNodePtr& b) {
        return comparator_->Compare(a, b, table_name_) > 0;
    }
    WorkloadGreater(Comparator* comparator, const std::string& table_name = "")
        : comparator_(comparator), table_name_(table_name) {}

private:
    Comparator* comparator_;
    std::string table_name_;
};

/////////////////////////////////////////////////
//                SizeScheduler
/////////////////////////////////////////////////

class SizeComparator : public Comparator {
public:
    int Compare(const TabletNodePtr& a, const TabletNodePtr& b,
                const std::string& table_name) {
        uint64_t a_size = a->GetSize(table_name);
        uint64_t b_size = b->GetSize(table_name);
        if (a_size < b_size) {
            return -1;
        } else if (a_size > b_size) {
            return 1;
        } else {
            return 0;
        }
    }
};

bool SizeScheduler::MayMoveOut(const TabletNodePtr& node,
                               const std::string& table_name) {
    VLOG(16) << "[size-sched] MayMoveOut()";
    int64_t node_size = node->GetSize(table_name);
    if (node_size <= 0) {
        VLOG(16) << "[size-sched] node has no data";
        return false;
    }
    return true;
}

bool SizeScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                 const std::string& table_name,
                                 size_t* best_index) {
    VLOG(16) << "[size-sched] FindBestNode()";
    if (node_list.size() == 0) {
        return false;
    }

    SizeComparator comparator;
    *best_index = 0;
    for (size_t i = 1; i < node_list.size(); ++i) {
        int r = comparator.Compare(node_list[*best_index], node_list[i], table_name);
        if (r > 0) {
            *best_index = i;
        } else if (r < 0) {
            // do nothing
        } else if (node_list[*best_index]->GetAddr() <= last_choose_node_
            && node_list[i]->GetAddr() > last_choose_node_) {
            // round-robin
            *best_index = i;
        }
    }
    last_choose_node_ = node_list[*best_index]->GetAddr();
    VLOG(16) << "[size-sched] best node = " << last_choose_node_;
    return true;
}

bool SizeScheduler::FindBestTablet(const TabletNodePtr& src_node,
                                   const TabletNodePtr& dst_node,
                                   const std::vector<TabletPtr>& tablet_list,
                                   const std::string& table_name,
                                   size_t* best_index) {
    VLOG(16) << "[size-sched] FindBestTablet() " << src_node->GetAddr()
            << " -> " << dst_node->GetAddr();

    int64_t src_node_size = src_node->GetSize(table_name);
    int64_t dst_node_size = dst_node->GetSize(table_name);

    const double& size_ratio = FLAGS_tera_master_load_balance_size_ratio_trigger;
    if ((double)src_node_size < (double)dst_node_size * size_ratio) {
        VLOG(16) << "[size-sched] size ratio not reach threshold: " << src_node_size
                << " : " << dst_node_size;
        return false;
    }

    int64_t ideal_move_size = (src_node_size - dst_node_size) / 2;
    VLOG(16) << "[size-sched] size = " << src_node_size << " : " << dst_node_size
            << " ideal_move_size = " << ideal_move_size;

    int64_t best_tablet_index = -1;
    int64_t best_tablet_size = 0;
    int64_t best_tablet_qps = 0;
    for (size_t i = 0; i < tablet_list.size(); ++i) {
        TabletPtr tablet = tablet_list[i];
        int64_t size = tablet->GetDataSize();
        int64_t qps = tablet->GetQps();
        if (size <= ideal_move_size
                && (best_tablet_index == -1 || size > best_tablet_size)) {
            best_tablet_index = i;
            best_tablet_size = size;
            best_tablet_qps = qps;
        }
    }
    if (best_tablet_index == -1) {
        return false;
    }
    *best_index = best_tablet_index;
    TabletPtr best_tablet = tablet_list[best_tablet_index];
    VLOG(16) << "[size-sched] best tablet = " << best_tablet->GetPath()
            << " size = " << best_tablet_size
            << " qps = " << best_tablet_qps;
    return true;
}

bool SizeScheduler::NeedSchedule(std::vector<TabletNodePtr>& node_list,
                                 const std::string& table_name) {
    return true;
}

void SizeScheduler::AscendingSort(std::vector<TabletNodePtr>& node_list,
                                  const std::string& table_name) {
    SizeComparator comparator;
    WorkloadLess less(&comparator, table_name);
    std::sort(node_list.begin(), node_list.end(), less);
}

void SizeScheduler::DescendingSort(std::vector<TabletNodePtr>& node_list,
                                   const std::string& table_name) {
    SizeComparator comparator;
    WorkloadGreater greater(&comparator, table_name);
    std::sort(node_list.begin(), node_list.end(), greater);
}

/////////////////////////////////////////////////
//                LoadScheduler
/////////////////////////////////////////////////

static uint64_t GetPending(const TabletNodePtr& ts) {
    return ts->GetReadPending() + ts->GetWritePending()
        + ts->GetScanPending() * FLAGS_tera_master_load_balance_scan_weight;
}

class LoadComparator : public Comparator {
public:
    int Compare(const TabletNodePtr& a, const TabletNodePtr& b,
                const std::string& table_name) {
        uint64_t a_read_pending = GetPending(a);
        uint64_t b_read_pending = GetPending(b);
        if (a_read_pending < b_read_pending) {
            return -1;
        } else if (a_read_pending > b_read_pending) {
            return 1;
        }

        uint64_t a_row_read_delay = a->GetRowReadDelay();
        uint64_t b_row_read_delay = b->GetRowReadDelay();
        if (a_row_read_delay < b_row_read_delay) {
            return -1;
        } else if (a_row_read_delay > b_row_read_delay) {
            return 1;
        }

        uint64_t a_qps = a->GetQps(table_name);
        uint64_t b_qps = b->GetQps(table_name);
        if (a_qps < b_qps) {
            return -1;
        } else if (a_qps > b_qps) {
            return 1;
        } else {
            return 0;
        }
    }

    virtual ~LoadComparator() {}
};

bool LoadScheduler::MayMoveOut(const TabletNodePtr& node, const std::string& table_name) {
    VLOG(16) << "[load-sched] MayMoveOut()";
    int64_t node_read_pending = GetPending(node);
    if (node_read_pending <= FLAGS_tera_master_load_balance_ts_load_threshold) {
        VLOG(16) << "[load-sched] node do not need loadbalance: " << node_read_pending;
        return false;
    }
    int64_t node_qps = node->GetQps(table_name);
    if (node_qps <= 0) {
        VLOG(16) << "[load-sched] node has 0 qps.";
        return false;
    }
    return true;
}

bool LoadScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                 const std::string& table_name,
                                 size_t* best_index) {
    VLOG(16) << "[load-sched] FindBestNode()";
    if (node_list.size() == 0) {
        return false;
    }

    LoadComparator comparator;
    *best_index = 0;
    for (size_t i = 1; i < node_list.size(); ++i) {
        int r = comparator.Compare(node_list[*best_index], node_list[i], table_name);
        if (r > 0) {
            *best_index = i;
        } else if (r < 0) {
            // do nothing
        } else if (node_list[*best_index]->GetAddr() <= last_choose_node_
            && node_list[i]->GetAddr() > last_choose_node_) {
            // round-robin
            *best_index = i;
        }
    }
    last_choose_node_ = node_list[*best_index]->GetAddr();
    VLOG(16) << "[load-sched] best node : " << last_choose_node_;
    return true;
}

bool LoadScheduler::FindBestTablet(const TabletNodePtr& src_node,
                                   const TabletNodePtr& dst_node,
                                   const std::vector<TabletPtr>& tablet_list,
                                   const std::string& table_name,
                                   size_t* best_index) {
    VLOG(16) << "[load-sched] FindBestTablet() " << src_node->GetAddr()
            << " -> " << dst_node->GetAddr();

    int64_t src_node_read_pending = GetPending(src_node);
    int64_t dst_node_read_pending = GetPending(dst_node);
    if (src_node_read_pending <= 0 || dst_node_read_pending > 0) {
        VLOG(16) << "[load-sched] read pending not reach threshold: " << src_node_read_pending
                << " : " << dst_node_read_pending;
        return false;
    }

    VLOG(16) << "[load-sched]"
            << " rpending = " <<  src_node_read_pending << " : " << dst_node_read_pending
            << " delay = " << src_node->GetRowReadDelay() << " : " << dst_node->GetRowReadDelay()
            << " qps = " << src_node->GetQps(table_name) << " : " << dst_node->GetQps(table_name);

    // Donot move out the most busy tablet, move the second one
    std::map<int64_t, int64_t> tablet_sort;
    for (size_t i = 0; i < tablet_list.size(); ++i) {
        TabletPtr tablet = tablet_list[i];
        int64_t qps = tablet->GetQps();
        tablet_sort[qps] = i;
    }
    std::map<int64_t, int64_t>::reverse_iterator it = tablet_sort.rbegin();
    it++;
    int64_t best_tablet_qps = it->first;
    int64_t best_tablet_index = it->second;
    if (best_tablet_qps == 0) {
        VLOG(16) << "[load-sched] no need to move 0 QPS tablet";
        return false;
    }
    *best_index = best_tablet_index;
    TabletPtr best_tablet = tablet_list[best_tablet_index];
    VLOG(16) << "[load-sched] best tablet = " << best_tablet->GetPath()
            << " size = " << best_tablet->GetDataSize()
            << " qps = " << best_tablet_qps;
    return true;
}

bool LoadScheduler::NeedSchedule(std::vector<TabletNodePtr>& node_list,
                                 const std::string& table_name) {
    size_t pending_node_num = 0;
    for (size_t i = 0; i < node_list.size(); ++i) {
        int64_t node_read_pending = GetPending(node_list[i]);
        if (node_read_pending > FLAGS_tera_master_load_balance_ts_load_threshold) {
            pending_node_num++;
        }
    }

    // If pending_node_num large than 10%, we think read bottleneck is dfs io,
    // do not need load balance by read.
    if (pending_node_num * 10 > node_list.size()) {
        return false;
    }
    return true;
}

void LoadScheduler::AscendingSort(std::vector<TabletNodePtr>& node_list,
                                  const std::string& table_name) {
    LoadComparator comparator;
    WorkloadLess less(&comparator, table_name);
    std::sort(node_list.begin(), node_list.end(), less);
}

void LoadScheduler::DescendingSort(std::vector<TabletNodePtr>& node_list,
                                   const std::string& table_name) {
    LoadComparator comparator;
    WorkloadGreater greater(&comparator, table_name);
    std::sort(node_list.begin(), node_list.end(), greater);
}

} // namespace master
} // namespace tera
