// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/workload_scheduler.h"

#include "glog/logging.h"
#include "master/tablet_manager.h"

DECLARE_double(tera_master_load_balance_size_ratio_trigger);
DECLARE_double(tera_master_load_balance_qps_ratio_trigger);
DECLARE_int32(tera_master_load_balance_qps_min_limit);

namespace tera {
namespace master {

class WorkloadGetter {
public:
    virtual int64_t operator() (const TabletNodePtr& tablet_node,
                                const std::string& table_name = "") const = 0;
    virtual int64_t operator() (const TabletPtr& tablet) const = 0;
    virtual ~WorkloadGetter() {}
};

class WorkloadComparator {
public:
    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    int Compare(const TabletNodePtr& a, const TabletNodePtr& b,
                const std::string& table_name) {
        if (!table_name.empty()) {
            uint64_t a_load = (*m_load_getter)(a, table_name);
            uint64_t b_load = (*m_load_getter)(b, table_name);
            if (a_load < b_load) {
                return -1;
            } else if (a_load > b_load) {
                return 1;
            }
        }

        uint64_t a_load = (*m_load_getter)(a);
        uint64_t b_load = (*m_load_getter)(b);
        if (a_load < b_load) {
            return -1;
        } else if (a_load > b_load) {
            return 1;
        } else if (m_next_comparator != NULL) {
            return m_next_comparator->Compare(a, b, table_name);
        } else {
            return 0;
        }
    }

    int Compare(const TabletPtr& a, const TabletPtr& b) {
        uint64_t a_load = (*m_load_getter)(a);
        uint64_t b_load = (*m_load_getter)(b);
        if (a_load < b_load) {
            return -1;
        } else if (a_load > b_load) {
            return 1;
        } else if (m_next_comparator != NULL) {
            return m_next_comparator->Compare(a, b);
        } else {
            return 0;
        }
    }

    WorkloadComparator(WorkloadGetter* load_getter,
                       bool owns_load_getter = false,
                       WorkloadComparator* next_comparator = NULL,
                       bool owns_next_comparator = false)
        : m_load_getter(load_getter),
          m_owns_load_getter(owns_load_getter),
          m_next_comparator(next_comparator),
          m_owns_next_comparator(owns_next_comparator) {
    }

    virtual ~WorkloadComparator() {
        if (m_owns_next_comparator) {
            delete m_next_comparator;
        }
        if (m_owns_load_getter) {
            delete m_load_getter;
        }
    }

private:
    WorkloadGetter* m_load_getter;
    bool m_owns_load_getter;
    WorkloadComparator* m_next_comparator;
    bool m_owns_next_comparator;
};

class WorkloadLess {
public:
    bool operator() (const TabletNodePtr& a, const TabletNodePtr& b) {
        return m_comparator->Compare(a, b, m_table_name) < 0;
    }
    bool operator() (const TabletPtr& a, const TabletPtr& b) {
        return m_comparator->Compare(a, b) < 0;
    }
    WorkloadLess(WorkloadComparator* comparator, const std::string& table_name = "")
        : m_comparator(comparator), m_table_name(table_name) {}
private:
    WorkloadComparator* m_comparator;
    std::string m_table_name;
};

class WorkloadGreater {
public:
    bool operator() (const TabletNodePtr& a, const TabletNodePtr& b) {
        return m_comparator->Compare(a, b, m_table_name) > 0;
    }
    bool operator() (const TabletPtr& a, const TabletPtr& b) {
        return m_comparator->Compare(a, b) > 0;
    }
    WorkloadGreater(WorkloadComparator* comparator, const std::string& table_name = "")
        : m_comparator(comparator), m_table_name(table_name) {}

private:
    WorkloadComparator* m_comparator;
    std::string m_table_name;
};

class SizeGetter : public WorkloadGetter {
public:
    virtual int64_t operator() (const TabletNodePtr& tabletnode,
                                const std::string& table_name = "") const {
        return tabletnode->GetSize(table_name);
    }
    virtual int64_t operator() (const TabletPtr& tablet) const {
        return tablet->GetDataSize();
    }
    virtual ~SizeGetter() {}
};

class SizeComparator : public WorkloadComparator {
public:
    SizeComparator() : WorkloadComparator(&m_size_getter) {}
private:
    SizeGetter m_size_getter;
};

class QPSGetter : public WorkloadGetter {
public:
    virtual int64_t operator() (const TabletNodePtr& tabletnode,
                                const std::string& table_name = "") const {
        return tabletnode->GetQps(table_name);
    }
    virtual int64_t operator() (const TabletPtr& tablet) const {
        return tablet->GetAverageCounter().read_rows();
    }
    virtual ~QPSGetter() {}
};

class QPSComparator : public WorkloadComparator {
public:
    QPSComparator() : WorkloadComparator(&m_qps_getter) {}
private:
    QPSGetter m_qps_getter;
};

/////////////////////////////////////////////////
//                SizeScheduler
/////////////////////////////////////////////////

bool SizeScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                 const std::string& table_name,
                                 size_t* best_index) {
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
        } else if (node_list[*best_index]->GetAddr() <= m_last_choose_node
            && node_list[i]->GetAddr() > m_last_choose_node) {
            // round-robin
            *best_index = i;
        }
    }
    m_last_choose_node = node_list[*best_index]->GetAddr();
    VLOG(7) << "[size-sched] best node = " << m_last_choose_node;
    return true;
}

bool SizeScheduler::FindBestTablet(TabletNodePtr src_node, TabletNodePtr dst_node,
                                   const std::vector<TabletPtr>& tablet_list,
                                   const std::string& table_name,
                                   size_t* best_index) {
    VLOG(7) << "[size-sched] FindBestTablet() " << src_node->GetAddr()
            << " -> " << dst_node->GetAddr();

    SizeGetter size_getter;
    int64_t src_node_size = size_getter(src_node, table_name);
    int64_t dst_node_size = size_getter(dst_node, table_name);

    const double& size_ratio = FLAGS_tera_master_load_balance_size_ratio_trigger;
    if ((double)src_node_size < (double)dst_node_size * size_ratio) {
        VLOG(7) << "[size-sched] size ratio not reach threshold: " << src_node_size
                << " : " << dst_node_size;
        return false;
    }

    QPSGetter qps_getter;
    int64_t src_node_qps = qps_getter(src_node, table_name);
    int64_t dst_node_qps = qps_getter(dst_node, table_name);

    const double& qps_ratio = FLAGS_tera_master_load_balance_qps_ratio_trigger;
    if (dst_node_qps >= FLAGS_tera_master_load_balance_qps_min_limit
            && (double)src_node_qps * qps_ratio <= (double)dst_node_qps) {
        VLOG(7) << "[size-sched] revert qps ratio reach threshold: " << src_node_qps
                << " : " << dst_node_qps;
        return false;
    }

    int64_t ideal_move_size = (src_node_size - dst_node_size) / 2;
    VLOG(7) << "[size-sched] size = " << src_node_size << " : " << dst_node_size
            << " qps = " << src_node_qps << " : " << dst_node_qps
            << " ideal_move_size = " << ideal_move_size;

    int64_t best_tablet_index = -1;
    int64_t best_tablet_size = 0;
    int64_t best_tablet_qps = 0;
    for (size_t i = 0; i < tablet_list.size(); ++i) {
        TabletPtr tablet = tablet_list[i];
        int64_t size = size_getter(tablet);
        int64_t qps = qps_getter(tablet);
        if (size <= ideal_move_size
                && (dst_node_qps + qps < FLAGS_tera_master_load_balance_qps_min_limit
                    || (src_node_qps - qps) * qps_ratio > (dst_node_qps + qps))
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
    VLOG(7) << "[size-sched] best tablet = " << best_tablet->GetPath()
            << " size = " << best_tablet_size
            << " qps = " << best_tablet_qps;
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
//                QPSScheduler
/////////////////////////////////////////////////

bool QPSScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                const std::string& table_name,
                                size_t* best_index) {
    if (node_list.size() == 0) {
        return false;
    }

    QPSComparator comparator;
    *best_index = 0;
    for (size_t i = 1; i < node_list.size(); ++i) {
        int r = comparator.Compare(node_list[*best_index], node_list[i], table_name);
        if (r > 0) {
            *best_index = i;
        } else if (r < 0) {
            // do nothing
        } else if (node_list[*best_index]->GetAddr() <= m_last_choose_node
            && node_list[i]->GetAddr() > m_last_choose_node) {
            // round-robin
            *best_index = i;
        }
    }
    m_last_choose_node = node_list[*best_index]->GetAddr();
    VLOG(7) << "[QPS-sched] best node : " << m_last_choose_node;
    return true;
}

bool QPSScheduler::FindBestTablet(TabletNodePtr src_node, TabletNodePtr dst_node,
                                  const std::vector<TabletPtr>& tablet_list,
                                  const std::string& table_name,
                                  size_t* best_index) {
    VLOG(7) << "[QPS-sched] FindBestTablet() " << src_node->GetAddr()
            << " -> " << dst_node->GetAddr();

    SizeGetter size_getter;
    QPSGetter qps_getter;
    int64_t src_node_qps = qps_getter(src_node, table_name);
    if (src_node_qps < FLAGS_tera_master_load_balance_qps_min_limit) {
        VLOG(7) << "qps not reach min limit: " << src_node_qps;
        return false;
    }

    int64_t dst_node_qps = qps_getter(dst_node, table_name);
    const double& qps_ratio = FLAGS_tera_master_load_balance_qps_ratio_trigger;
    if ((double)src_node_qps < (double)dst_node_qps * qps_ratio) {
        VLOG(7) << "qps ratio not reach threshold: " << src_node_qps
                << " : " << dst_node_qps;
        return false;
    }

    int64_t ideal_move_qps = (src_node_qps - dst_node_qps) / 2;
    VLOG(7) << "[QPS-sched] qps = " << src_node_qps << " : " << dst_node_qps
            << " ideal_move_qps = " << ideal_move_qps;

    int64_t best_tablet_index = -1;
    int64_t best_tablet_size = 0;
    int64_t best_tablet_qps = 0;
    for (size_t i = 0; i < tablet_list.size(); ++i) {
        TabletPtr tablet = tablet_list[i];
        int64_t size = size_getter(tablet);
        int64_t qps = qps_getter(tablet);
        if (qps <= ideal_move_qps
                && (best_tablet_index == -1 || qps > best_tablet_qps)) {
            best_tablet_index = i;
            best_tablet_size = size;
            best_tablet_qps = qps;
        }
    }
    if (best_tablet_index == -1) {
        return false;
    }
    if (best_tablet_qps == 0) {
        VLOG(7) << "[QPS-sched] no need to move 0 QPS tablet";
        return false;
    }
    *best_index = best_tablet_index;
    TabletPtr best_tablet = tablet_list[best_tablet_index];
    VLOG(7) << "[QPS-sched] best tablet = " << best_tablet->GetPath()
            << " size = " << best_tablet_size
            << " qps = " << best_tablet_qps;
    return true;
}

void QPSScheduler::AscendingSort(std::vector<TabletNodePtr>& node_list,
                                 const std::string& table_name) {
    QPSComparator comparator;
    WorkloadLess less(&comparator, table_name);
    std::sort(node_list.begin(), node_list.end(), less);
}

void QPSScheduler::DescendingSort(std::vector<TabletNodePtr>& node_list,
                                  const std::string& table_name) {
    QPSComparator comparator;
    WorkloadGreater greater(&comparator, table_name);
    std::sort(node_list.begin(), node_list.end(), greater);
}

} // namespace master
} // namespace tera
