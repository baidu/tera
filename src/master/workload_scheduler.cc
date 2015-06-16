// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/workload_scheduler.h"

#include "glog/logging.h"
#include "master/tablet_manager.h"

namespace tera {
namespace master {

WorkloadScheduler::WorkloadScheduler(WorkloadComparator* comparator)
    : m_comparator(comparator) {
    LOG(INFO) << "workload schduler [" << comparator->Name()
        << "] is activated";
}

WorkloadScheduler::~WorkloadScheduler() {}

bool WorkloadScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                     std::string* node_addr) {
    if (node_list.size() == 0) {
        return false;
    }

    TabletNodePtr null_ptr;
    TabletNodePtr best_one = node_list[0];

    for (size_t i = 1; i < node_list.size(); ++i) {
        TabletNodePtr cur_one = node_list[i];
//        VLOG(5) << "node: " << cur_one->m_addr << ", load: "
//            << cur_one->m_data_size;
        int r = m_comparator->Compare(best_one, cur_one);
        if (r > 0) {
            best_one = cur_one;
        } else if (r < 0) {
            // do nothing
        } else if (best_one->GetAddr() <= m_last_choose_node
            && cur_one->GetAddr() > m_last_choose_node) {
            // round-robin
            best_one = cur_one;
        }
    }
//    VLOG(5) << "choose node: " << best_one->m_addr << ", load: "
//        << best_one->m_data_size;
    m_last_choose_node = best_one->GetAddr();
    *node_addr = best_one->GetAddr();
    return true;
}

class WorkloadLess {
public:
    bool operator() (const TabletNodePtr& a, const TabletNodePtr& b) {
        return m_comparator->Compare(a, b) < 0;
    }
    WorkloadLess(WorkloadComparator* comparator) : m_comparator(comparator) {}
private:
    WorkloadComparator* m_comparator;
};

void WorkloadScheduler::AscendingSort(std::vector<TabletNodePtr>& node_list) {
    WorkloadLess less(m_comparator);
    std::sort(node_list.begin(), node_list.end(), less);
}

class WorkloadGreater {
public:
    bool operator() (const TabletNodePtr& a, const TabletNodePtr& b) {
        return m_comparator->Compare(a, b) > 0;
    }
    WorkloadGreater(WorkloadComparator* comparator) : m_comparator(comparator) {}
private:
    WorkloadComparator* m_comparator;
};

void WorkloadScheduler::DescendingSort(std::vector<TabletNodePtr>& node_list) {
    WorkloadGreater greater(m_comparator);
    std::sort(node_list.begin(), node_list.end(), greater);
}

} // namespace master
} // namespace tera
