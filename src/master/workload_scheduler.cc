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
                                     size_t* best_index) {
    if (node_list.size() == 0) {
        return false;
    }

    *best_index = 0;
    for (size_t i = 1; i < node_list.size(); ++i) {
        int r = m_comparator->Compare(node_list[*best_index], node_list[i]);
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
