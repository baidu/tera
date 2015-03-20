// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "master/workload_scheduler.h"

#include "glog/logging.h"
#include "master/tablet_manager.h"

namespace tera {
namespace master {

WorkloadScheduler::WorkloadScheduler() {
    LOG(INFO) << "workload schduling is activated";
}

WorkloadScheduler::~WorkloadScheduler() {}

bool WorkloadScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                     std::string* node_addr) {
    if (node_list.size() == 0) {
        return false;
    }

    TabletNodePtr null_ptr;
    TabletNodePtr best_one;

    for (size_t i = 0; i < node_list.size(); ++i) {
        TabletNodePtr cur_one = node_list[i];
//        VLOG(5) << "node: " << cur_one->m_addr << ", load: "
//            << cur_one->m_data_size;
        if (best_one == null_ptr) {
            best_one = cur_one;
        } else if (best_one->GetSize() > cur_one->GetSize()) {
            best_one = cur_one;
        } else if (best_one->GetSize() < cur_one->GetSize()) {
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

bool WorkLoadLess(TabletNodePtr i, TabletNodePtr j) {
    return (i->GetSize() < j->GetSize());
}

bool WorkLoadGreater(TabletNodePtr i, TabletNodePtr j) {
    return (i->GetSize() > j->GetSize());
}

void WorkloadScheduler::AscendingSort(std::vector<TabletNodePtr>& node_list) {
    std::sort(node_list.begin(), node_list.end(), WorkLoadLess);
}

void WorkloadScheduler::DescendingSort(std::vector<TabletNodePtr>& node_list) {
    std::sort(node_list.begin(), node_list.end(), WorkLoadGreater);
}

bool WorkloadScheduler::FindBestNode(const std::vector<TabletNodePtr>& node_list,
                                     const std::string& table_name,
                                     std::string* node_addr) {
    if (table_name.empty()) {
        return FindBestNode(node_list, node_addr);
    }

    if (node_list.size() == 0) {
        return false;
    }

    TabletNodePtr null_ptr;
    TabletNodePtr best_one;
    uint64_t best_node_table_size = 0;

    for (size_t i = 0; i < node_list.size(); ++i) {
        TabletNodePtr cur_one = node_list[i];
        uint64_t cur_node_table_size = cur_one->GetTableSize(table_name);

        if (best_one == null_ptr) {
            best_one = cur_one;
            best_node_table_size = cur_node_table_size;
        } else if (best_node_table_size > cur_node_table_size) {
            best_one = cur_one;
            best_node_table_size = cur_node_table_size;
        } else if (best_node_table_size < cur_node_table_size) {
            // do nothing
        } else if (best_one->GetSize() > cur_one->GetSize()) {
            best_one = cur_one;
            best_node_table_size = cur_node_table_size;
        } else if (best_one->GetSize() < cur_one->GetSize()) {
            // do nothing
        } else if (best_one->GetAddr() <= m_last_choose_node
            && cur_one->GetAddr() > m_last_choose_node) {
            // round-robin
            best_one = cur_one;
            best_node_table_size = cur_node_table_size;
        }
    }
//    VLOG(5) << "choose node: " << best_one->m_addr << ", load: "
//        << best_one->m_data_size;
    m_last_choose_node = best_one->GetAddr();
    *node_addr = best_one->GetAddr();
    return true;
}

struct TableSizeLess {
    bool operator() (TabletNodePtr i, TabletNodePtr j) {
        uint64_t i_table_size = i->GetTableSize(table_name);
        uint64_t j_table_size = j->GetTableSize(table_name);
        uint64_t i_size = i->GetSize();
        uint64_t j_size = j->GetSize();

        if (i_table_size < j_table_size) {
            return true;
        } else if (i_table_size > j_table_size) {
            return false;
        } else {
            return (i_size < j_size);
        }
    }

    TableSizeLess(const std::string& tn) : table_name(tn) {}
    std::string table_name;
};

struct TableSizeGreater {
    bool operator() (TabletNodePtr i, TabletNodePtr j) {
        uint64_t i_table_size = i->GetTableSize(table_name);
        uint64_t j_table_size = j->GetTableSize(table_name);
        uint64_t i_size = i->GetSize();
        uint64_t j_size = j->GetSize();

        if (i_table_size > j_table_size) {
            return true;
        } else if (i_table_size < j_table_size) {
            return false;
        } else {
            return (i_size > j_size);
        }
    }

    TableSizeGreater(const std::string& tn) : table_name(tn) {}
    std::string table_name;
};

void WorkloadScheduler::AscendingSort(const std::string& table_name,
                                      std::vector<TabletNodePtr>& node_list) {
    TableSizeLess less(table_name);
    std::sort(node_list.begin(), node_list.end(), less);
}

void WorkloadScheduler::DescendingSort(const std::string& table_name,
                                       std::vector<TabletNodePtr>& node_list) {
    TableSizeGreater greater(table_name);
    std::sort(node_list.begin(), node_list.end(), greater);
}

} // namespace master
} // namespace tera
