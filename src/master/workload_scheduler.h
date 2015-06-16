// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_WORKLOAD_SCHEDULER_H_
#define TERA_MASTER_WORKLOAD_SCHEDULER_H_

#include "master/scheduler.h"

namespace tera {
namespace master {

class TabletManager;

class WorkloadComparator {
public:
    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    virtual int Compare(const TabletNodePtr& a, const TabletNodePtr& b) = 0;
    virtual const char* Name() = 0;
};

class TotalSizeComparator : public WorkloadComparator {
public:
    virtual const char* Name() {
        return "total-size";
    }
    virtual int Compare(const TabletNodePtr& a, const TabletNodePtr& b) {
        uint64_t a_size = a->GetSize();
        uint64_t b_size = b->GetSize();
        if (a_size < b_size) {
            return -1;
        } else if (a_size > b_size) {
            return 1;
        } else {
            return 0;
        }
    }
};

class TableSizeComparator : public WorkloadComparator {
public:
    virtual const char* Name() {
        return m_name.c_str();
    }
    virtual int Compare(const TabletNodePtr& a, const TabletNodePtr& b) {
        uint64_t a_table_size = a->GetTableSize(m_table_name);
        uint64_t b_table_size = b->GetTableSize(m_table_name);
        uint64_t a_size = a->GetSize();
        uint64_t b_size = b->GetSize();

        if (a_table_size < b_table_size) {
            return -1;
        } else if (a_table_size > b_table_size) {
            return 1;
        } else if (a_size < b_size) {
            return -1;
        } else if (a_size > b_size) {
            return 1;
        } else {
            return 0;
        }
    }
    TableSizeComparator(const std::string& table_name)
        : m_table_name(table_name) {
        m_name += "table (" + m_table_name + ") size";
    }
private:
    std::string m_table_name;
    std::string m_name;
};

class WorkloadScheduler : public Scheduler {
public:
    WorkloadScheduler(WorkloadComparator* comparator);
    ~WorkloadScheduler();

    bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                      std::string* node_addr);
    void AscendingSort(std::vector<TabletNodePtr>& node_list);
    void DescendingSort(std::vector<TabletNodePtr>& node_list);

private:
    WorkloadComparator* m_comparator;
    std::string m_last_choose_node;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_WORKLOAD_SCHEDULER_H_
