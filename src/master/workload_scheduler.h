// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_WORKLOAD_SCHEDULER_H_
#define TERA_MASTER_WORKLOAD_SCHEDULER_H_

#include "master/scheduler.h"

namespace tera {
namespace master {

class TabletManager;

class WorkloadGetter {
public:
    virtual int64_t operator() (const TabletNodePtr& t) = 0;
    virtual int64_t operator() (const TabletPtr& t) = 0;
    virtual const char* Name() = 0;
};

class WorkloadComparator {
public:
    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    virtual int Compare(const TabletNodePtr& a, const TabletNodePtr& b) {
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

    virtual const char* Name() {
        return m_load_getter->Name();
    }

    WorkloadComparator(WorkloadGetter* load_getter, WorkloadComparator* next_comparator = NULL)
        : m_load_getter(load_getter),
          m_next_comparator(next_comparator) {}

    virtual ~WorkloadComparator() {}

private:
    WorkloadGetter* m_load_getter;
    WorkloadComparator* m_next_comparator;
};

class WorkloadScheduler : public Scheduler {
public:
    WorkloadScheduler(WorkloadComparator* comparator);
    ~WorkloadScheduler();

    bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                      size_t* best_index);
    void AscendingSort(std::vector<TabletNodePtr>& node_list);
    void DescendingSort(std::vector<TabletNodePtr>& node_list);

    const char* Name() {
        return m_comparator->Name();
    }

private:
    WorkloadComparator* m_comparator;
    std::string m_last_choose_node;
};

class SizeGetter : public WorkloadGetter {
public:
    virtual int64_t operator() (const TabletNodePtr& ts) {
        return ts->GetSize(m_table_name);
    }
    virtual int64_t operator() (const TabletPtr& t) {
        return t->GetDataSize();
    }
    virtual const char* Name() {
        return "datasize";
    }
    SizeGetter() {}
    SizeGetter(const std::string& table_name) : m_table_name(table_name) {}
    virtual ~SizeGetter() {}

private:
    std::string m_table_name;
};

class QPSGetter : public WorkloadGetter {
public:
    virtual int64_t operator() (const TabletNodePtr& ts) {
        return ts->GetQps(m_table_name);
    }
    virtual int64_t operator() (const TabletPtr& t) {
        return t->GetAverageCounter().read_rows();
    }
    virtual const char* Name() {
        return "qps";
    }
    QPSGetter() {}
    QPSGetter(const std::string& table_name) : m_table_name(table_name) {}
    virtual ~QPSGetter() {}

private:
    std::string m_table_name;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_WORKLOAD_SCHEDULER_H_
