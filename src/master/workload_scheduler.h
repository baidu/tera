// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_WORKLOAD_SCHEDULER_H_
#define TERA_MASTER_WORKLOAD_SCHEDULER_H_

#include "master/scheduler.h"

namespace tera {
namespace master {

class TabletManager;

class WorkloadScheduler : public Scheduler {
public:
    WorkloadScheduler();
    ~WorkloadScheduler();

    bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                      std::string* node_addr);
    void AscendingSort(std::vector<TabletNodePtr>& node_list);
    void DescendingSort(std::vector<TabletNodePtr>& node_list);

    bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                      const std::string& table_name,
                      std::string* node_addr);
    void AscendingSort(const std::string& table_name,
                       std::vector<TabletNodePtr>& node_list);
    void DescendingSort(const std::string& table_name,
                        std::vector<TabletNodePtr>& node_list);

private:
    std::string m_last_choose_node;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_WORKLOAD_SCHEDULER_H_
