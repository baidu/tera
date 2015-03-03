// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_SCHEDULER_H
#define TERA_MASTER_SCHEDULER_H

#include <map>
#include <string>

#include "tera/master/tabletnode_manager.h"

namespace tera {
namespace master {

class Scheduler {
public:
    virtual ~Scheduler() {}

    // all-table schedule
    virtual bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                              std::string* node_addr) = 0;
    virtual void AscendingSort(std::vector<TabletNodePtr>& node_list) = 0;
    virtual void DescendingSort(std::vector<TabletNodePtr>& node_list) = 0;

    // per-table schedule
    virtual bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                              const std::string& table_name,
                              std::string* node_addr) = 0;
    virtual void AscendingSort(const std::string& table_name,
                               std::vector<TabletNodePtr>& node_list) = 0;
    virtual void DescendingSort(const std::string& table_name,
                                std::vector<TabletNodePtr>& node_list) = 0;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_SCHEDULER_H
