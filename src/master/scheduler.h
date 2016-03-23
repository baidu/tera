// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_MASTER_SCHEDULER_H_
#define TERA_MASTER_SCHEDULER_H_

#include <map>
#include <string>

#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace master {

class Scheduler {
public:
    virtual ~Scheduler() {}

    virtual bool MayMoveOut(TabletNodePtr node, const std::string& table_name) = 0;
    virtual bool FindBestNode(const std::vector<TabletNodePtr>& node_list,
                              const std::string& table_name,
                              size_t* best_index) = 0;
    virtual bool FindBestTablet(TabletNodePtr src_node, TabletNodePtr dst_node,
                                const std::vector<TabletPtr>& table_list,
                                const std::string& table_name,
                                size_t* best_index) = 0;

    virtual bool NeedSchedule(std::vector<TabletNodePtr>& node_list,
                              const std::string& table_name) = 0;

    virtual void AscendingSort(std::vector<TabletNodePtr>& node_list,
                               const std::string& table_name) = 0;
    virtual void DescendingSort(std::vector<TabletNodePtr>& node_list,
                                const std::string& table_name) = 0;

    virtual const char* Name() = 0;
};

} // namespace master
} // namespace tera

#endif // TERA_MASTER_SCHEDULER_H_
