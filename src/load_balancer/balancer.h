// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_BALANCER_H_
#define TERA_LOAD_BALANCER_BALANCER_H_

#include <string>
#include <vector>

#include "load_balancer/lb_node.h"
#include "load_balancer/options.h"
#include "load_balancer/plan.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace load_balancer {

class Balancer {
public:
    virtual ~Balancer() {}

    // balance the whole cluster
    virtual bool BalanceCluster(
            const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
            std::vector<Plan>* plans) = 0;

    // balance for the specified table
    virtual bool BalanceCluster(
            const std::string& table_name,
            const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
            std::vector<Plan>* plans) = 0;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_BALANCER_H_
