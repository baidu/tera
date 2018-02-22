// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_LB_NODE_H_
#define TERA_LOAD_BALANCER_LB_NODE_H_

#include <memory>
#include <string>
#include <vector>

#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace load_balancer {

struct LBTablet {
    tera::master::TabletPtr tablet_ptr;
};

struct LBTabletNode {
    tera::master::TabletNodePtr tablet_node_ptr;
    std::vector<std::shared_ptr<LBTablet>> tablets;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_LB_NODE_H_
