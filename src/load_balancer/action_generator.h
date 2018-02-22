// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_ACTION_GENERATOR_H_
#define TERA_LOAD_BALANCER_ACTION_GENERATOR_H_

#include <limits>
#include <memory>
#include <string>

#include "load_balancer/action.h"
#include "load_balancer/cluster.h"
#include "load_balancer/random.h"

namespace tera {
namespace load_balancer {

const uint32_t kInvalidNodeIndex = std::numeric_limits<uint32_t>::max();
const uint32_t kInvalidTabletIndex = std::numeric_limits<uint32_t>::max();

class ActionGenerator {
public:
    virtual ~ActionGenerator() {}

    virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) = 0;

    virtual std::string Name() = 0;

    virtual uint32_t PickRandomNode(const std::shared_ptr<Cluster>& cluster) {
        if (cluster->tablet_node_num_ > 0) {
            return Random::Rand(0, cluster->tablet_node_num_);
        } else {
            return kInvalidNodeIndex;
        }
    }

    // pick a different node with the picked_index
    virtual uint32_t PickOtherRandomNode(const std::shared_ptr<Cluster>& cluster,
                                         const uint32_t picked_index) {
        assert(cluster->tablet_node_num_ >= 2);

        while (true) {
            uint32_t node_index = PickRandomNode(cluster);
            if (node_index != picked_index) {
                return node_index;
            }
        }
    }

    virtual uint32_t PickRandomTabletOfNode(const std::shared_ptr<Cluster>& cluster,
                                            const uint32_t node_index) {
        uint32_t tablet_num = cluster->tablets_per_node_[node_index].size();

        if (tablet_num > 0) {
            uint32_t rand = Random::Rand(0, tablet_num);
            return cluster->tablets_per_node_[node_index][rand];
        } else {
            return kInvalidTabletIndex;
        }
    }
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_ACTION_GENERATOR_H_
