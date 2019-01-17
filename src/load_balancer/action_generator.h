// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_ACTION_GENERATOR_H_
#define TERA_LOAD_BALANCER_ACTION_GENERATOR_H_

#include <functional>
#include <limits>
#include <memory>
#include <string>

#include "load_balancer/action.h"
#include "load_balancer/cluster.h"
#include "load_balancer/random.h"

namespace tera {
namespace load_balancer {

class ActionGenerator {
 public:
  virtual ~ActionGenerator() {}

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) = 0;

  virtual std::string Name() = 0;

 public:
  static uint32_t PickRandomNode(const std::shared_ptr<Cluster>& cluster) {
    if (cluster->tablet_node_num_ > 0) {
      return Random::Rand(0, cluster->tablet_node_num_);
    } else {
      return kInvalidNodeIndex;
    }
  }

  static uint32_t PickRandomTabletFromSourceNode(const std::shared_ptr<Cluster>& cluster,
                                                 uint32_t node_index,
                                                 const std::function<bool(uint32_t)>& is_proper) {
    uint32_t tablet_num = cluster->tablets_per_node_[node_index].size();
    if (tablet_num < 1) {
      return kInvalidTabletIndex;
    }

    uint32_t tablet_index = kInvalidTabletIndex;
    uint32_t cnt = 0;
    while (true) {
      ++cnt;
      if (cnt > 5 * tablet_num) {
        tablet_index = kInvalidTabletIndex;
        break;
      }

      uint32_t rand = Random::Rand(0, tablet_num);
      tablet_index = cluster->tablets_per_node_[node_index][rand];
      if (is_proper(tablet_index)) {
        break;
      }
    }

    return tablet_index;
  }

  // pick a different node with the picked_node_index
  static uint32_t PickRandomDestNode(
      const std::shared_ptr<Cluster>& cluster, uint32_t picked_node_index,
      uint32_t chosen_tablet_index,
      const std::function<bool(uint32_t, uint32_t)>& is_proper_location) {
    if (cluster->tablet_node_num_ < 2) {
      return kInvalidNodeIndex;
    }

    uint32_t node_index = kInvalidNodeIndex;
    uint32_t cnt = 0;
    while (true) {
      ++cnt;
      if (cnt > 5 * cluster->tablet_node_num_) {
        node_index = kInvalidNodeIndex;
        break;
      }

      node_index = PickRandomNode(cluster);
      if (node_index == picked_node_index) {
        continue;
      }
      if (is_proper_location(chosen_tablet_index, node_index)) {
        break;
      }
    }

    return node_index;
  }

  static uint32_t PickLightestNode(
      const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index,
      uint32_t chosen_tablet_index,
      const std::function<bool(uint32_t, uint32_t)>& is_proper_location) {
    uint32_t node_num = sorted_node_index.size();
    if (node_num < 1) {
      return kInvalidNodeIndex;
    }

    uint32_t i = 0;
    while (!is_proper_location(chosen_tablet_index, sorted_node_index[i])) {
      ++i;
      if (i == node_num) {
        return kInvalidNodeIndex;
      }
    }

    return sorted_node_index[i];
  }

  static uint32_t PickHeaviestNode(const std::shared_ptr<Cluster>& cluster,
                                   const std::vector<uint32_t>& sorted_node_index) {
    uint32_t node_num = sorted_node_index.size();
    if (node_num < 1) {
      return kInvalidNodeIndex;
    }

    return sorted_node_index[node_num - 1];
  }

  static uint32_t PickHeaviestNode(const std::shared_ptr<Cluster>& cluster,
                                   const std::vector<uint32_t>& sorted_node_index,
                                   const std::function<bool(uint32_t)>& is_proper) {
    uint32_t node_num = sorted_node_index.size();
    if (node_num < 1) {
      return kInvalidNodeIndex;
    }

    uint32_t i = node_num - 1;
    while (!is_proper(sorted_node_index[i])) {
      if (i == 0) {
        return kInvalidNodeIndex;
      }
      --i;
    }

    return sorted_node_index[i];
  }

  static uint32_t PickHeaviestTabletFromSourceNode(const std::shared_ptr<Cluster>& cluster,
                                                   const std::vector<uint32_t>& sorted_tablet_index,
                                                   const std::function<bool(uint32_t)>& is_proper) {
    uint32_t tablet_num = sorted_tablet_index.size();
    if (tablet_num < 1) {
      return kInvalidTabletIndex;
    }

    uint32_t i = tablet_num - 1;
    while (!is_proper(sorted_tablet_index[i])) {
      if (i == 0) {
        return kInvalidTabletIndex;
      }
      --i;
    }

    return sorted_tablet_index[i];
  }
};

}  // namespace load_balancer
}  // namespace tera

#endif  // TERA_LOAD_BALANCER_ACTION_GENERATOR_H_
