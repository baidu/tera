// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_CLUSTER_H_
#define TERA_LOAD_BALANCER_CLUSTER_H_

#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "load_balancer/action.h"
#include "load_balancer/lb_node.h"
#include "load_balancer/options.h"
#include "master/tablet_manager.h"
#include "master/tabletnode_manager.h"

namespace tera {
namespace load_balancer {

class Cluster {
public:
    Cluster(const std::vector<std::shared_ptr<LBTabletNode>>& tablet_nodes,
            const LBOptions& options);

    virtual ~Cluster();

    void DebugCluster();

    bool ValidAction(const std::shared_ptr<Action>& action);

    void DoAction(const std::shared_ptr<Action>& action);

    void SortNodesByTabletCount();

    void SortNodesBySize();

    void SortNodesByReadLoad();

    void SortNodesByWriteLoad();

    void SortNodesByScanLoad();

private:
    void RegisterTablet(const std::shared_ptr<LBTablet>& tablet, uint32_t tablet_index, uint32_t node_index);
    void AddTablet(uint32_t tablet_index, uint32_t to_node_index);
    void RemoveTablet(uint32_t tablet_index, uint32_t from_node_index);
    void MoveTablet(uint32_t tablet_index, uint32_t source_node_index, uint32_t dest_node_index);

// cluster info, use index to speed up the calculation
// make these info public also for speeding up
public:
    uint32_t table_num_;
    uint32_t tablet_node_num_;
    uint32_t tablet_num_;
    uint32_t tablet_moved_num_;

    // table_index -> table
    std::map<uint32_t, std::string> tables_;
    // node_index -> node
    std::map<uint32_t, std::shared_ptr<LBTabletNode>> nodes_;
    // tablet_index -> tablet
    std::map<uint32_t, std::shared_ptr<LBTablet>> tablets_;

    // table -> table_index
    std::map<std::string, uint32_t> tables_to_index_;
    // node -> node_index
    std::map<std::string, uint32_t> nodes_to_index_;
    // tablet -> tablet_index
    std::map<std::string, uint32_t> tablets_to_index_;

    // tablet_index -> node_index
    std::map<uint32_t, uint32_t> tablet_index_to_node_index_;
    // initial tablet_index -> node_index, it's the initial cluster state
    std::map<uint32_t, uint32_t> initial_tablet_index_to_node_index_;
    // tablet_index -> table_index
    std::map<uint32_t, uint32_t> tablet_index_to_table_index_;

    // node_index -> tablets index on the node
    std::map<uint32_t, std::vector<uint32_t>> tablets_per_node_;
    // node_index -> tablets index of not ready on the node
    std::map<uint32_t, std::vector<uint32_t>> initial_tablets_not_ready_per_node_;
    // abnormal nodes index
    std::unordered_set<uint32_t> abnormal_nodes_index_;
    // index of tablets moved to abnormal nodes
    std::unordered_set<uint32_t> tablets_moved_to_abnormal_nodes_;
    // read pending nodes index
    std::unordered_set<uint32_t> read_pending_nodes_index_;
    // index of tablets moved to read pending nodes
    std::unordered_set<uint32_t> tablets_moved_to_read_pending_nodes_;
    // write pending nodes index
    std::unordered_set<uint32_t> write_pending_nodes_index_;
    // index of tablets moved to write pending nodes
    std::unordered_set<uint32_t> tablets_moved_to_write_pending_nodes_;
    // scan pending nodes index
    std::unordered_set<uint32_t> scan_pending_nodes_index_;
    // index of tablets moved to scan pending nodes
    std::unordered_set<uint32_t> tablets_moved_to_scan_pending_nodes_;
    // node_index -> data size on the node
    std::map<uint32_t, uint64_t> size_per_node_;
    // node_index -> read load on the node
    std::map<uint32_t, uint64_t> read_load_per_node_;
    // node_index -> write load on the node
    std::map<uint32_t, uint64_t> write_load_per_node_;
    // node_index -> scan load on the node
    std::map<uint32_t, uint64_t> scan_load_per_node_;
    // tablets index of moved too frequently
    std::unordered_set<uint32_t> tablets_moved_too_frequently_;

    // meta table node index
    uint32_t meta_table_node_index_;

    // for ActionGenerator
    std::vector<uint32_t> node_index_sorted_by_tablet_count_;
    std::vector<uint32_t> node_index_sorted_by_size_;
    std::vector<uint32_t> node_index_sorted_by_read_load_;
    std::vector<uint32_t> node_index_sorted_by_write_load_;
    std::vector<uint32_t> node_index_sorted_by_scan_load_;

    LBOptions lb_options_;

private:
    std::vector<std::shared_ptr<LBTabletNode>> lb_nodes_;
};

} // namespace load_balancer
} // namespace tera

#endif // TERA_LOAD_BALANCER_CLUSTER_H_
