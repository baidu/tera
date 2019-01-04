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

const uint32_t kInvalidNodeIndex = std::numeric_limits<uint32_t>::max();
const uint32_t kInvalidTabletIndex = std::numeric_limits<uint32_t>::max();

class Cluster {
 public:
  Cluster(const std::vector<std::shared_ptr<LBTabletNode>>& tablet_nodes, const LBOptions& options,
          bool skip_meta_node);

  virtual ~Cluster();

  void DebugCluster();

  bool ValidAction(const std::shared_ptr<Action>& action);
  void DoAction(const std::shared_ptr<Action>& action);

  void SortNodesByTabletCount(std::vector<uint32_t>* sorted_node_index);
  void SortNodesBySize(std::vector<uint32_t>* sorted_node_index);
  void SortNodesByFlashSizePercent(std::vector<uint32_t>* sorted_node_index);
  void SortNodesByReadLoad(std::vector<uint32_t>* sorted_node_index);
  void SortNodesByWriteLoad(std::vector<uint32_t>* sorted_node_index);
  void SortNodesByScanLoad(std::vector<uint32_t>* sorted_node_index);
  void SortNodesByLRead(std::vector<uint32_t>* sorted_node_index);
  void SortNodesByComplexLoad(std::vector<uint32_t>* sorted_node_index);

  void SortTabletsOfNodeByReadLoad(uint32_t node_index, std::vector<uint32_t>* sorted_tablet_index);
  void SortTabletsOfNodeByWriteLoad(uint32_t node_index,
                                    std::vector<uint32_t>* sorted_tablet_index);
  void SortTabletsOfNodeByScanLoad(uint32_t node_index, std::vector<uint32_t>* sorted_tablet_index);
  void SortTabletsOfNodeByLRead(uint32_t node_index, std::vector<uint32_t>* sorted_tablet_index);

  // return true if meta table is on this node, otherwise false
  bool IsMetaNode(uint32_t node_index);

  bool IsReadyNode(uint32_t node_index);

  bool IsReadPendingNode(uint32_t node_index);
  bool IsWritePendingNode(uint32_t node_index);
  bool IsScanPendingNode(uint32_t node_index);
  bool IsPendingNode(uint32_t node_index);
  bool IsHeavyReadPendingNode(uint32_t node_index);
  bool IsHeavyWritePendingNode(uint32_t node_index);
  bool IsHeavyScanPendingNode(uint32_t node_index);
  bool IsHeavyPendingNode(uint32_t node_index);
  uint32_t HeavyPendingNodeNum();
  bool IsHeavyLReadNode(uint32_t node_index);

  bool IsAbnormalNode(uint32_t node_index);

  /*
   * check whether a tablet has flash lg located on a node who does not has ssd
   */
  bool IsFlashSizeEnough(uint32_t tablet_index, uint32_t node_index);

  /**
   * check whether it is proper to locate the tablet on the node
   */
  bool IsProperLocation(uint32_t tablet_index, uint32_t node_index);

  bool IsMetaTablet(uint32_t tablet_index);
  bool IsReadyTablet(uint32_t tablet_index);
  bool IsTabletMoveTooFrequent(uint32_t tablet_index);

  bool IsProperTargetTablet(uint32_t tablet_index);

 private:
  void RegisterTablet(const std::shared_ptr<LBTablet>& tablet, uint32_t tablet_index,
                      uint32_t node_index);
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
  // node_index -> data size on the node
  std::map<uint32_t, uint64_t> size_per_node_;
  // node_index -> flash data size on the node
  std::map<uint32_t, uint64_t> flash_size_per_node_;
  // node_index -> read load on the node
  std::map<uint32_t, uint64_t> read_load_per_node_;
  // node_index -> write load on the node
  std::map<uint32_t, uint64_t> write_load_per_node_;
  // node_index -> scan load on the node
  std::map<uint32_t, uint64_t> scan_load_per_node_;
  // node_index -> lread on the node
  std::map<uint32_t, uint64_t> lread_per_node_;
  // node_index -> read pending on the node
  std::map<uint32_t, uint64_t> read_pending_per_node_;
  // node_index -> write pending on the node
  std::map<uint32_t, uint64_t> write_pending_per_node_;
  // node_index -> scan pending on the node
  std::map<uint32_t, uint64_t> scan_pending_per_node_;

  // meta table node index
  uint32_t meta_table_node_index_;

  LBOptions lb_options_;

 private:
  std::vector<std::shared_ptr<LBTabletNode>> lb_nodes_;
};

}  // namespace load_balancer
}  // namespace tera

#endif  // TERA_LOAD_BALANCER_CLUSTER_H_
