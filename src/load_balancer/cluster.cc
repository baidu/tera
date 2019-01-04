// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <assert.h>

#include <algorithm>
#include <limits>

#include "glog/logging.h"
#include "load_balancer/actions.h"
#include "load_balancer/cluster.h"
#include "common/timer.h"

namespace tera {
namespace load_balancer {

Cluster::Cluster(const std::vector<std::shared_ptr<LBTabletNode>>& lb_nodes,
                 const LBOptions& options, bool skip_meta_node)
    : meta_table_node_index_(std::numeric_limits<uint32_t>::max()), lb_options_(options) {
  int64_t start_time_ns = get_micros();

  lb_nodes_.reserve(lb_nodes.size());
  for (const auto& node : lb_nodes) {
    if (skip_meta_node && node->tablet_node_ptr->GetAddr() == lb_options_.meta_table_node_addr) {
      VLOG(5) << "skip meta table node:" << lb_options_.meta_table_node_addr;
    } else {
      lb_nodes_.emplace_back(node);
    }
  }

  table_num_ = 0;
  tablet_node_num_ = 0;
  tablet_num_ = 0;
  tablet_moved_num_ = 0;

  for (const auto& node : lb_nodes_) {
    uint32_t node_index = nodes_.size();
    nodes_[node_index] = node;

    std::string addr = node->tablet_node_ptr->GetAddr();
    assert(nodes_to_index_.find(addr) == nodes_to_index_.end());
    nodes_to_index_[addr] = node_index;

    tablets_per_node_[node_index].clear();
    initial_tablets_not_ready_per_node_[node_index].clear();
    size_per_node_[node_index] = 0;
    flash_size_per_node_[node_index] = 0;
    read_load_per_node_[node_index] = 0;
    write_load_per_node_[node_index] = 0;
    scan_load_per_node_[node_index] = 0;
    lread_per_node_[node_index] = 0;

    uint64_t read_pending_num = node->tablet_node_ptr->GetReadPending();
    read_pending_per_node_[node_index] = read_pending_num;
    read_load_per_node_[node_index] += lb_options_.read_pending_factor * read_pending_num;

    uint64_t write_pending_num = node->tablet_node_ptr->GetWritePending();
    write_pending_per_node_[node_index] = write_pending_num;
    write_load_per_node_[node_index] += lb_options_.write_pending_factor * write_pending_num;

    uint64_t scan_pending_num = node->tablet_node_ptr->GetScanPending();
    scan_pending_per_node_[node_index] = scan_pending_num;
    scan_load_per_node_[node_index] += lb_options_.scan_pending_factor * scan_pending_num;

    for (const auto& tablet : node->tablets) {
      uint32_t tablet_index = tablets_.size();

      RegisterTablet(tablet, tablet_index, node_index);

      tablets_per_node_[node_index].emplace_back(tablet_index);
      if (tablets_[tablet_index]->tablet_ptr->GetStatus() != TabletMeta::kTabletReady) {
        initial_tablets_not_ready_per_node_[node_index].emplace_back(tablet_index);
      }
      size_per_node_[node_index] += static_cast<uint64_t>(tablet->tablet_ptr->GetDataSize());
      flash_size_per_node_[node_index] +=
          static_cast<uint64_t>(tablet->tablet_ptr->GetDataSizeOnFlash());
      read_load_per_node_[node_index] += static_cast<uint64_t>(tablet->tablet_ptr->GetReadQps());
      write_load_per_node_[node_index] += static_cast<uint64_t>(tablet->tablet_ptr->GetWriteQps());
      scan_load_per_node_[node_index] += static_cast<uint64_t>(tablet->tablet_ptr->GetScanQps());
      lread_per_node_[node_index] += static_cast<uint64_t>(tablet->tablet_ptr->GetLRead());

      ++tablet_num_;
    }

    ++tablet_node_num_;
  }

  // if not ready tablets' ratio is higher than option, the node is considered
  // abnormal
  for (uint32_t i = 0; i < tablets_per_node_.size(); ++i) {
    if (tablets_per_node_[i].size() != 0) {
      double note_ready_num = static_cast<double>(initial_tablets_not_ready_per_node_[i].size());
      double total_num = static_cast<double>(tablets_per_node_[i].size());
      if (note_ready_num / total_num >= lb_options_.abnormal_node_ratio) {
        abnormal_nodes_index_.insert(i);
      }
    }
  }

  assert(table_num_ == tables_.size());
  assert(tablet_node_num_ == nodes_.size());
  assert(tablet_num_ == tablets_.size());

  assert(table_num_ == tables_to_index_.size());
  assert(tablet_node_num_ == nodes_to_index_.size());
  assert(tablet_num_ == tablets_to_index_.size());

  assert(tablet_num_ == tablet_index_to_node_index_.size());
  assert(tablet_num_ == initial_tablet_index_to_node_index_.size());
  assert(tablet_num_ == tablet_index_to_table_index_.size());

  assert(tablet_node_num_ == tablets_per_node_.size());
  assert(tablet_node_num_ == initial_tablets_not_ready_per_node_.size());
  assert(tablet_node_num_ == size_per_node_.size());
  assert(tablet_node_num_ == flash_size_per_node_.size());
  assert(tablet_node_num_ == read_load_per_node_.size());
  assert(tablet_node_num_ == write_load_per_node_.size());
  assert(tablet_node_num_ == scan_load_per_node_.size());
  assert(tablet_node_num_ == lread_per_node_.size());
  assert(tablet_node_num_ == read_pending_per_node_.size());
  assert(tablet_node_num_ == write_pending_per_node_.size());
  assert(tablet_node_num_ == scan_pending_per_node_.size());
  assert(tablet_node_num_ >= abnormal_nodes_index_.size());

  VLOG(20) << "[lb] construct Cluster cost time(ms):" << (get_micros() - start_time_ns) / 1000;
}

Cluster::~Cluster() {}

void Cluster::DebugCluster() {
  LOG(INFO) << "";
  LOG(INFO) << "DebugCluster begin -----";

  LOG(INFO) << "table_num_:" << table_num_;
  LOG(INFO) << "tablet_node_num_:" << tablet_node_num_;
  LOG(INFO) << "tablet_num_:" << tablet_num_;
  LOG(INFO) << "tablet_moved_num_:" << tablet_moved_num_;

  LOG(INFO) << "[table_index -> table]:";
  for (const auto& table : tables_) {
    LOG(INFO) << table.first << " -> " << table.second;
  }

  LOG(INFO) << "[node_index -> node]:";
  for (const auto& node : nodes_) {
    LOG(INFO) << node.first << " -> " << node.second->tablet_node_ptr->GetAddr();
  }
  LOG(INFO) << "meta_table_node_index_:" << meta_table_node_index_;

  LOG(INFO) << "[tablet_index -> tablet]:";
  for (const auto& tablet : tablets_) {
    LOG(INFO) << tablet.first << " -> " << tablet.second->tablet_ptr->GetPath();
  }

  LOG(INFO) << "[table -> table_index]:";
  for (const auto& table : tables_to_index_) {
    LOG(INFO) << table.first << " -> " << table.second;
  }

  LOG(INFO) << "[node -> node_index]:";
  for (const auto& node : nodes_to_index_) {
    LOG(INFO) << node.first << " -> " << node.second;
  }

  LOG(INFO) << "[tablet -> tablet_index]:";
  for (const auto& tablet : tablets_to_index_) {
    LOG(INFO) << tablet.first << " -> " << tablet.second;
  }

  LOG(INFO) << "[tablet_index -> node_index]:";
  for (const auto& it : tablet_index_to_node_index_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[initial tablet_index -> node_index]:";
  for (const auto& it : initial_tablet_index_to_node_index_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[tablet_index -> table_index]:";
  for (const auto& it : tablet_index_to_table_index_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[node_index -> tablets index]:";
  for (const auto& it : tablets_per_node_) {
    std::string line = std::to_string(it.first) + " ->";
    for (const auto tablet : it.second) {
      line += " ";
      line += std::to_string(tablet);
    }
    LOG(INFO) << line;
  }

  LOG(INFO) << "[node_index -> data size]:";
  for (const auto& it : size_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second << "B";
  }

  LOG(INFO) << "[node_index -> flash data size]:";
  for (const auto& it : flash_size_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second << "B";
  }

  LOG(INFO) << "[node_index -> read load]:";
  for (const auto& it : read_load_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[node_index -> write load]:";
  for (const auto& it : write_load_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[node_index -> scan load]:";
  for (const auto& it : scan_load_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[node_index -> lread]:";
  for (const auto& it : lread_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[read pending nodes index]:";
  for (const auto& it : read_pending_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[write pending nodes index]:";
  for (const auto& it : write_pending_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[scan pending nodes index]:";
  for (const auto& it : scan_pending_per_node_) {
    LOG(INFO) << it.first << " -> " << it.second;
  }

  LOG(INFO) << "[node_index -> not ready tablets index]:";
  for (const auto& it : initial_tablets_not_ready_per_node_) {
    std::string line = std::to_string(it.first) + " ->";
    for (const auto tablet : it.second) {
      line += " ";
      line += std::to_string(tablet);
    }
    LOG(INFO) << line;
  }

  LOG(INFO) << "[abnormal nodes index]:";
  for (const auto& node : abnormal_nodes_index_) {
    LOG(INFO) << node;
  }

  LOG(INFO) << "DebugCluster end -----";
  LOG(INFO) << "";
}

bool Cluster::ValidAction(const std::shared_ptr<Action>& action) {
  switch (action->GetType()) {
    case Action::Type::EMPTY:
      return false;
    case Action::Type::ASSIGN:
      return true;
    case Action::Type::MOVE:
      return true;
    case Action::Type::SWAP:
      return true;
    default:
      return false;
  }
}

void Cluster::DoAction(const std::shared_ptr<Action>& action) {
  switch (action->GetType()) {
    case Action::Type::EMPTY:
      break;
    case Action::Type::ASSIGN:
      break;
    case Action::Type::MOVE: {
      MoveAction* move_action = dynamic_cast<MoveAction*>(action.get());
      VLOG(20) << "[lb] DoAction: " << move_action->ToString();
      assert(move_action->source_node_index_ != move_action->dest_node_index_);

      RemoveTablet(move_action->tablet_index_, move_action->source_node_index_);
      AddTablet(move_action->tablet_index_, move_action->dest_node_index_);
      MoveTablet(move_action->tablet_index_, move_action->source_node_index_,
                 move_action->dest_node_index_);

      break;
    }
    case Action::Type::SWAP:
      break;
    default:
      break;
  }
}

void Cluster::SortNodesByTabletCount(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : tablets_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(), [this](int a, int b) {
    return tablets_per_node_[a].size() < tablets_per_node_[b].size();
  });
}

void Cluster::SortNodesBySize(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : size_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(),
            [this](int a, int b) { return size_per_node_[a] < size_per_node_[b]; });
}

void Cluster::SortNodesByFlashSizePercent(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : flash_size_per_node_) {
    if (nodes_[e.first]->tablet_node_ptr->GetPersistentCacheSize() <= 0) {
      continue;
    }
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(), [this](int a, int b) {
    assert(nodes_[a]->tablet_node_ptr->GetPersistentCacheSize() > 0);
    assert(nodes_[b]->tablet_node_ptr->GetPersistentCacheSize() > 0);
    return static_cast<double>(flash_size_per_node_[a]) /
               nodes_[a]->tablet_node_ptr->GetPersistentCacheSize() <
           static_cast<double>(flash_size_per_node_[b]) /
               nodes_[b]->tablet_node_ptr->GetPersistentCacheSize();
  });
}

void Cluster::SortNodesByReadLoad(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : read_load_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(),
            [this](int a, int b) { return read_load_per_node_[a] < read_load_per_node_[b]; });
}

void Cluster::SortNodesByWriteLoad(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : write_load_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(),
            [this](int a, int b) { return write_load_per_node_[a] < write_load_per_node_[b]; });
}

void Cluster::SortNodesByScanLoad(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : scan_load_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(),
            [this](int a, int b) { return scan_load_per_node_[a] < scan_load_per_node_[b]; });
}

void Cluster::SortNodesByLRead(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : lread_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(),
            [this](int a, int b) { return lread_per_node_[a] < lread_per_node_[b]; });
}

void Cluster::SortNodesByComplexLoad(std::vector<uint32_t>* sorted_node_index) {
  sorted_node_index->clear();

  for (const auto& e : size_per_node_) {
    sorted_node_index->emplace_back(e.first);
  }

  std::sort(sorted_node_index->begin(), sorted_node_index->end(),
            [this](int a, int b) { return size_per_node_[a] < size_per_node_[b]; });
}

void Cluster::SortTabletsOfNodeByReadLoad(uint32_t node_index,
                                          std::vector<uint32_t>* sorted_tablet_index) {
  sorted_tablet_index->clear();

  assert(tablets_per_node_.find(node_index) != tablets_per_node_.end());
  for (const auto& e : tablets_per_node_[node_index]) {
    sorted_tablet_index->emplace_back(e);
  }

  std::sort(sorted_tablet_index->begin(), sorted_tablet_index->end(), [this](int a, int b) {
    return tablets_[a]->tablet_ptr->GetReadQps() < tablets_[b]->tablet_ptr->GetReadQps();
  });
}

void Cluster::SortTabletsOfNodeByWriteLoad(uint32_t node_index,
                                           std::vector<uint32_t>* sorted_tablet_index) {
  sorted_tablet_index->clear();

  assert(tablets_per_node_.find(node_index) != tablets_per_node_.end());
  for (const auto& e : tablets_per_node_[node_index]) {
    sorted_tablet_index->emplace_back(e);
  }

  std::sort(sorted_tablet_index->begin(), sorted_tablet_index->end(), [this](int a, int b) {
    return tablets_[a]->tablet_ptr->GetWriteQps() < tablets_[b]->tablet_ptr->GetWriteQps();
  });
}

void Cluster::SortTabletsOfNodeByScanLoad(uint32_t node_index,
                                          std::vector<uint32_t>* sorted_tablet_index) {
  sorted_tablet_index->clear();

  assert(tablets_per_node_.find(node_index) != tablets_per_node_.end());
  for (const auto& e : tablets_per_node_[node_index]) {
    sorted_tablet_index->emplace_back(e);
  }

  std::sort(sorted_tablet_index->begin(), sorted_tablet_index->end(), [this](int a, int b) {
    return tablets_[a]->tablet_ptr->GetScanQps() < tablets_[b]->tablet_ptr->GetScanQps();
  });
}

void Cluster::SortTabletsOfNodeByLRead(uint32_t node_index,
                                       std::vector<uint32_t>* sorted_tablet_index) {
  sorted_tablet_index->clear();

  assert(tablets_per_node_.find(node_index) != tablets_per_node_.end());
  for (const auto& e : tablets_per_node_[node_index]) {
    sorted_tablet_index->emplace_back(e);
  }

  std::sort(sorted_tablet_index->begin(), sorted_tablet_index->end(), [this](int a, int b) {
    return tablets_[a]->tablet_ptr->GetLRead() < tablets_[b]->tablet_ptr->GetLRead();
  });
}

bool Cluster::IsMetaNode(uint32_t node_index) {
  assert(nodes_.find(node_index) != nodes_.end());
  return node_index == meta_table_node_index_;
}

bool Cluster::IsReadyNode(uint32_t node_index) {
  assert(nodes_.find(node_index) != nodes_.end());
  if (nodes_[node_index]->tablet_node_ptr->GetState() == tera::master::kReady) {
    return true;
  } else {
    return false;
  }
}

bool Cluster::IsReadPendingNode(uint32_t node_index) {
  assert(read_pending_per_node_.find(node_index) != read_pending_per_node_.end());
  return read_pending_per_node_[node_index] > 0;
}

bool Cluster::IsWritePendingNode(uint32_t node_index) {
  assert(write_pending_per_node_.find(node_index) != write_pending_per_node_.end());
  return write_pending_per_node_[node_index] > 0;
}

bool Cluster::IsScanPendingNode(uint32_t node_index) {
  assert(scan_pending_per_node_.find(node_index) != scan_pending_per_node_.end());
  return scan_pending_per_node_[node_index] > 0;
}

bool Cluster::IsPendingNode(uint32_t node_index) {
  if (IsReadPendingNode(node_index) || IsWritePendingNode(node_index) ||
      IsScanPendingNode(node_index)) {
    return true;
  } else {
    return false;
  }
}

bool Cluster::IsHeavyReadPendingNode(uint32_t node_index) {
  assert(read_pending_per_node_.find(node_index) != read_pending_per_node_.end());
  return read_pending_per_node_[node_index] >= lb_options_.heavy_read_pending_threshold;
}

bool Cluster::IsHeavyWritePendingNode(uint32_t node_index) {
  assert(write_pending_per_node_.find(node_index) != write_pending_per_node_.end());
  return write_pending_per_node_[node_index] >= lb_options_.heavy_write_pending_threshold;
}

bool Cluster::IsHeavyScanPendingNode(uint32_t node_index) {
  assert(scan_pending_per_node_.find(node_index) != scan_pending_per_node_.end());
  return scan_pending_per_node_[node_index] >= lb_options_.heavy_scan_pending_threshold;
}

bool Cluster::IsHeavyPendingNode(uint32_t node_index) {
  if (IsHeavyReadPendingNode(node_index) || IsHeavyWritePendingNode(node_index) ||
      IsHeavyScanPendingNode(node_index)) {
    return true;
  } else {
    return false;
  }
}

uint32_t Cluster::HeavyPendingNodeNum() {
  uint32_t num = 0;
  for (auto& it : nodes_) {
    if (IsHeavyPendingNode(it.first)) {
      ++num;
    }
  }
  return num;
}

bool Cluster::IsHeavyLReadNode(uint32_t node_index) {
  assert(lread_per_node_.find(node_index) != lread_per_node_.end());
  if (lread_per_node_[node_index] >= lb_options_.heavy_lread_threshold) {
    return true;
  } else {
    return false;
  }
}

bool Cluster::IsAbnormalNode(uint32_t node_index) {
  if (abnormal_nodes_index_.find(node_index) == abnormal_nodes_index_.end()) {
    return false;
  } else {
    return true;
  }
}

bool Cluster::IsFlashSizeEnough(uint32_t tablet_index, uint32_t node_index) {
  assert(tablets_.find(tablet_index) != tablets_.end());
  assert(nodes_.find(node_index) != nodes_.end());

  if (lb_options_.flash_size_cost_weight == 0) {
    return true;
  }

  if (tablets_[tablet_index]->tablet_ptr->HasFlashLg() &&
      nodes_[node_index]->tablet_node_ptr->GetPersistentCacheSize() == 0) {
    return false;
  }

  return true;
}

bool Cluster::IsProperLocation(uint32_t tablet_index, uint32_t node_index) {
  if (lb_options_.meta_table_isolate_enabled && IsMetaNode(node_index)) {
    return false;
  }

  if (!IsReadyNode(node_index)) {
    return false;
  }

  if (IsPendingNode(node_index)) {
    return false;
  }

  if (IsHeavyLReadNode(node_index)) {
    return false;
  }

  if (IsAbnormalNode(node_index)) {
    return false;
  }

  if (!IsFlashSizeEnough(tablet_index, node_index)) {
    return false;
  }

  return true;
}

bool Cluster::IsMetaTablet(uint32_t tablet_index) {
  assert(tablets_.find(tablet_index) != tablets_.end());

  if (tables_[tablet_index_to_table_index_[tablet_index]] == lb_options_.meta_table_name) {
    return true;
  } else {
    return false;
  }
}

bool Cluster::IsReadyTablet(uint32_t tablet_index) {
  assert(tablets_.find(tablet_index) != tablets_.end());

  if (tablets_[tablet_index]->tablet_ptr->GetStatus() == TabletMeta::kTabletReady) {
    return true;
  } else {
    return false;
  }
}

bool Cluster::IsTabletMoveTooFrequent(uint32_t tablet_index) {
  assert(tablets_.find(tablet_index) != tablets_.end());

  int64_t last_move_time_us = tablets_[tablet_index]->tablet_ptr->LastMoveTime();
  int64_t current_time_us = get_micros();
  if (current_time_us - last_move_time_us <
      1000000 * static_cast<int64_t>(lb_options_.tablet_move_too_frequently_threshold_s)) {
    VLOG(20) << "[lb] tablet move too frequently, tablet index: " << tablet_index
             << ", path: " << tablets_[tablet_index]->tablet_ptr->GetPath()
             << ", last_move_time: " << last_move_time_us << ", current time: " << current_time_us;
    return true;
  } else {
    return false;
  }
}

bool Cluster::IsProperTargetTablet(uint32_t tablet_index) {
  if (IsMetaTablet(tablet_index)) {
    return false;
  }

  if (!IsReadyTablet(tablet_index)) {
    return false;
  }

  if (IsTabletMoveTooFrequent(tablet_index)) {
    return false;
  }

  return true;
}

void Cluster::RegisterTablet(const std::shared_ptr<LBTablet>& tablet, uint32_t tablet_index,
                             uint32_t node_index) {
  std::string table_name = tablet->tablet_ptr->GetTableName();
  if (tables_to_index_.find(table_name) == tables_to_index_.end()) {
    uint32_t table_index = tables_.size();
    tables_[table_index] = table_name;
    tables_to_index_[table_name] = table_index;
    ++table_num_;

    if (table_name == lb_options_.meta_table_name) {
      meta_table_node_index_ = node_index;
    }
  }

  std::string path = tablet->tablet_ptr->GetPath();
  tablets_to_index_[path] = tablet_index;
  tablets_[tablet_index] = tablet;

  tablet_index_to_node_index_[tablet_index] = node_index;
  initial_tablet_index_to_node_index_[tablet_index] = node_index;
  tablet_index_to_table_index_[tablet_index] = tables_to_index_[table_name];
}

void Cluster::AddTablet(uint32_t tablet_index, uint32_t to_node_index) {
  tablets_per_node_[to_node_index].emplace_back(tablet_index);

  size_per_node_[to_node_index] +=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetDataSize());
  flash_size_per_node_[to_node_index] +=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetDataSizeOnFlash());
  read_load_per_node_[to_node_index] +=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetReadQps());
  write_load_per_node_[to_node_index] +=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetWriteQps());
  scan_load_per_node_[to_node_index] +=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetScanQps());
  lread_per_node_[to_node_index] +=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetLRead());
}

void Cluster::RemoveTablet(uint32_t tablet_index, uint32_t from_node_index) {
  if (tablets_per_node_.find(from_node_index) == tablets_per_node_.end()) {
    return;
  }
  auto& tablets = tablets_per_node_[from_node_index];
  for (auto it = tablets.begin(); it != tablets.end();) {
    if (*it == tablet_index) {
      it = tablets.erase(it);
      break;
    } else {
      ++it;
    }
  }

  size_per_node_[from_node_index] -=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetDataSize());
  flash_size_per_node_[from_node_index] -=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetDataSizeOnFlash());
  read_load_per_node_[from_node_index] -=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetReadQps());
  write_load_per_node_[from_node_index] -=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetWriteQps());
  scan_load_per_node_[from_node_index] -=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetScanQps());
  lread_per_node_[from_node_index] -=
      static_cast<uint64_t>(tablets_[tablet_index]->tablet_ptr->GetLRead());

  assert(size_per_node_[from_node_index] >= 0);
  assert(flash_size_per_node_[from_node_index] >= 0);
  assert(read_load_per_node_[from_node_index] >= 0);
  assert(write_load_per_node_[from_node_index] >= 0);
  assert(scan_load_per_node_[from_node_index] >= 0);
  assert(lread_per_node_[from_node_index] >= 0);
}

void Cluster::MoveTablet(uint32_t tablet_index, uint32_t source_node_index,
                         uint32_t dest_node_index) {
  tablet_index_to_node_index_[tablet_index] = dest_node_index;

  if (initial_tablet_index_to_node_index_[tablet_index] == source_node_index) {
    ++tablet_moved_num_;
  } else if (initial_tablet_index_to_node_index_[tablet_index] == dest_node_index) {
    // tablet moved back
    --tablet_moved_num_;
    assert(tablet_moved_num_ >= 0);
  }
}

}  // namespace load_balancer
}  // namespace tera
