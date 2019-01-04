// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "load_balancer/action_generators.h"

#include <assert.h>

#include <limits>

#include "glog/logging.h"
#include "load_balancer/actions.h"
#include "load_balancer/random.h"

namespace tera {
namespace load_balancer {

RandomActionGenerator::RandomActionGenerator() : name_("RandomActionGenerator") {}

RandomActionGenerator::~RandomActionGenerator() {}

Action* RandomActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] RandomActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  uint32_t source_node_index = PickRandomNode(cluster);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  uint32_t tablet_index = PickRandomTabletFromSourceNode(cluster, source_node_index, is_proper);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  uint32_t dest_node_index =
      PickRandomDestNode(cluster, source_node_index, tablet_index, is_proper_location);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

std::string RandomActionGenerator::Name() { return name_; }

TabletCountActionGenerator::TabletCountActionGenerator() : name_("TabletCountActionGenerator") {}

TabletCountActionGenerator::~TabletCountActionGenerator() {}

Action* TabletCountActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] TabletCountActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByTabletCount(&sorted_node_index);

  uint32_t source_node_index = PickMostTabletsNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  uint32_t tablet_index = PickRandomTabletFromSourceNode(cluster, source_node_index, is_proper);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickLeastTabletsNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t TabletCountActionGenerator::PickMostTabletsNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index) {
  return PickHeaviestNode(cluster, sorted_node_index);
}

uint32_t TabletCountActionGenerator::PickLeastTabletsNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index,
    uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string TabletCountActionGenerator::Name() { return name_; }

SizeActionGenerator::SizeActionGenerator() : name_("SizeActionGenerator") {}

SizeActionGenerator::~SizeActionGenerator() {}

Action* SizeActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] SizeActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesBySize(&sorted_node_index);

  uint32_t source_node_index = PickLargestSizeNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  uint32_t tablet_index = PickRandomTabletFromSourceNode(cluster, source_node_index, is_proper);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickSmallestSizeNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t SizeActionGenerator::PickLargestSizeNode(const std::shared_ptr<Cluster>& cluster,
                                                  const std::vector<uint32_t>& sorted_node_index) {
  return PickHeaviestNode(cluster, sorted_node_index);
}

uint32_t SizeActionGenerator::PickSmallestSizeNode(const std::shared_ptr<Cluster>& cluster,
                                                   const std::vector<uint32_t>& sorted_node_index,
                                                   uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string SizeActionGenerator::Name() { return name_; }

FlashSizeActionGenerator::FlashSizeActionGenerator() : name_("FlashSizeActionGenerator") {}

FlashSizeActionGenerator::~FlashSizeActionGenerator() {}

Action* FlashSizeActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] FlashSizeActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByFlashSizePercent(&sorted_node_index);

  uint32_t source_node_index = PickHighestFlashSizePercentNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  uint32_t tablet_index = PickRandomTabletFromSourceNode(cluster, source_node_index, is_proper);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index =
      PickLowestFlashSizePercentNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t FlashSizeActionGenerator::PickHighestFlashSizePercentNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index) {
  return PickHeaviestNode(cluster, sorted_node_index);
}

uint32_t FlashSizeActionGenerator::PickLowestFlashSizePercentNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index,
    uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string FlashSizeActionGenerator::Name() { return name_; }

ReadLoadActionGenerator::ReadLoadActionGenerator() : name_("ReadLoadActionGenerator") {}

ReadLoadActionGenerator::~ReadLoadActionGenerator() {}

Action* ReadLoadActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] ReadLoadActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByReadLoad(&sorted_node_index);

  uint32_t source_node_index = PickMostReadNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_tablet_index;
  cluster->SortTabletsOfNodeByReadLoad(source_node_index, &sorted_tablet_index);

  uint32_t tablet_index = PickMostReadTabletFromSourceNode(cluster, sorted_tablet_index);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickLeastReadNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t ReadLoadActionGenerator::PickMostReadNode(const std::shared_ptr<Cluster>& cluster,
                                                   const std::vector<uint32_t>& sorted_node_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_read_pending_node =
      std::bind(&Cluster::IsReadPendingNode, cluster.get(), _1);

  return PickHeaviestNode(cluster, sorted_node_index, is_read_pending_node);
}

uint32_t ReadLoadActionGenerator::PickMostReadTabletFromSourceNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_tablet_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  return PickHeaviestTabletFromSourceNode(cluster, sorted_tablet_index, is_proper);
}

uint32_t ReadLoadActionGenerator::PickLeastReadNode(const std::shared_ptr<Cluster>& cluster,
                                                    const std::vector<uint32_t>& sorted_node_index,
                                                    uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string ReadLoadActionGenerator::Name() { return name_; }

WriteLoadActionGenerator::WriteLoadActionGenerator()
    : name_("WriteLoadActionGenerator"), last_chosen_dest_node_index_(kInvalidNodeIndex) {}

WriteLoadActionGenerator::~WriteLoadActionGenerator() {}

Action* WriteLoadActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] WriteLoadActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByWriteLoad(&sorted_node_index);

  uint32_t source_node_index = PickMostWriteNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_tablet_index;
  cluster->SortTabletsOfNodeByWriteLoad(source_node_index, &sorted_tablet_index);

  uint32_t tablet_index = PickMostWriteTabletFromSourceNode(cluster, sorted_tablet_index);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickLeastWriteNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t WriteLoadActionGenerator::PickMostWriteNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_write_pending_node =
      std::bind(&Cluster::IsWritePendingNode, cluster.get(), _1);

  return PickHeaviestNode(cluster, sorted_node_index, is_write_pending_node);
}

uint32_t WriteLoadActionGenerator::PickMostWriteTabletFromSourceNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_tablet_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  return PickHeaviestTabletFromSourceNode(cluster, sorted_tablet_index, is_proper);
}

uint32_t WriteLoadActionGenerator::PickLeastWriteNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index,
    uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string WriteLoadActionGenerator::Name() { return name_; }

ScanLoadActionGenerator::ScanLoadActionGenerator() : name_("ScanLoadActionGenerator") {}

ScanLoadActionGenerator::~ScanLoadActionGenerator() {}

Action* ScanLoadActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] ScanLoadActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByScanLoad(&sorted_node_index);

  uint32_t source_node_index = PickMostScanNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_tablet_index;
  cluster->SortTabletsOfNodeByScanLoad(source_node_index, &sorted_tablet_index);

  uint32_t tablet_index = PickMostScanTabletFromSourceNode(cluster, sorted_tablet_index);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickLeastScanNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t ScanLoadActionGenerator::PickMostScanNode(const std::shared_ptr<Cluster>& cluster,
                                                   const std::vector<uint32_t>& sorted_node_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_scan_pending_node =
      std::bind(&Cluster::IsScanPendingNode, cluster.get(), _1);

  return PickHeaviestNode(cluster, sorted_node_index, is_scan_pending_node);
}

uint32_t ScanLoadActionGenerator::PickMostScanTabletFromSourceNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_tablet_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  return PickHeaviestTabletFromSourceNode(cluster, sorted_tablet_index, is_proper);
}

uint32_t ScanLoadActionGenerator::PickLeastScanNode(const std::shared_ptr<Cluster>& cluster,
                                                    const std::vector<uint32_t>& sorted_node_index,
                                                    uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string ScanLoadActionGenerator::Name() { return name_; }

LReadActionGenerator::LReadActionGenerator() : name_("LReadActionGenerator") {}

LReadActionGenerator::~LReadActionGenerator() {}

Action* LReadActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] LReadActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByLRead(&sorted_node_index);

  uint32_t source_node_index = PickMostLReadNode(cluster, sorted_node_index);
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_tablet_index;
  cluster->SortTabletsOfNodeByLRead(source_node_index, &sorted_tablet_index);

  uint32_t tablet_index = PickMostLReadTabletFromSourceNode(cluster, sorted_tablet_index);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickLeastLReadNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t LReadActionGenerator::PickMostLReadNode(const std::shared_ptr<Cluster>& cluster,
                                                 const std::vector<uint32_t>& sorted_node_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_heavy_lread_node =
      std::bind(&Cluster::IsHeavyLReadNode, cluster.get(), _1);

  return PickHeaviestNode(cluster, sorted_node_index, is_heavy_lread_node);
}

uint32_t LReadActionGenerator::PickMostLReadTabletFromSourceNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_tablet_index) {
  using std::placeholders::_1;
  std::function<bool(uint32_t)> is_proper =
      std::bind(&Cluster::IsProperTargetTablet, cluster.get(), _1);
  return PickHeaviestTabletFromSourceNode(cluster, sorted_tablet_index, is_proper);
}

uint32_t LReadActionGenerator::PickLeastLReadNode(const std::shared_ptr<Cluster>& cluster,
                                                  const std::vector<uint32_t>& sorted_node_index,
                                                  uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string LReadActionGenerator::Name() { return name_; }

MetaIsolateActionGenerator::MetaIsolateActionGenerator() : name_("MetaIsolateActionGenerator") {}

MetaIsolateActionGenerator::~MetaIsolateActionGenerator() {}

Action* MetaIsolateActionGenerator::Generate(const std::shared_ptr<Cluster>& cluster) {
  VLOG(20) << "[lb] MetaIsolateActionGenerator worked";

  if (cluster->tablet_node_num_ < 2) {
    return new EmptyAction();
  }

  std::vector<uint32_t> sorted_node_index;
  cluster->SortNodesByComplexLoad(&sorted_node_index);

  uint32_t source_node_index = cluster->meta_table_node_index_;
  if (source_node_index == kInvalidNodeIndex) {
    return new EmptyAction();
  }

  uint32_t tablet_index = PickRandomTabletOfMetaNode(cluster, source_node_index);
  if (tablet_index == kInvalidTabletIndex) {
    return new EmptyAction();
  }

  uint32_t dest_node_index = PickLeastComplexLoadNode(cluster, sorted_node_index, tablet_index);
  if (dest_node_index == kInvalidNodeIndex || dest_node_index == source_node_index) {
    return new EmptyAction();
  }

  return new MoveAction(tablet_index, source_node_index, dest_node_index, Name());
}

uint32_t MetaIsolateActionGenerator::PickRandomTabletOfMetaNode(
    const std::shared_ptr<Cluster>& cluster, uint32_t source_node_index) {
  uint32_t tablet_num = cluster->tablets_per_node_[source_node_index].size();
  if (tablet_num < 2) {
    return kInvalidTabletIndex;
  }

  while (true) {
    uint32_t rand = Random::Rand(0, tablet_num);
    uint32_t tablet_index = cluster->tablets_per_node_[source_node_index][rand];
    if (!cluster->IsMetaTablet(tablet_index)) {
      return tablet_index;
    }
  }
}

uint32_t MetaIsolateActionGenerator::PickLeastComplexLoadNode(
    const std::shared_ptr<Cluster>& cluster, const std::vector<uint32_t>& sorted_node_index,
    uint32_t chosen_tablet_index) {
  using std::placeholders::_1;
  using std::placeholders::_2;
  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      std::bind(&Cluster::IsProperLocation, cluster.get(), _1, _2);
  return PickLightestNode(cluster, sorted_node_index, chosen_tablet_index, is_proper_location);
}

std::string MetaIsolateActionGenerator::Name() { return name_; }

}  // namespace load_balancer
}  // namespace tera
