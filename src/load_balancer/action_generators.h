// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_LOAD_BALANCER_ACTION_GENERATORS_H_
#define TERA_LOAD_BALANCER_ACTION_GENERATORS_H_

#include <memory>

#include "load_balancer/action_generator.h"
#include "load_balancer/actions.h"

namespace tera {
namespace load_balancer {

class RandomActionGenerator : public ActionGenerator {
 public:
  RandomActionGenerator();
  virtual ~RandomActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  std::string name_;
};

class TabletCountActionGenerator : public ActionGenerator {
 public:
  TabletCountActionGenerator();
  virtual ~TabletCountActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickMostTabletsNode(const std::shared_ptr<Cluster>& cluster,
                               const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickLeastTabletsNode(const std::shared_ptr<Cluster>& cluster,
                                const std::vector<uint32_t>& sorted_node_index,
                                uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

class SizeActionGenerator : public ActionGenerator {
 public:
  SizeActionGenerator();
  virtual ~SizeActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickLargestSizeNode(const std::shared_ptr<Cluster>& cluster,
                               const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickSmallestSizeNode(const std::shared_ptr<Cluster>& cluster,
                                const std::vector<uint32_t>& sorted_node_index,
                                uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

class FlashSizeActionGenerator : public ActionGenerator {
 public:
  FlashSizeActionGenerator();
  virtual ~FlashSizeActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickHighestFlashSizePercentNode(const std::shared_ptr<Cluster>& cluster,
                                           const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickLowestFlashSizePercentNode(const std::shared_ptr<Cluster>& cluster,
                                          const std::vector<uint32_t>& sorted_node_index,
                                          uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

class ReadLoadActionGenerator : public ActionGenerator {
 public:
  ReadLoadActionGenerator();
  virtual ~ReadLoadActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickMostReadNode(const std::shared_ptr<Cluster>& cluster,
                            const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickMostReadTabletFromSourceNode(const std::shared_ptr<Cluster>& cluster,
                                            const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickLeastReadNode(const std::shared_ptr<Cluster>& cluster,
                             const std::vector<uint32_t>& sorted_node_index,
                             uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

class WriteLoadActionGenerator : public ActionGenerator {
 public:
  WriteLoadActionGenerator();
  virtual ~WriteLoadActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickMostWriteNode(const std::shared_ptr<Cluster>& cluster,
                             const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickMostWriteTabletFromSourceNode(const std::shared_ptr<Cluster>& cluster,
                                             const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickLeastWriteNode(const std::shared_ptr<Cluster>& cluster,
                              const std::vector<uint32_t>& sorted_node_index,
                              uint32_t chosen_tablet_index);

 private:
  std::string name_;
  uint32_t last_chosen_dest_node_index_;
};

class ScanLoadActionGenerator : public ActionGenerator {
 public:
  ScanLoadActionGenerator();
  virtual ~ScanLoadActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickMostScanNode(const std::shared_ptr<Cluster>& cluster,
                            const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickMostScanTabletFromSourceNode(const std::shared_ptr<Cluster>& cluster,
                                            const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickLeastScanNode(const std::shared_ptr<Cluster>& cluster,
                             const std::vector<uint32_t>& sorted_node_index,
                             uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

class LReadActionGenerator : public ActionGenerator {
 public:
  LReadActionGenerator();
  virtual ~LReadActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickMostLReadNode(const std::shared_ptr<Cluster>& cluster,
                             const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickMostLReadTabletFromSourceNode(const std::shared_ptr<Cluster>& cluster,
                                             const std::vector<uint32_t>& sorted_node_index);
  uint32_t PickLeastLReadNode(const std::shared_ptr<Cluster>& cluster,
                              const std::vector<uint32_t>& sorted_node_index,
                              uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

class MetaIsolateActionGenerator : public ActionGenerator {
 public:
  MetaIsolateActionGenerator();
  virtual ~MetaIsolateActionGenerator();

  virtual Action* Generate(const std::shared_ptr<Cluster>& cluster) override;

  virtual std::string Name() override;

 private:
  uint32_t PickRandomTabletOfMetaNode(const std::shared_ptr<Cluster>& cluster,
                                      uint32_t source_node_index);
  uint32_t PickLeastComplexLoadNode(const std::shared_ptr<Cluster>& cluster,
                                    const std::vector<uint32_t>& sorted_node_index,
                                    uint32_t chosen_tablet_index);

 private:
  std::string name_;
};

}  // namespace load_balancer
}  // namespace tera

#endif  // TERA_LOAD_BALANCER_ACTION_GENERATORS_H_
