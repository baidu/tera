// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <limits>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "load_balancer/action_generators.h"

namespace tera {
namespace load_balancer {

class ActionGeneratorTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
    LBOptions options;
    cluster_.reset(new Cluster(empty_lb_nodes, options, false));
  }

  virtual void TearDown() {}

 private:
  std::shared_ptr<Cluster> cluster_;
};

class MetaIsolateActionGeneratorTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    meta_isolate_action_generator_.reset(new MetaIsolateActionGenerator());

    std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
    LBOptions options;
    cluster_.reset(new Cluster(empty_lb_nodes, options, false));
  }

  virtual void TearDown() {}

 private:
  std::shared_ptr<MetaIsolateActionGenerator> meta_isolate_action_generator_;
  std::shared_ptr<Cluster> cluster_;
};

TEST_F(ActionGeneratorTest, PickRandomNodeTest) {
  cluster_->tablet_node_num_ = 10;

  for (uint32_t i = 0; i < 100; ++i) {
    uint32_t index = ActionGenerator::PickRandomNode(cluster_);
    ASSERT_GE(index, 0);
    ASSERT_LT(index, cluster_->tablet_node_num_);
  }
}

TEST_F(ActionGeneratorTest, PickRandomTabletFromSourceNodeTest1) {
  std::function<bool(uint32_t)> is_proper = [](uint32_t tablet_index) -> bool { return true; };
  ASSERT_EQ(ActionGenerator::PickRandomTabletFromSourceNode(cluster_, 0, is_proper),
            kInvalidTabletIndex);
}

TEST_F(ActionGeneratorTest, PickRandomTabletFromSourceNodeTest2) {
  cluster_->tablets_per_node_[0].emplace_back(0);
  std::function<bool(uint32_t)> is_proper = [](uint32_t tablet_index) -> bool { return true; };
  ASSERT_EQ(ActionGenerator::PickRandomTabletFromSourceNode(cluster_, 0, is_proper), 0);
}

TEST_F(ActionGeneratorTest, PickRandomTabletFromSourceNodeTest3) {
  cluster_->tablets_per_node_[0].emplace_back(0);
  std::function<bool(uint32_t)> is_not_proper = [](uint32_t tablet_index) -> bool { return false; };
  ASSERT_EQ(ActionGenerator::PickRandomTabletFromSourceNode(cluster_, 0, is_not_proper),
            kInvalidTabletIndex);
}

TEST_F(ActionGeneratorTest, PickRandomTabletFromSourceNodeTest4) {
  cluster_->tablets_per_node_[0].emplace_back(0);
  cluster_->tablets_per_node_[0].emplace_back(1);
  std::function<bool(uint32_t)> tablet0_is_not_proper = [](uint32_t tablet_index) -> bool {
    if (tablet_index == 0) {
      return false;
    } else {
      return true;
    }
  };
  ASSERT_EQ(ActionGenerator::PickRandomTabletFromSourceNode(cluster_, 0, tablet0_is_not_proper), 1);
}

TEST_F(ActionGeneratorTest, PickRandomDestNodeTest1) {
  cluster_->tablet_node_num_ = 0;
  uint32_t source_node_index = 0;
  uint32_t chosen_tablet_index = 0;

  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      [](uint32_t tablet_index, uint32_t node_index) -> bool { return true; };
  uint32_t dest_node_index = ActionGenerator::PickRandomDestNode(
      cluster_, source_node_index, chosen_tablet_index, is_proper_location);
  ASSERT_EQ(dest_node_index, kInvalidNodeIndex);

  cluster_->tablet_node_num_ = 1;
  dest_node_index = ActionGenerator::PickRandomDestNode(cluster_, source_node_index,
                                                        chosen_tablet_index, is_proper_location);
  ASSERT_EQ(dest_node_index, kInvalidNodeIndex);
}

TEST_F(ActionGeneratorTest, PickRandomDestNodeTest2) {
  cluster_->tablet_node_num_ = 2;
  uint32_t source_node_index = 0;
  uint32_t chosen_tablet_index = 0;

  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      [](uint32_t tablet_index, uint32_t node_index) -> bool { return true; };
  uint32_t dest_node_index = ActionGenerator::PickRandomDestNode(
      cluster_, source_node_index, chosen_tablet_index, is_proper_location);
  ASSERT_EQ(dest_node_index, 1);
}

TEST_F(ActionGeneratorTest, PickRandomDestNodeTest3) {
  cluster_->tablet_node_num_ = 2;
  uint32_t source_node_index = 0;
  uint32_t chosen_tablet_index = 0;

  std::function<bool(uint32_t, uint32_t)> is_not_proper_location =
      [](uint32_t tablet_index, uint32_t node_index) -> bool { return false; };
  uint32_t dest_node_index = ActionGenerator::PickRandomDestNode(
      cluster_, source_node_index, chosen_tablet_index, is_not_proper_location);
  ASSERT_EQ(dest_node_index, kInvalidNodeIndex);
}

TEST_F(ActionGeneratorTest, PickLightestNodeTest1) {
  std::vector<uint32_t> sorted_node_index;
  uint32_t chosen_tablet_index = 0;

  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      [](uint32_t tablet_index, uint32_t node_index) -> bool { return true; };
  ASSERT_EQ(kInvalidNodeIndex,
            ActionGenerator::PickLightestNode(cluster_, sorted_node_index, chosen_tablet_index,
                                              is_proper_location));
}

TEST_F(ActionGeneratorTest, PickLightestNodeTest2) {
  uint32_t heavier_node_index = 0;
  uint32_t lighter_node_index = 1;
  uint32_t chosen_tablet_index = 0;

  std::vector<uint32_t> sorted_node_index;
  sorted_node_index.emplace_back(lighter_node_index);
  sorted_node_index.emplace_back(heavier_node_index);

  std::function<bool(uint32_t, uint32_t)> is_proper_location =
      [](uint32_t tablet_index, uint32_t node_index) -> bool { return true; };
  ASSERT_EQ(lighter_node_index,
            ActionGenerator::PickLightestNode(cluster_, sorted_node_index, chosen_tablet_index,
                                              is_proper_location));
}

TEST_F(ActionGeneratorTest, PickLightestNodeTest3) {
  uint32_t heavier_node_index = 0;
  uint32_t lighter_node_index = 1;
  uint32_t chosen_tablet_index = 0;

  std::vector<uint32_t> sorted_node_index;
  sorted_node_index.emplace_back(lighter_node_index);
  sorted_node_index.emplace_back(heavier_node_index);

  std::function<bool(uint32_t, uint32_t)> lighter_is_not_proper_location =
      [lighter_node_index](uint32_t tablet_index, uint32_t node_index) -> bool {
        if (node_index == lighter_node_index) {
          return false;
        } else {
          return true;
        }
      };
  ASSERT_EQ(heavier_node_index,
            ActionGenerator::PickLightestNode(cluster_, sorted_node_index, chosen_tablet_index,
                                              lighter_is_not_proper_location));
}

TEST_F(ActionGeneratorTest, PickLightestNodeTest4) {
  uint32_t heavier_node_index = 0;
  uint32_t lighter_node_index = 1;
  uint32_t chosen_tablet_index = 0;

  std::vector<uint32_t> sorted_node_index;
  sorted_node_index.emplace_back(lighter_node_index);
  sorted_node_index.emplace_back(heavier_node_index);

  std::function<bool(uint32_t, uint32_t)> is_not_proper_location =
      [](uint32_t tablet_index, uint32_t node_index) -> bool { return false; };
  ASSERT_EQ(kInvalidNodeIndex,
            ActionGenerator::PickLightestNode(cluster_, sorted_node_index, chosen_tablet_index,
                                              is_not_proper_location));
}

TEST_F(ActionGeneratorTest, PickHeaviestNodeTest1) {
  std::vector<uint32_t> sorted_node_index;
  ASSERT_EQ(kInvalidNodeIndex, ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index));

  sorted_node_index.emplace_back(0);
  ASSERT_EQ(0, ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index));

  sorted_node_index.emplace_back(1);
  ASSERT_EQ(1, ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index));
}

TEST_F(ActionGeneratorTest, PickHeaviestNodeTest2) {
  std::function<bool(uint32_t)> is_proper = [](uint32_t node_index) -> bool { return true; };

  std::vector<uint32_t> sorted_node_index;
  ASSERT_EQ(kInvalidNodeIndex,
            ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index, is_proper));

  sorted_node_index.emplace_back(0);
  ASSERT_EQ(0, ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index, is_proper));

  sorted_node_index.emplace_back(1);
  ASSERT_EQ(1, ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index, is_proper));

  std::function<bool(uint32_t)> node1_is_not_proper = [](uint32_t node_index) -> bool {
    if (node_index == 1) {
      return false;
    } else {
      return true;
    }
  };
  ASSERT_EQ(0, ActionGenerator::PickHeaviestNode(cluster_, sorted_node_index, node1_is_not_proper));
}

TEST_F(ActionGeneratorTest, PickHeaviestTabletFromSourceNodeTest1) {
  std::vector<uint32_t> sorted_tablet_index;
  std::function<bool(uint32_t)> is_proper = [](uint32_t tablet_index) -> bool { return true; };
  ASSERT_EQ(
      ActionGenerator::PickHeaviestTabletFromSourceNode(cluster_, sorted_tablet_index, is_proper),
      kInvalidTabletIndex);
}

TEST_F(ActionGeneratorTest, PickHeaviestTabletFromSourceNodeTest2) {
  std::vector<uint32_t> sorted_tablet_index;
  sorted_tablet_index.emplace_back(0);

  std::function<bool(uint32_t)> is_proper = [](uint32_t tablet_index) -> bool { return true; };
  ASSERT_EQ(
      ActionGenerator::PickHeaviestTabletFromSourceNode(cluster_, sorted_tablet_index, is_proper),
      0);
}

TEST_F(ActionGeneratorTest, PickHeaviestTabletFromSourceNodeTest3) {
  std::vector<uint32_t> sorted_tablet_index;
  sorted_tablet_index.emplace_back(0);

  std::function<bool(uint32_t)> is_not_proper = [](uint32_t tablet_index) -> bool { return false; };
  ASSERT_EQ(kInvalidNodeIndex, ActionGenerator::PickHeaviestTabletFromSourceNode(
                                   cluster_, sorted_tablet_index, is_not_proper));
}

TEST_F(MetaIsolateActionGeneratorTest, PickRandomTabletOfMetaNodeTest1) {
  ASSERT_EQ(meta_isolate_action_generator_->PickRandomTabletOfMetaNode(cluster_, 0),
            kInvalidTabletIndex);

  cluster_->tablets_per_node_[0].emplace_back(0);
  ASSERT_EQ(meta_isolate_action_generator_->PickRandomTabletOfMetaNode(cluster_, 0),
            kInvalidTabletIndex);
}

TEST_F(MetaIsolateActionGeneratorTest, PickRandomTabletOfMetaNodeTest2) {
  cluster_->tablets_per_node_[0].emplace_back(0);
  cluster_->tablets_per_node_[0].emplace_back(1);
  cluster_->tablets_[0];
  cluster_->tablets_[1];
  cluster_->tablet_index_to_table_index_[0] = 0;
  cluster_->tablet_index_to_table_index_[1] = 1;
  cluster_->tables_[0] = cluster_->lb_options_.meta_table_name;
  cluster_->tables_[1] = "user_table";

  ASSERT_TRUE(cluster_->IsMetaTablet(0));
  ASSERT_FALSE(cluster_->IsMetaTablet(1));
  ASSERT_EQ(meta_isolate_action_generator_->PickRandomTabletOfMetaNode(cluster_, 0), 1);
}

}  // namespace load_balancer
}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
