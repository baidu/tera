// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "load_balancer/actions.h"
#include "load_balancer/cluster.h"
#include "load_balancer/lb_node.h"
#include "common/timer.h"

namespace tera {
namespace load_balancer {

class ClusterTest : public ::testing::Test {
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

TEST_F(ClusterTest, ValidActionTest) {
  std::shared_ptr<Action> empty_action(new EmptyAction());
  ASSERT_FALSE(cluster_->ValidAction(empty_action));
}

TEST_F(ClusterTest, SortNodesByTabletCount) {
  cluster_->tablets_per_node_[0].emplace_back(0);
  cluster_->tablets_per_node_[0].emplace_back(1);
  cluster_->tablets_per_node_[1].emplace_back(2);
  cluster_->tablets_per_node_[2].emplace_back(3);
  cluster_->tablets_per_node_[2].emplace_back(4);
  cluster_->tablets_per_node_[2].emplace_back(5);

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByTabletCount(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesBySizeTest) {
  cluster_->size_per_node_[0] = 20;
  cluster_->size_per_node_[1] = 10;
  cluster_->size_per_node_[2] = 30;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesBySize(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesByFlashSizePercentTest1) {
  cluster_->flash_size_per_node_[0] = 20;
  cluster_->flash_size_per_node_[1] = 10;
  cluster_->flash_size_per_node_[2] = 30;

  tera::master::TabletNodePtr tablet_node_ptr_0(new tera::master::TabletNode());
  tablet_node_ptr_0->persistent_cache_size_ = 1;
  std::shared_ptr<LBTabletNode> lb_node_0 = std::make_shared<LBTabletNode>();
  lb_node_0->tablet_node_ptr = tablet_node_ptr_0;
  cluster_->nodes_[0] = lb_node_0;

  tera::master::TabletNodePtr tablet_node_ptr_1(new tera::master::TabletNode());
  tablet_node_ptr_1->persistent_cache_size_ = 1;
  std::shared_ptr<LBTabletNode> lb_node_1 = std::make_shared<LBTabletNode>();
  lb_node_1->tablet_node_ptr = tablet_node_ptr_1;
  cluster_->nodes_[1] = lb_node_1;

  tera::master::TabletNodePtr tablet_node_ptr_2(new tera::master::TabletNode());
  tablet_node_ptr_2->persistent_cache_size_ = 1;
  std::shared_ptr<LBTabletNode> lb_node_2 = std::make_shared<LBTabletNode>();
  lb_node_2->tablet_node_ptr = tablet_node_ptr_2;
  cluster_->nodes_[2] = lb_node_2;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByFlashSizePercent(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesByFlashSizePercentTest2) {
  cluster_->flash_size_per_node_[0] = 20;
  cluster_->flash_size_per_node_[1] = 10;
  cluster_->flash_size_per_node_[2] = 30;

  tera::master::TabletNodePtr tablet_node_ptr_0(new tera::master::TabletNode());
  tablet_node_ptr_0->persistent_cache_size_ = 1;
  std::shared_ptr<LBTabletNode> lb_node_0 = std::make_shared<LBTabletNode>();
  lb_node_0->tablet_node_ptr = tablet_node_ptr_0;
  cluster_->nodes_[0] = lb_node_0;

  tera::master::TabletNodePtr tablet_node_ptr_1(new tera::master::TabletNode());
  tablet_node_ptr_1->persistent_cache_size_ = 1;
  std::shared_ptr<LBTabletNode> lb_node_1 = std::make_shared<LBTabletNode>();
  lb_node_1->tablet_node_ptr = tablet_node_ptr_1;
  cluster_->nodes_[1] = lb_node_1;

  tera::master::TabletNodePtr tablet_node_ptr_2(new tera::master::TabletNode());
  tablet_node_ptr_2->persistent_cache_size_ = 0;
  std::shared_ptr<LBTabletNode> lb_node_2 = std::make_shared<LBTabletNode>();
  lb_node_2->tablet_node_ptr = tablet_node_ptr_2;
  cluster_->nodes_[2] = lb_node_2;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByFlashSizePercent(&sorted_node_index);
  ASSERT_EQ(sorted_node_index.size(), 2);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
}

TEST_F(ClusterTest, SortNodesByReadLoad) {
  cluster_->read_load_per_node_[0] = 20;
  cluster_->read_load_per_node_[1] = 10;
  cluster_->read_load_per_node_[2] = 30;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByReadLoad(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesByWriteLoad) {
  cluster_->write_load_per_node_[0] = 20;
  cluster_->write_load_per_node_[1] = 10;
  cluster_->write_load_per_node_[2] = 30;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByWriteLoad(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesByScanLoad) {
  cluster_->scan_load_per_node_[0] = 20;
  cluster_->scan_load_per_node_[1] = 10;
  cluster_->scan_load_per_node_[2] = 30;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByScanLoad(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesByLRead) {
  cluster_->lread_per_node_[0] = 20;
  cluster_->lread_per_node_[1] = 10;
  cluster_->lread_per_node_[2] = 30;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByLRead(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortNodesByComplexLoadTest) {
  cluster_->size_per_node_[0] = 20;
  cluster_->size_per_node_[1] = 10;
  cluster_->size_per_node_[2] = 30;

  std::vector<uint32_t> sorted_node_index;
  cluster_->SortNodesByComplexLoad(&sorted_node_index);

  ASSERT_EQ(1, sorted_node_index[0]);
  ASSERT_EQ(0, sorted_node_index[1]);
  ASSERT_EQ(2, sorted_node_index[2]);
}

TEST_F(ClusterTest, SortTabletsOfNodeByReadTest) {
  TabletMeta tablet_meta_low;
  TabletMeta tablet_meta_middle;
  TabletMeta tablet_meta_high;

  tera::master::TabletPtr tablet_ptr_low(new tera::master::Tablet(tablet_meta_low));
  tera::master::TabletPtr tablet_ptr_middle(new tera::master::Tablet(tablet_meta_middle));
  tera::master::TabletPtr tablet_ptr_high(new tera::master::Tablet(tablet_meta_high));

  tablet_ptr_low->average_counter_.set_read_rows(10);
  tablet_ptr_middle->average_counter_.set_read_rows(20);
  tablet_ptr_high->average_counter_.set_read_rows(30);

  std::shared_ptr<LBTablet> lb_tablet_low = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_middle = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_high = std::make_shared<LBTablet>();

  lb_tablet_low->tablet_ptr = tablet_ptr_low;
  lb_tablet_middle->tablet_ptr = tablet_ptr_middle;
  lb_tablet_high->tablet_ptr = tablet_ptr_high;

  uint32_t tablet_index_low = 0;
  uint32_t tablet_index_middle = 1;
  uint32_t tablet_index_high = 2;

  cluster_->tablets_[tablet_index_low] = lb_tablet_low;
  cluster_->tablets_[tablet_index_middle] = lb_tablet_middle;
  cluster_->tablets_[tablet_index_high] = lb_tablet_high;

  uint32_t node_index = 0;
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_low);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_high);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_middle);

  std::vector<uint32_t> sorted_tablet_index;
  cluster_->SortTabletsOfNodeByReadLoad(node_index, &sorted_tablet_index);

  ASSERT_EQ(tablet_index_low, sorted_tablet_index[0]);
  ASSERT_EQ(tablet_index_middle, sorted_tablet_index[1]);
  ASSERT_EQ(tablet_index_high, sorted_tablet_index[2]);
}

TEST_F(ClusterTest, SortTabletsOfNodeByWriteLoadTest) {
  TabletMeta tablet_meta_low;
  TabletMeta tablet_meta_middle;
  TabletMeta tablet_meta_high;

  tera::master::TabletPtr tablet_ptr_low(new tera::master::Tablet(tablet_meta_low));
  tera::master::TabletPtr tablet_ptr_middle(new tera::master::Tablet(tablet_meta_middle));
  tera::master::TabletPtr tablet_ptr_high(new tera::master::Tablet(tablet_meta_high));

  tablet_ptr_low->average_counter_.set_write_rows(10);
  tablet_ptr_middle->average_counter_.set_write_rows(20);
  tablet_ptr_high->average_counter_.set_write_rows(30);

  std::shared_ptr<LBTablet> lb_tablet_low = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_middle = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_high = std::make_shared<LBTablet>();

  lb_tablet_low->tablet_ptr = tablet_ptr_low;
  lb_tablet_middle->tablet_ptr = tablet_ptr_middle;
  lb_tablet_high->tablet_ptr = tablet_ptr_high;

  uint32_t tablet_index_low = 0;
  uint32_t tablet_index_middle = 1;
  uint32_t tablet_index_high = 2;

  cluster_->tablets_[tablet_index_low] = lb_tablet_low;
  cluster_->tablets_[tablet_index_middle] = lb_tablet_middle;
  cluster_->tablets_[tablet_index_high] = lb_tablet_high;

  uint32_t node_index = 0;
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_low);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_high);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_middle);

  std::vector<uint32_t> sorted_tablet_index;
  cluster_->SortTabletsOfNodeByWriteLoad(node_index, &sorted_tablet_index);

  ASSERT_EQ(tablet_index_low, sorted_tablet_index[0]);
  ASSERT_EQ(tablet_index_middle, sorted_tablet_index[1]);
  ASSERT_EQ(tablet_index_high, sorted_tablet_index[2]);
}

TEST_F(ClusterTest, SortTabletsOfNodeByScanLoad) {
  TabletMeta tablet_meta_low;
  TabletMeta tablet_meta_middle;
  TabletMeta tablet_meta_high;

  tera::master::TabletPtr tablet_ptr_low(new tera::master::Tablet(tablet_meta_low));
  tera::master::TabletPtr tablet_ptr_middle(new tera::master::Tablet(tablet_meta_middle));
  tera::master::TabletPtr tablet_ptr_high(new tera::master::Tablet(tablet_meta_high));

  tablet_ptr_low->average_counter_.set_scan_rows(10);
  tablet_ptr_middle->average_counter_.set_scan_rows(20);
  tablet_ptr_high->average_counter_.set_scan_rows(30);

  std::shared_ptr<LBTablet> lb_tablet_low = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_middle = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_high = std::make_shared<LBTablet>();

  lb_tablet_low->tablet_ptr = tablet_ptr_low;
  lb_tablet_middle->tablet_ptr = tablet_ptr_middle;
  lb_tablet_high->tablet_ptr = tablet_ptr_high;

  uint32_t tablet_index_low = 0;
  uint32_t tablet_index_middle = 1;
  uint32_t tablet_index_high = 2;

  cluster_->tablets_[tablet_index_low] = lb_tablet_low;
  cluster_->tablets_[tablet_index_middle] = lb_tablet_middle;
  cluster_->tablets_[tablet_index_high] = lb_tablet_high;

  uint32_t node_index = 0;
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_low);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_high);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_middle);

  std::vector<uint32_t> sorted_tablet_index;
  cluster_->SortTabletsOfNodeByScanLoad(node_index, &sorted_tablet_index);

  ASSERT_EQ(tablet_index_low, sorted_tablet_index[0]);
  ASSERT_EQ(tablet_index_middle, sorted_tablet_index[1]);
  ASSERT_EQ(tablet_index_high, sorted_tablet_index[2]);
}

TEST_F(ClusterTest, SortTabletsOfNodeByLRead) {
  TabletMeta tablet_meta_low;
  TabletMeta tablet_meta_middle;
  TabletMeta tablet_meta_high;

  tera::master::TabletPtr tablet_ptr_low(new tera::master::Tablet(tablet_meta_low));
  tera::master::TabletPtr tablet_ptr_middle(new tera::master::Tablet(tablet_meta_middle));
  tera::master::TabletPtr tablet_ptr_high(new tera::master::Tablet(tablet_meta_high));

  tablet_ptr_low->average_counter_.set_low_read_cell(10);
  tablet_ptr_middle->average_counter_.set_low_read_cell(20);
  tablet_ptr_high->average_counter_.set_low_read_cell(30);

  std::shared_ptr<LBTablet> lb_tablet_low = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_middle = std::make_shared<LBTablet>();
  std::shared_ptr<LBTablet> lb_tablet_high = std::make_shared<LBTablet>();

  lb_tablet_low->tablet_ptr = tablet_ptr_low;
  lb_tablet_middle->tablet_ptr = tablet_ptr_middle;
  lb_tablet_high->tablet_ptr = tablet_ptr_high;

  uint32_t tablet_index_low = 0;
  uint32_t tablet_index_middle = 1;
  uint32_t tablet_index_high = 2;

  cluster_->tablets_[tablet_index_low] = lb_tablet_low;
  cluster_->tablets_[tablet_index_middle] = lb_tablet_middle;
  cluster_->tablets_[tablet_index_high] = lb_tablet_high;

  uint32_t node_index = 0;
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_low);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_high);
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index_middle);

  std::vector<uint32_t> sorted_tablet_index;
  cluster_->SortTabletsOfNodeByLRead(node_index, &sorted_tablet_index);

  ASSERT_EQ(tablet_index_low, sorted_tablet_index[0]);
  ASSERT_EQ(tablet_index_middle, sorted_tablet_index[1]);
  ASSERT_EQ(tablet_index_high, sorted_tablet_index[2]);
}

TEST_F(ClusterTest, IsMetaNodeTest) {
  cluster_->meta_table_node_index_ = 0;
  cluster_->nodes_[0];
  cluster_->nodes_[1];
  ASSERT_TRUE(cluster_->IsMetaNode(0));
  ASSERT_FALSE(cluster_->IsMetaNode(1));
}

TEST_F(ClusterTest, IsReadPendingNodeTest) {
  cluster_->read_pending_per_node_[0] = 0;
  ASSERT_FALSE(cluster_->IsReadPendingNode(0));
  cluster_->read_pending_per_node_[0] = 10;
  ASSERT_TRUE(cluster_->IsReadPendingNode(0));
}

TEST_F(ClusterTest, IsWritePendingNodeTest) {
  cluster_->write_pending_per_node_[0] = 0;
  ASSERT_FALSE(cluster_->IsWritePendingNode(0));
  cluster_->write_pending_per_node_[0] = 10;
  ASSERT_TRUE(cluster_->IsWritePendingNode(0));
}

TEST_F(ClusterTest, IsScanPendingNodeTest) {
  cluster_->scan_pending_per_node_[0] = 0;
  ASSERT_FALSE(cluster_->IsScanPendingNode(0));
  cluster_->scan_pending_per_node_[0] = 10;
  ASSERT_TRUE(cluster_->IsScanPendingNode(0));
}

TEST_F(ClusterTest, IsHeavyReadPendingNodeTest) {
  cluster_->read_pending_per_node_[0] = 10;
  ASSERT_FALSE(cluster_->IsHeavyReadPendingNode(0));
  cluster_->read_pending_per_node_[0] = 1000;
  ASSERT_TRUE(cluster_->IsHeavyReadPendingNode(0));
}

TEST_F(ClusterTest, IsHeavyWritePendingNodeTest) {
  cluster_->write_pending_per_node_[0] = 10;
  ASSERT_FALSE(cluster_->IsHeavyWritePendingNode(0));
  cluster_->write_pending_per_node_[0] = 1000;
  ASSERT_TRUE(cluster_->IsHeavyWritePendingNode(0));
}

TEST_F(ClusterTest, IsHeavyScanPendingNodeTest) {
  cluster_->scan_pending_per_node_[0] = 10;
  ASSERT_FALSE(cluster_->IsHeavyScanPendingNode(0));
  cluster_->scan_pending_per_node_[0] = 1000;
  ASSERT_TRUE(cluster_->IsHeavyScanPendingNode(0));
}

TEST_F(ClusterTest, HeavyPendingNodeNumTest) {
  cluster_->nodes_[0];
  cluster_->nodes_[1];
  cluster_->nodes_[2];
  cluster_->read_pending_per_node_[0] = 10;
  cluster_->write_pending_per_node_[0] = 0;
  cluster_->scan_pending_per_node_[0] = 0;

  cluster_->read_pending_per_node_[1] = 0;
  cluster_->write_pending_per_node_[1] = 1000;
  cluster_->scan_pending_per_node_[1] = 0;

  cluster_->read_pending_per_node_[2] = 0;
  cluster_->write_pending_per_node_[2] = 0;
  cluster_->scan_pending_per_node_[2] = 2000;

  ASSERT_EQ(2, cluster_->HeavyPendingNodeNum());
}

TEST_F(ClusterTest, IsHeavyLReadNodeTest) {
  uint32_t node_index = 0;
  cluster_->lread_per_node_[node_index] = 1000;
  ASSERT_FALSE(cluster_->IsHeavyLReadNode(node_index));
  cluster_->lread_per_node_[node_index] = 1000000;
  ASSERT_TRUE(cluster_->IsHeavyLReadNode(node_index));
}

TEST_F(ClusterTest, IsAbnormalNodeTest) {
  cluster_->abnormal_nodes_index_.insert(0);
  ASSERT_TRUE(cluster_->IsAbnormalNode(0));
  ASSERT_FALSE(cluster_->IsAbnormalNode(1));
}

TEST_F(ClusterTest, IsFlashSizeEnoughTest1) {
  cluster_->lb_options_.flash_size_cost_weight = 100;
  TableSchema schema;
  LocalityGroupSchema *lg_schema = schema.add_locality_groups();
  lg_schema->set_store_type(MemoryStore);
  tera::master::TablePtr table_ptr(new tera::master::Table("", schema, kTableEnable));

  TabletMeta tablet_meta;
  tera::master::TabletPtr tablet_ptr(new tera::master::Tablet(tablet_meta, table_ptr));
  std::shared_ptr<LBTablet> lb_tablet = std::make_shared<LBTablet>();
  lb_tablet->tablet_ptr = tablet_ptr;
  cluster_->tablets_[0] = lb_tablet;

  tera::master::TabletNodePtr tablet_node_ptr(new tera::master::TabletNode());
  tablet_node_ptr->persistent_cache_size_ = 1024;
  std::shared_ptr<LBTabletNode> lb_node = std::make_shared<LBTabletNode>();
  lb_node->tablet_node_ptr = tablet_node_ptr;
  cluster_->nodes_[0] = lb_node;

  ASSERT_TRUE(cluster_->IsFlashSizeEnough(0, 0));
}

TEST_F(ClusterTest, IsFlashSizeEnoughTest2) {
  cluster_->lb_options_.flash_size_cost_weight = 100;
  TableSchema schema;
  LocalityGroupSchema *lg_schema = schema.add_locality_groups();
  lg_schema->set_store_type(FlashStore);
  tera::master::TablePtr table_ptr(new tera::master::Table("", schema, kTableEnable));

  TabletMeta tablet_meta;
  tera::master::TabletPtr tablet_ptr(new tera::master::Tablet(tablet_meta, table_ptr));
  std::shared_ptr<LBTablet> lb_tablet = std::make_shared<LBTablet>();
  lb_tablet->tablet_ptr = tablet_ptr;
  cluster_->tablets_[0] = lb_tablet;

  tera::master::TabletNodePtr tablet_node_ptr(new tera::master::TabletNode());
  tablet_node_ptr->persistent_cache_size_ = 1024;
  std::shared_ptr<LBTabletNode> lb_node = std::make_shared<LBTabletNode>();
  lb_node->tablet_node_ptr = tablet_node_ptr;
  cluster_->nodes_[0] = lb_node;

  ASSERT_TRUE(cluster_->IsFlashSizeEnough(0, 0));
}

TEST_F(ClusterTest, IsFlashSizeEnoughTest3) {
  cluster_->lb_options_.flash_size_cost_weight = 100;
  TableSchema schema;
  LocalityGroupSchema *lg_schema = schema.add_locality_groups();
  lg_schema->set_store_type(FlashStore);
  tera::master::TablePtr table_ptr(new tera::master::Table("", schema, kTableEnable));

  TabletMeta tablet_meta;
  tera::master::TabletPtr tablet_ptr(new tera::master::Tablet(tablet_meta, table_ptr));
  std::shared_ptr<LBTablet> lb_tablet = std::make_shared<LBTablet>();
  lb_tablet->tablet_ptr = tablet_ptr;
  cluster_->tablets_[0] = lb_tablet;

  tera::master::TabletNodePtr tablet_node_ptr(new tera::master::TabletNode());
  tablet_node_ptr->persistent_cache_size_ = 0;
  std::shared_ptr<LBTabletNode> lb_node = std::make_shared<LBTabletNode>();
  lb_node->tablet_node_ptr = tablet_node_ptr;
  cluster_->nodes_[0] = lb_node;

  ASSERT_FALSE(cluster_->IsFlashSizeEnough(0, 0));
}

TEST_F(ClusterTest, IsFlashSizeEnoughTest4) {
  cluster_->lb_options_.flash_size_cost_weight = 100;
  TableSchema schema;
  LocalityGroupSchema *lg_schema = schema.add_locality_groups();
  lg_schema->set_store_type(DiskStore);
  tera::master::TablePtr table_ptr(new tera::master::Table("", schema, kTableEnable));

  TabletMeta tablet_meta;
  tera::master::TabletPtr tablet_ptr(new tera::master::Tablet(tablet_meta, table_ptr));
  std::shared_ptr<LBTablet> lb_tablet = std::make_shared<LBTablet>();
  lb_tablet->tablet_ptr = tablet_ptr;
  cluster_->tablets_[0] = lb_tablet;

  tera::master::TabletNodePtr tablet_node_ptr(new tera::master::TabletNode());
  tablet_node_ptr->persistent_cache_size_ = 0;
  std::shared_ptr<LBTabletNode> lb_node = std::make_shared<LBTabletNode>();
  lb_node->tablet_node_ptr = tablet_node_ptr;
  cluster_->nodes_[0] = lb_node;

  ASSERT_TRUE(cluster_->IsFlashSizeEnough(0, 0));
}

TEST_F(ClusterTest, IsMetaTabletTest1) {
  cluster_->tablets_[0];
  ASSERT_FALSE(cluster_->IsMetaTablet(0));
}

TEST_F(ClusterTest, IsMetaTabletTest2) {
  cluster_->lb_options_.meta_table_name = "meta_table";
  cluster_->tables_[0] = cluster_->lb_options_.meta_table_name;
  cluster_->tables_[1] = "user_table";
  cluster_->tablet_index_to_table_index_[0] = 0;
  cluster_->tablet_index_to_table_index_[1] = 1;
  cluster_->tablets_[0];
  cluster_->tablets_[1];
  ASSERT_TRUE(cluster_->IsMetaTablet(0));
  ASSERT_FALSE(cluster_->IsMetaTablet(1));
}

TEST_F(ClusterTest, IsTabletMoveTooFrequentTest1) {
  cluster_->lb_options_.tablet_move_too_frequently_threshold_s = 300;

  TabletMeta tablet_meta;
  tera::master::TabletPtr tablet_ptr(new tera::master::Tablet(tablet_meta));
  tablet_ptr->SetLastMoveTime(0);
  std::shared_ptr<LBTablet> lb_tablet = std::make_shared<LBTablet>();
  lb_tablet->tablet_ptr = tablet_ptr;
  cluster_->tablets_[0] = lb_tablet;

  ASSERT_FALSE(cluster_->IsTabletMoveTooFrequent(0));
}

TEST_F(ClusterTest, IsTabletMoveTooFrequentTest2) {
  cluster_->lb_options_.tablet_move_too_frequently_threshold_s = 300;

  TabletMeta tablet_meta;
  tera::master::TabletPtr tablet_ptr(new tera::master::Tablet(tablet_meta));
  tablet_ptr->SetLastMoveTime(get_micros());
  std::shared_ptr<LBTablet> lb_tablet = std::make_shared<LBTablet>();
  lb_tablet->tablet_ptr = tablet_ptr;
  cluster_->tablets_[0] = lb_tablet;

  ASSERT_TRUE(cluster_->IsTabletMoveTooFrequent(0));
}
TEST_F(ClusterTest, RegisterTabletTest) {
  TabletMeta tablet_meta_meta;
  tablet_meta_meta.set_table_name("meta_table");
  tablet_meta_meta.set_path("path/meta_table");
  tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
  std::shared_ptr<LBTablet> lb_tablet_meta = std::make_shared<LBTablet>();
  lb_tablet_meta->tablet_ptr = tablet_ptr_meta;

  uint32_t tablet_index_0 = 0;
  uint32_t node_index_0 = 0;
  cluster_->RegisterTablet(lb_tablet_meta, tablet_index_0, node_index_0);

  ASSERT_EQ(1, cluster_->table_num_);
  ASSERT_EQ(1, cluster_->tables_.size());
  ASSERT_STREQ("meta_table", cluster_->tables_[0].c_str());
  ASSERT_EQ(0, cluster_->tables_to_index_["meta_table"]);

  ASSERT_EQ(tablet_index_0, cluster_->tablets_to_index_["path/meta_table"]);

  ASSERT_EQ(node_index_0, cluster_->tablet_index_to_node_index_[tablet_index_0]);
  ASSERT_EQ(node_index_0, cluster_->initial_tablet_index_to_node_index_[tablet_index_0]);
  ASSERT_EQ(0, cluster_->tablet_index_to_table_index_[tablet_index_0]);
}

TEST_F(ClusterTest, AddTabletTest) {
  TabletMeta tablet_meta_meta;
  tablet_meta_meta.set_size(10);
  tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
  tablet_ptr_meta->SetDataSizeOnFlash(1024);
  tablet_ptr_meta->average_counter_.set_read_rows(20);
  tablet_ptr_meta->average_counter_.set_write_rows(30);
  tablet_ptr_meta->average_counter_.set_scan_rows(40);
  std::shared_ptr<LBTablet> lb_tablet_meta = std::make_shared<LBTablet>();
  lb_tablet_meta->tablet_ptr = tablet_ptr_meta;

  uint32_t tablet_index = 0;
  cluster_->tablets_[tablet_index] = lb_tablet_meta;

  uint32_t node_index = 0;
  cluster_->size_per_node_[node_index] = 0;
  cluster_->flash_size_per_node_[node_index] = 0;
  cluster_->read_load_per_node_[node_index] = 0;
  cluster_->write_load_per_node_[node_index] = 0;
  cluster_->scan_load_per_node_[node_index] = 0;

  cluster_->AddTablet(tablet_index, node_index);

  ASSERT_EQ(1, cluster_->tablets_per_node_.size());
  ASSERT_EQ(10, cluster_->size_per_node_[node_index]);
  ASSERT_EQ(1024, cluster_->flash_size_per_node_[node_index]);
  ASSERT_EQ(20, cluster_->read_load_per_node_[node_index]);
  ASSERT_EQ(30, cluster_->write_load_per_node_[node_index]);
  ASSERT_EQ(40, cluster_->scan_load_per_node_[node_index]);
}

TEST_F(ClusterTest, RemoveTabletTest) {
  TabletMeta tablet_meta_meta;
  tablet_meta_meta.set_size(10);
  tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
  tablet_ptr_meta->SetDataSizeOnFlash(1024);
  tablet_ptr_meta->average_counter_.set_read_rows(20);
  tablet_ptr_meta->average_counter_.set_write_rows(30);
  tablet_ptr_meta->average_counter_.set_scan_rows(40);
  std::shared_ptr<LBTablet> lb_tablet_meta = std::make_shared<LBTablet>();
  lb_tablet_meta->tablet_ptr = tablet_ptr_meta;

  uint32_t tablet_index = 0;
  cluster_->tablets_[tablet_index] = lb_tablet_meta;

  uint32_t node_index = 0;
  cluster_->tablets_per_node_[node_index].emplace_back(tablet_index);

  cluster_->size_per_node_[node_index] = 10;
  cluster_->flash_size_per_node_[node_index] = 1024;
  cluster_->read_load_per_node_[node_index] = 20;
  cluster_->write_load_per_node_[node_index] = 30;
  cluster_->scan_load_per_node_[node_index] = 40;

  cluster_->RemoveTablet(tablet_index, node_index);

  ASSERT_EQ(0, cluster_->tablets_per_node_[node_index].size());
  ASSERT_EQ(0, cluster_->size_per_node_[node_index]);
  ASSERT_EQ(0, cluster_->flash_size_per_node_[node_index]);
  ASSERT_EQ(0, cluster_->read_load_per_node_[node_index]);
  ASSERT_EQ(0, cluster_->write_load_per_node_[node_index]);
  ASSERT_EQ(0, cluster_->scan_load_per_node_[node_index]);
}

TEST_F(ClusterTest, MoveTabletTest) {
  TabletMeta tablet_meta_meta;
  tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
  std::shared_ptr<LBTablet> lb_tablet_meta = std::make_shared<LBTablet>();
  lb_tablet_meta->tablet_ptr = tablet_ptr_meta;

  uint32_t tablet_index = 0;
  uint32_t first_node_index = 0;
  uint32_t second_node_index = 1;
  uint32_t third_node_index = 2;

  cluster_->tablets_[tablet_index] = lb_tablet_meta;
  cluster_->tablet_moved_num_ = 0;
  cluster_->initial_tablet_index_to_node_index_[tablet_index] = first_node_index;
  cluster_->tablet_index_to_node_index_[tablet_index] = first_node_index;

  cluster_->MoveTablet(tablet_index, first_node_index, second_node_index);
  ASSERT_EQ(first_node_index, cluster_->initial_tablet_index_to_node_index_[tablet_index]);
  ASSERT_EQ(second_node_index, cluster_->tablet_index_to_node_index_[tablet_index]);
  ASSERT_EQ(1, cluster_->tablet_moved_num_);

  cluster_->MoveTablet(tablet_index, second_node_index, third_node_index);
  ASSERT_EQ(first_node_index, cluster_->initial_tablet_index_to_node_index_[tablet_index]);
  ASSERT_EQ(third_node_index, cluster_->tablet_index_to_node_index_[tablet_index]);
  ASSERT_EQ(1, cluster_->tablet_moved_num_);

  cluster_->MoveTablet(tablet_index, third_node_index, first_node_index);
  ASSERT_EQ(first_node_index, cluster_->initial_tablet_index_to_node_index_[tablet_index]);
  ASSERT_EQ(first_node_index, cluster_->tablet_index_to_node_index_[tablet_index]);
  ASSERT_EQ(0, cluster_->tablet_moved_num_);
}

}  // namespace load_balancer
}  // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
