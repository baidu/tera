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

class RandomActionGeneratorTest : public ::testing::Test {
public:
    virtual void SetUp() {
        random_action_generator_.reset(new RandomActionGenerator());

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<RandomActionGenerator> random_action_generator_;
    std::shared_ptr<Cluster> cluster_;
};

class TabletCountActionGeneratorTest : public ::testing::Test {
public:
    virtual void SetUp() {
        tablet_count_action_generator_.reset(new TabletCountActionGenerator());

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<TabletCountActionGenerator> tablet_count_action_generator_;
    std::shared_ptr<Cluster> cluster_;
};

class SizeActionGeneratorTest : public ::testing::Test {
public:
    virtual void SetUp() {
        size_action_generator_.reset(new SizeActionGenerator());

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<SizeActionGenerator> size_action_generator_;
    std::shared_ptr<Cluster> cluster_;
};

class ReadLoadActionGeneratorTest : public ::testing::Test {
public:
    virtual void SetUp() {
        read_load_action_generator_.reset(new ReadLoadActionGenerator());

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<ReadLoadActionGenerator> read_load_action_generator_;
    std::shared_ptr<Cluster> cluster_;
};

class WriteLoadActionGeneratorTest : public ::testing::Test {
public:
    virtual void SetUp() {
        write_load_action_generator_.reset(new WriteLoadActionGenerator());

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<WriteLoadActionGenerator> write_load_action_generator_;
    std::shared_ptr<Cluster> cluster_;
};

class ScanLoadActionGeneratorTest : public ::testing::Test {
public:
    virtual void SetUp() {
        scan_load_action_generator_.reset(new ScanLoadActionGenerator());

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<ScanLoadActionGenerator> scan_load_action_generator_;
    std::shared_ptr<Cluster> cluster_;
};

TEST_F(RandomActionGeneratorTest, PickNodeTest) {
    cluster_->tablet_node_num_ = 10;

    uint32_t index = random_action_generator_->PickRandomNode(cluster_);
    ASSERT_GE(index, 0);
    ASSERT_LT(index, cluster_->tablet_node_num_);

    uint32_t other_index = random_action_generator_->PickOtherRandomNode(cluster_, index);
    ASSERT_GE(other_index, 0);
    ASSERT_LT(other_index, cluster_->tablet_node_num_);
    ASSERT_NE(index, other_index);
}

TEST_F(RandomActionGeneratorTest, PickRandomTabletOfNodeTest) {
    cluster_->tablet_node_num_ = 1;
    ASSERT_EQ(random_action_generator_->PickRandomTabletOfNode(cluster_, 0), std::numeric_limits<uint32_t>::max());

    cluster_->tablets_per_node_[0].emplace_back(0);
    ASSERT_EQ(random_action_generator_->PickRandomTabletOfNode(cluster_, 0), 0);
}

TEST_F(RandomActionGeneratorTest, GenerateTest) {
    cluster_->tablet_node_num_ = 1;
    std::shared_ptr<Action> action(random_action_generator_->Generate(cluster_));
    ASSERT_EQ(Action::Type::EMPTY, action->GetType());

    cluster_->tablet_node_num_ = 2;
    cluster_->tablets_per_node_[0].emplace_back(0);
    cluster_->tablets_per_node_[1].emplace_back(1);
    std::shared_ptr<Action> action_0(random_action_generator_->Generate(cluster_));
    ASSERT_EQ(Action::Type::MOVE, action_0->GetType());
}

TEST_F(TabletCountActionGeneratorTest, GenerateTest) {
    uint32_t more_tablets_node_index = 0;
    uint32_t less_tablets_node_index = 1;
    cluster_->tablets_per_node_[more_tablets_node_index].emplace_back(0);
    cluster_->tablets_per_node_[more_tablets_node_index].emplace_back(1);
    cluster_->tablets_per_node_[less_tablets_node_index].emplace_back(2);

    cluster_->tablet_node_num_ = 2;

    cluster_->node_index_sorted_by_tablet_count_.emplace_back(more_tablets_node_index);
    cluster_->node_index_sorted_by_tablet_count_.emplace_back(less_tablets_node_index);

    cluster_->SortNodesByTabletCount();
    ASSERT_EQ(more_tablets_node_index, tablet_count_action_generator_->PickMostTabletsNode(cluster_));
    ASSERT_EQ(less_tablets_node_index, tablet_count_action_generator_->PickLeastTabletsNode(cluster_));

    std::shared_ptr<Action> action(tablet_count_action_generator_->Generate(cluster_));
    ASSERT_EQ(Action::Type::MOVE, action->GetType());
    MoveAction* move_action = dynamic_cast<MoveAction*>(action.get());
    ASSERT_EQ(more_tablets_node_index, move_action->source_node_index_);
    ASSERT_EQ(less_tablets_node_index, move_action->dest_node_index_);

    cluster_->meta_table_node_index_ = less_tablets_node_index;
    ASSERT_EQ(more_tablets_node_index, tablet_count_action_generator_->PickMostTabletsNode(cluster_));
    ASSERT_EQ(more_tablets_node_index, tablet_count_action_generator_->PickLeastTabletsNode(cluster_));
}

TEST_F(SizeActionGeneratorTest, GenerateTest) {
    uint32_t larger_size_node_index = 0;
    uint32_t smaller_size_node_index = 1;
    cluster_->size_per_node_[larger_size_node_index] = 20;
    cluster_->size_per_node_[smaller_size_node_index] = 10;

    uint32_t tablet_index_on_larger_size_node = 0;
    uint32_t tablet_index_on_smaller_size_node = 1;
    cluster_->tablet_node_num_ = 2;
    cluster_->tablets_per_node_[larger_size_node_index].emplace_back(tablet_index_on_larger_size_node);
    cluster_->tablets_per_node_[smaller_size_node_index].emplace_back(tablet_index_on_smaller_size_node);

    cluster_->node_index_sorted_by_size_.emplace_back(larger_size_node_index);
    cluster_->node_index_sorted_by_size_.emplace_back(smaller_size_node_index);

    cluster_->SortNodesBySize();
    ASSERT_EQ(larger_size_node_index, size_action_generator_->PickLargestSizeNode(cluster_));
    ASSERT_EQ(smaller_size_node_index, size_action_generator_->PickSmallestSizeNode(cluster_));

    std::shared_ptr<Action> action(size_action_generator_->Generate(cluster_));
    ASSERT_EQ(Action::Type::MOVE, action->GetType());
    MoveAction* move_action = dynamic_cast<MoveAction*>(action.get());
    ASSERT_EQ(tablet_index_on_larger_size_node, move_action->tablet_index_);
    ASSERT_EQ(larger_size_node_index, move_action->source_node_index_);
    ASSERT_EQ(smaller_size_node_index, move_action->dest_node_index_);

    cluster_->meta_table_node_index_ = smaller_size_node_index;
    ASSERT_EQ(larger_size_node_index, size_action_generator_->PickLargestSizeNode(cluster_));
    ASSERT_EQ(larger_size_node_index, size_action_generator_->PickSmallestSizeNode(cluster_));
}

TEST_F(ReadLoadActionGeneratorTest, GenerateTest) {
    uint32_t more_read_node_index = 0;
    uint32_t less_read_node_index = 1;
    cluster_->read_load_per_node_[more_read_node_index] = 20;
    cluster_->read_load_per_node_[less_read_node_index] = 10;

    uint32_t tablet_index_on_more_read_node = 0;
    uint32_t tablet_index_on_less_read_node = 1;
    cluster_->tablet_node_num_ = 2;
    cluster_->tablets_per_node_[more_read_node_index].emplace_back(tablet_index_on_more_read_node);
    cluster_->tablets_per_node_[less_read_node_index].emplace_back(tablet_index_on_less_read_node);

    cluster_->node_index_sorted_by_read_load_.emplace_back(more_read_node_index);
    cluster_->node_index_sorted_by_read_load_.emplace_back(less_read_node_index);

    cluster_->SortNodesByReadLoad();
    ASSERT_EQ(more_read_node_index, read_load_action_generator_->PickMostReadNode(cluster_));
    ASSERT_EQ(less_read_node_index, read_load_action_generator_->PickLeastReadNode(cluster_));

    cluster_->read_pending_nodes_index_.clear();
    ASSERT_EQ(kInvalidNodeIndex, read_load_action_generator_->PickMostReadNodeWithPending(cluster_));
    ASSERT_EQ(less_read_node_index, read_load_action_generator_->PickLeastReadNode(cluster_));

    cluster_->read_pending_nodes_index_.clear();
    cluster_->read_pending_nodes_index_.insert(more_read_node_index);
    ASSERT_EQ(more_read_node_index, read_load_action_generator_->PickMostReadNodeWithPending(cluster_));
    ASSERT_EQ(less_read_node_index, read_load_action_generator_->PickLeastReadNode(cluster_));

    cluster_->read_pending_nodes_index_.clear();
    cluster_->read_pending_nodes_index_.insert(less_read_node_index);
    ASSERT_EQ(less_read_node_index, read_load_action_generator_->PickMostReadNodeWithPending(cluster_));
    ASSERT_EQ(more_read_node_index, read_load_action_generator_->PickLeastReadNode(cluster_));
    cluster_->read_pending_nodes_index_.clear();

    cluster_->meta_table_node_index_ = less_read_node_index;
    ASSERT_EQ(more_read_node_index, read_load_action_generator_->PickMostReadNode(cluster_));
    ASSERT_EQ(more_read_node_index, read_load_action_generator_->PickLeastReadNode(cluster_));
}

TEST_F(WriteLoadActionGeneratorTest, GenerateTest) {
    uint32_t more_write_node_index = 0;
    uint32_t less_write_node_index = 1;
    cluster_->write_load_per_node_[more_write_node_index] = 20;
    cluster_->write_load_per_node_[less_write_node_index] = 10;

    uint32_t tablet_index_on_more_write_node = 0;
    uint32_t tablet_index_on_less_write_node = 1;
    cluster_->tablet_node_num_ = 2;
    cluster_->tablets_per_node_[more_write_node_index].emplace_back(tablet_index_on_more_write_node);
    cluster_->tablets_per_node_[less_write_node_index].emplace_back(tablet_index_on_less_write_node);

    cluster_->node_index_sorted_by_write_load_.emplace_back(more_write_node_index);
    cluster_->node_index_sorted_by_write_load_.emplace_back(less_write_node_index);

    cluster_->SortNodesByWriteLoad();
    ASSERT_EQ(more_write_node_index, write_load_action_generator_->PickMostWriteNode(cluster_));
    ASSERT_EQ(less_write_node_index, write_load_action_generator_->PickLeastWriteNode(cluster_));

    cluster_->write_pending_nodes_index_.clear();
    ASSERT_EQ(kInvalidNodeIndex, write_load_action_generator_->PickMostWriteNodeWithPending(cluster_));
    ASSERT_EQ(less_write_node_index, write_load_action_generator_->PickLeastWriteNode(cluster_));

    cluster_->write_pending_nodes_index_.clear();
    cluster_->write_pending_nodes_index_.insert(more_write_node_index);
    ASSERT_EQ(more_write_node_index, write_load_action_generator_->PickMostWriteNodeWithPending(cluster_));
    ASSERT_EQ(less_write_node_index, write_load_action_generator_->PickLeastWriteNode(cluster_));

    cluster_->write_pending_nodes_index_.clear();
    cluster_->write_pending_nodes_index_.insert(less_write_node_index);
    ASSERT_EQ(less_write_node_index, write_load_action_generator_->PickMostWriteNodeWithPending(cluster_));
    ASSERT_EQ(more_write_node_index, write_load_action_generator_->PickLeastWriteNode(cluster_));
    cluster_->write_pending_nodes_index_.clear();

    cluster_->meta_table_node_index_ = less_write_node_index;
    ASSERT_EQ(more_write_node_index, write_load_action_generator_->PickMostWriteNode(cluster_));
    ASSERT_EQ(more_write_node_index, write_load_action_generator_->PickLeastWriteNode(cluster_));
}

TEST_F(ScanLoadActionGeneratorTest, GenerateTest) {
    uint32_t more_scan_node_index = 0;
    uint32_t less_scan_node_index = 1;
    cluster_->scan_load_per_node_[more_scan_node_index] = 20;
    cluster_->scan_load_per_node_[less_scan_node_index] = 10;

    uint32_t tablet_index_on_more_scan_node = 0;
    uint32_t tablet_index_on_less_scan_node = 1;
    cluster_->tablet_node_num_ = 2;
    cluster_->tablets_per_node_[more_scan_node_index].emplace_back(tablet_index_on_more_scan_node);
    cluster_->tablets_per_node_[less_scan_node_index].emplace_back(tablet_index_on_less_scan_node);

    cluster_->node_index_sorted_by_scan_load_.emplace_back(more_scan_node_index);
    cluster_->node_index_sorted_by_scan_load_.emplace_back(less_scan_node_index);

    cluster_->SortNodesByScanLoad();
    ASSERT_EQ(more_scan_node_index, scan_load_action_generator_->PickMostScanNode(cluster_));
    ASSERT_EQ(less_scan_node_index, scan_load_action_generator_->PickLeastScanNode(cluster_));

    cluster_->scan_pending_nodes_index_.clear();
    ASSERT_EQ(kInvalidNodeIndex, scan_load_action_generator_->PickMostScanNodeWithPending(cluster_));
    ASSERT_EQ(less_scan_node_index, scan_load_action_generator_->PickLeastScanNode(cluster_));

    cluster_->scan_pending_nodes_index_.clear();
    cluster_->scan_pending_nodes_index_.insert(more_scan_node_index);
    ASSERT_EQ(more_scan_node_index, scan_load_action_generator_->PickMostScanNodeWithPending(cluster_));
    ASSERT_EQ(less_scan_node_index, scan_load_action_generator_->PickLeastScanNode(cluster_));

    cluster_->scan_pending_nodes_index_.clear();
    cluster_->scan_pending_nodes_index_.insert(less_scan_node_index);
    ASSERT_EQ(less_scan_node_index, scan_load_action_generator_->PickMostScanNodeWithPending(cluster_));
    ASSERT_EQ(more_scan_node_index, scan_load_action_generator_->PickLeastScanNode(cluster_));
    cluster_->scan_pending_nodes_index_.clear();

    cluster_->meta_table_node_index_ = less_scan_node_index;
    ASSERT_EQ(more_scan_node_index, scan_load_action_generator_->PickMostScanNode(cluster_));
    ASSERT_EQ(more_scan_node_index, scan_load_action_generator_->PickLeastScanNode(cluster_));
}

} // namespace load_balancer
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
