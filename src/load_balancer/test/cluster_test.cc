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
        cluster_.reset(new Cluster(empty_lb_nodes, options));
    }

    virtual void TearDown() {
    }

private:
    std::shared_ptr<Cluster> cluster_;
};

TEST_F(ClusterTest, ValidActionTest) {
    TabletMeta tablet_meta_meta;
    TabletMeta tablet_meta_other;
    tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
    tera::master::TabletPtr tablet_ptr_other(new tera::master::Tablet(tablet_meta_other));
    std::shared_ptr<LBTablet> lb_tablet_meta = std::make_shared<LBTablet>();
    std::shared_ptr<LBTablet> lb_tablet_other = std::make_shared<LBTablet>();
    lb_tablet_meta->tablet_ptr = tablet_ptr_meta;
    lb_tablet_other->tablet_ptr = tablet_ptr_other;

    cluster_->lb_options_.meta_table_name = "meta_table";
    uint32_t table_index_meta = 0;
    uint32_t table_index_other= 1;
    cluster_->tables_[table_index_meta] = "meta_table";
    cluster_->tables_[table_index_other] = "other_table";
    uint32_t tablet_index_meta = 0;
    uint32_t tablet_index_other = 1;
    cluster_->tablet_index_to_table_index_[tablet_index_meta] = table_index_meta;
    cluster_->tablet_index_to_table_index_[tablet_index_other] = table_index_other;
    cluster_->tablets_[tablet_index_meta] = lb_tablet_meta;
    cluster_->tablets_[tablet_index_other] = lb_tablet_other;

    uint32_t meta_table_node_index = 0;
    uint32_t other_node_index = 1;
    cluster_->meta_table_node_index_ = meta_table_node_index;

    // empty action is invalid
    std::shared_ptr<Action> empty_action(new EmptyAction());
    ASSERT_FALSE(cluster_->ValidAction(empty_action));

    std::shared_ptr<Action> normal_move_action(new MoveAction(tablet_index_meta, 0, 1));
    // move not ready tablet is invalid
    ASSERT_TRUE(cluster_->tablets_[tablet_index_meta]->tablet_ptr->SetStatus(kTableOffLine));
    ASSERT_FALSE(cluster_->ValidAction(normal_move_action));

    // move meta table is invalid
    std::shared_ptr<Action> move_meta_table_action(new MoveAction(tablet_index_meta, 0, 1));
    ASSERT_TRUE(cluster_->tablets_[tablet_index_meta]->tablet_ptr->SetStatus(kTableReady));
    ASSERT_FALSE(cluster_->ValidAction(move_meta_table_action));
    // move nomal tablet is valid
    std::shared_ptr<Action> move_other_table_action(new MoveAction(tablet_index_other, 0, 1));
    ASSERT_TRUE(cluster_->tablets_[tablet_index_other]->tablet_ptr->SetStatus(kTableReady));
    ASSERT_TRUE(cluster_->ValidAction(move_other_table_action));

    std::shared_ptr<Action> move_to_meta_table_node_action(new MoveAction(tablet_index_other, 0, meta_table_node_index));
    std::shared_ptr<Action> move_to_other_node_action(new MoveAction(tablet_index_other, 0, other_node_index));
    cluster_->lb_options_.meta_table_isolate_enabled = true;
    // move tablet to meta node is invalid if meta_table_isolate_enabled is true
    ASSERT_FALSE(cluster_->ValidAction(move_to_meta_table_node_action));
    // move tablet to normal node is valid even if meta_table_isolate_enabled is true
    ASSERT_TRUE(cluster_->ValidAction(move_to_other_node_action));
    cluster_->lb_options_.meta_table_isolate_enabled = false;
    // move tablet to any node is valid if meta_table_isolate_enabled is true
    ASSERT_TRUE(cluster_->ValidAction(move_to_meta_table_node_action));
    ASSERT_TRUE(cluster_->ValidAction(move_to_other_node_action));
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
    tablet_ptr_meta->average_counter_.set_read_rows(20);
    tablet_ptr_meta->average_counter_.set_write_rows(30);
    tablet_ptr_meta->average_counter_.set_scan_rows(40);
    std::shared_ptr<LBTablet> lb_tablet_meta = std::make_shared<LBTablet>();
    lb_tablet_meta->tablet_ptr = tablet_ptr_meta;

    uint32_t tablet_index = 0;
    cluster_->tablets_[tablet_index] = lb_tablet_meta;

    uint32_t node_index = 0;
    cluster_->size_per_node_[node_index] = 0;
    cluster_->read_load_per_node_[node_index] = 0;
    cluster_->write_load_per_node_[node_index] = 0;
    cluster_->scan_load_per_node_[node_index] = 0;

    cluster_->AddTablet(tablet_index, node_index);

    ASSERT_EQ(1, cluster_->tablets_per_node_.size());
    ASSERT_EQ(10, cluster_->size_per_node_[node_index]);
    ASSERT_EQ(20, cluster_->read_load_per_node_[node_index]);
    ASSERT_EQ(30, cluster_->write_load_per_node_[node_index]);
    ASSERT_EQ(40, cluster_->scan_load_per_node_[node_index]);
}

TEST_F(ClusterTest, RemoveTabletTest) {
    TabletMeta tablet_meta_meta;
    tablet_meta_meta.set_size(10);
    tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
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
    cluster_->read_load_per_node_[node_index] = 20;
    cluster_->write_load_per_node_[node_index] = 30;
    cluster_->scan_load_per_node_[node_index] = 40;

    cluster_->RemoveTablet(tablet_index, node_index);

    ASSERT_EQ(0, cluster_->tablets_per_node_[node_index].size());
    ASSERT_EQ(0, cluster_->size_per_node_[node_index]);
    ASSERT_EQ(0, cluster_->read_load_per_node_[node_index]);
    ASSERT_EQ(0, cluster_->write_load_per_node_[node_index]);
    ASSERT_EQ(0, cluster_->scan_load_per_node_[node_index]);
}

TEST_F(ClusterTest, MoveTabletTest) {
    TabletMeta tablet_meta_meta;
    tablet_meta_meta.set_size(10);
    tera::master::TabletPtr tablet_ptr_meta(new tera::master::Tablet(tablet_meta_meta));
    tablet_ptr_meta->average_counter_.set_read_rows(20);
    tablet_ptr_meta->average_counter_.set_write_rows(30);
    tablet_ptr_meta->average_counter_.set_scan_rows(40);
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
    cluster_->abnormal_nodes_index_.insert(second_node_index);
    cluster_->read_pending_nodes_index_.insert(second_node_index);
    cluster_->write_pending_nodes_index_.insert(second_node_index);
    cluster_->scan_pending_nodes_index_.insert(second_node_index);

    ASSERT_EQ(0, cluster_->tablets_moved_too_frequently_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_abnormal_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_read_pending_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_write_pending_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_scan_pending_nodes_.size());
    ASSERT_TRUE(cluster_->tablets_[tablet_index]->tablet_ptr->SetStatus(kTableReady));
    int64_t current_time_us = tera::get_micros();
    cluster_->lb_options_.tablet_move_too_frequently_threshold_s = 600;
    cluster_->tablets_[tablet_index]->tablet_ptr->last_move_time_us_ = current_time_us;

    cluster_->MoveTablet(tablet_index, first_node_index, second_node_index);
    ASSERT_EQ(first_node_index, cluster_->initial_tablet_index_to_node_index_[tablet_index]);
    ASSERT_EQ(second_node_index, cluster_->tablet_index_to_node_index_[tablet_index]);
    ASSERT_EQ(1, cluster_->tablet_moved_num_);
    ASSERT_EQ(1, cluster_->tablets_moved_too_frequently_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_abnormal_nodes_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_read_pending_nodes_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_write_pending_nodes_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_scan_pending_nodes_.size());

    cluster_->MoveTablet(tablet_index, second_node_index, third_node_index);
    ASSERT_EQ(first_node_index, cluster_->initial_tablet_index_to_node_index_[tablet_index]);
    ASSERT_EQ(third_node_index, cluster_->tablet_index_to_node_index_[tablet_index]);
    ASSERT_EQ(1, cluster_->tablet_moved_num_);
    ASSERT_EQ(1, cluster_->tablets_moved_too_frequently_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_abnormal_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_read_pending_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_write_pending_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_scan_pending_nodes_.size());

    cluster_->MoveTablet(tablet_index, third_node_index, first_node_index);
    ASSERT_EQ(first_node_index, cluster_->initial_tablet_index_to_node_index_[tablet_index]);
    ASSERT_EQ(first_node_index, cluster_->tablet_index_to_node_index_[tablet_index]);
    ASSERT_EQ(0, cluster_->tablet_moved_num_);
    ASSERT_EQ(0, cluster_->tablets_moved_too_frequently_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_abnormal_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_read_pending_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_write_pending_nodes_.size());
    ASSERT_EQ(0, cluster_->tablets_moved_to_scan_pending_nodes_.size());

    cluster_->tablets_[tablet_index]->tablet_ptr->last_move_time_us_ = current_time_us - 2 * cluster_->lb_options_.tablet_move_too_frequently_threshold_s * 1000000;
    cluster_->MoveTablet(tablet_index, first_node_index, second_node_index);
    ASSERT_EQ(0, cluster_->tablets_moved_too_frequently_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_abnormal_nodes_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_read_pending_nodes_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_write_pending_nodes_.size());
    ASSERT_EQ(1, cluster_->tablets_moved_to_scan_pending_nodes_.size());
}

TEST_F(ClusterTest, AbnormalNodeConstructTest) {
    TabletMeta tablet_meta_0;
    tablet_meta_0.set_path("path/meta_0");
    tera::master::TabletPtr tablet_ptr_0(new tera::master::Tablet(tablet_meta_0));
    std::shared_ptr<LBTablet> lb_tablet_0 = std::make_shared<LBTablet>();
    lb_tablet_0->tablet_ptr = tablet_ptr_0;

    TabletMeta tablet_meta_1;
    tablet_meta_1.set_path("path/meta_1");
    tera::master::TabletPtr tablet_ptr_1(new tera::master::Tablet(tablet_meta_1));
    std::shared_ptr<LBTablet> lb_tablet_1 = std::make_shared<LBTablet>();
    lb_tablet_1->tablet_ptr = tablet_ptr_1;

    TabletMeta tablet_meta_2;
    tablet_meta_2.set_path("path/meta_2");
    tera::master::TabletPtr tablet_ptr_2(new tera::master::Tablet(tablet_meta_2));
    std::shared_ptr<LBTablet> lb_tablet_2 = std::make_shared<LBTablet>();
    lb_tablet_2->tablet_ptr = tablet_ptr_2;

    tera::master::TabletNodePtr tablet_node_ptr(new tera::master::TabletNode());
    tablet_node_ptr->addr_ = "127.0.0.1:2200";
    std::shared_ptr<LBTabletNode> lb_node = std::make_shared<LBTabletNode>();
    lb_node->tablet_node_ptr = tablet_node_ptr;
    lb_node->tablets.emplace_back(lb_tablet_0);
    lb_node->tablets.emplace_back(lb_tablet_1);
    lb_node->tablets.emplace_back(lb_tablet_2);

    std::vector<std::shared_ptr<LBTabletNode>> lb_nodes;
    lb_nodes.emplace_back(lb_node);

    LBOptions options;
    options.abnormal_node_ratio = 0.5;

    tablet_ptr_0->SetStatus(kTableReady);
    tablet_ptr_1->SetStatus(kTableReady);
    tablet_ptr_2->SetStatus(kTableReady);
    cluster_.reset(new Cluster(lb_nodes, options));
    ASSERT_EQ(0, cluster_->initial_tablets_not_ready_per_node_[0].size());
    ASSERT_EQ(0, cluster_->abnormal_nodes_index_.size());

    tablet_ptr_0->SetStatus(kTableOffLine);
    cluster_.reset(new Cluster(lb_nodes, options));
    ASSERT_EQ(1, cluster_->initial_tablets_not_ready_per_node_[0].size());
    ASSERT_EQ(0, cluster_->abnormal_nodes_index_.size());

    tablet_ptr_1->SetStatus(kTableOffLine);
    cluster_.reset(new Cluster(lb_nodes, options));
    ASSERT_EQ(2, cluster_->initial_tablets_not_ready_per_node_[0].size());
    ASSERT_EQ(1, cluster_->abnormal_nodes_index_.size());
}

TEST_F(ClusterTest, SortNodesByTabletCount) {
    cluster_->tablets_per_node_[0].emplace_back(0);
    cluster_->tablets_per_node_[0].emplace_back(1);
    cluster_->tablets_per_node_[1].emplace_back(2);
    cluster_->tablets_per_node_[2].emplace_back(3);
    cluster_->tablets_per_node_[2].emplace_back(4);
    cluster_->tablets_per_node_[2].emplace_back(5);

    cluster_->node_index_sorted_by_tablet_count_.emplace_back(0);
    cluster_->node_index_sorted_by_tablet_count_.emplace_back(1);
    cluster_->node_index_sorted_by_tablet_count_.emplace_back(2);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_tablet_count_[0]);
    ASSERT_EQ(1, cluster_->node_index_sorted_by_tablet_count_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_tablet_count_[2]);

    cluster_->SortNodesByTabletCount();
    ASSERT_EQ(1, cluster_->node_index_sorted_by_tablet_count_[0]);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_tablet_count_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_tablet_count_[2]);
}

TEST_F(ClusterTest, SortNodesBySizeTest) {
    cluster_->size_per_node_[0] = 20;
    cluster_->size_per_node_[1] = 10;
    cluster_->size_per_node_[2] = 30;

    cluster_->node_index_sorted_by_size_.emplace_back(0);
    cluster_->node_index_sorted_by_size_.emplace_back(1);
    cluster_->node_index_sorted_by_size_.emplace_back(2);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_size_[0]);
    ASSERT_EQ(1, cluster_->node_index_sorted_by_size_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_size_[2]);

    cluster_->SortNodesBySize();
    ASSERT_EQ(1, cluster_->node_index_sorted_by_size_[0]);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_size_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_size_[2]);
}

TEST_F(ClusterTest, SortNodesByReadLoad) {
    cluster_->read_load_per_node_[0] = 20;
    cluster_->read_load_per_node_[1] = 10;
    cluster_->read_load_per_node_[2] = 30;

    cluster_->node_index_sorted_by_read_load_.emplace_back(0);
    cluster_->node_index_sorted_by_read_load_.emplace_back(1);
    cluster_->node_index_sorted_by_read_load_.emplace_back(2);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_read_load_[0]);
    ASSERT_EQ(1, cluster_->node_index_sorted_by_read_load_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_read_load_[2]);

    cluster_->SortNodesByReadLoad();
    ASSERT_EQ(1, cluster_->node_index_sorted_by_read_load_[0]);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_read_load_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_read_load_[2]);
}

TEST_F(ClusterTest, SortNodesByWriteLoad) {
    cluster_->write_load_per_node_[0] = 20;
    cluster_->write_load_per_node_[1] = 10;
    cluster_->write_load_per_node_[2] = 30;

    cluster_->node_index_sorted_by_write_load_.emplace_back(0);
    cluster_->node_index_sorted_by_write_load_.emplace_back(1);
    cluster_->node_index_sorted_by_write_load_.emplace_back(2);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_write_load_[0]);
    ASSERT_EQ(1, cluster_->node_index_sorted_by_write_load_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_write_load_[2]);

    cluster_->SortNodesByWriteLoad();
    ASSERT_EQ(1, cluster_->node_index_sorted_by_write_load_[0]);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_write_load_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_write_load_[2]);
}

TEST_F(ClusterTest, SortNodesByScanLoad) {
    cluster_->scan_load_per_node_[0] = 20;
    cluster_->scan_load_per_node_[1] = 10;
    cluster_->scan_load_per_node_[2] = 30;

    cluster_->node_index_sorted_by_scan_load_.emplace_back(0);
    cluster_->node_index_sorted_by_scan_load_.emplace_back(1);
    cluster_->node_index_sorted_by_scan_load_.emplace_back(2);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_scan_load_[0]);
    ASSERT_EQ(1, cluster_->node_index_sorted_by_scan_load_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_scan_load_[2]);

    cluster_->SortNodesByScanLoad();
    ASSERT_EQ(1, cluster_->node_index_sorted_by_scan_load_[0]);
    ASSERT_EQ(0, cluster_->node_index_sorted_by_scan_load_[1]);
    ASSERT_EQ(2, cluster_->node_index_sorted_by_scan_load_[2]);
}

} // namespace load_balancer
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
