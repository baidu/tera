// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "load_balancer/cost_functions.h"
#include "load_balancer/random.h"

namespace tera {
namespace load_balancer {

class CostFunctionTest : public ::testing::Test {
public:
    virtual void SetUp() {
        move_cost_function_.reset(new MoveCountCostFunction(lb_options_));
    }

    virtual void TearDown() {
    }

private:
    LBOptions lb_options_;
    std::shared_ptr<MoveCountCostFunction> move_cost_function_;
};

class MoveCountCostFunctionTest : public ::testing::Test {
public:
    virtual void SetUp() {
        move_cost_function_.reset(new MoveCountCostFunction(lb_options_));

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));

        move_cost_function_->Init(cluster_);
    }

    virtual void TearDown() {
    }

private:
    LBOptions lb_options_;
    std::shared_ptr<MoveCountCostFunction> move_cost_function_;
    std::shared_ptr<Cluster> cluster_;
};

class TabletCountCostFunctionTest : public ::testing::Test {
public:
    virtual void SetUp() {
        tablet_count_cost_function_.reset(new TabletCountCostFunction(lb_options_));

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));

        tablet_count_cost_function_->Init(cluster_);
    }

    virtual void TearDown() {
    }

private:
    LBOptions lb_options_;
    std::shared_ptr<TabletCountCostFunction> tablet_count_cost_function_;
    std::shared_ptr<Cluster> cluster_;
};

class SizeCostFunctionTest : public ::testing::Test {
public:
    virtual void SetUp() {
        size_cost_function_.reset(new SizeCostFunction(lb_options_));

        std::vector<std::shared_ptr<LBTabletNode>> empty_lb_nodes;
        LBOptions options;
        cluster_.reset(new Cluster(empty_lb_nodes, options));

        size_cost_function_->Init(cluster_);
    }

    virtual void TearDown() {
    }

private:
    LBOptions lb_options_;
    std::shared_ptr<SizeCostFunction> size_cost_function_;
    std::shared_ptr<Cluster> cluster_;
};

TEST_F(CostFunctionTest, WeightTest) {
    double w = 3.14;
    move_cost_function_->SetWeight(w);
    ASSERT_DOUBLE_EQ(w, move_cost_function_->GetWeight());
}

TEST_F(CostFunctionTest, SumTest) {
    std::vector<double> stats = {1, 2, 3};
    ASSERT_DOUBLE_EQ(6, move_cost_function_->GetSum(stats));
}

TEST_F(CostFunctionTest, ScaleTest) {
    // value <= min
    ASSERT_DOUBLE_EQ(0, move_cost_function_->Scale(0, 10, -1));
    ASSERT_DOUBLE_EQ(0, move_cost_function_->Scale(0, 10, 0));

    // max <= min
    ASSERT_DOUBLE_EQ(0, move_cost_function_->Scale(0, 0, 5));
    ASSERT_DOUBLE_EQ(0, move_cost_function_->Scale(0, -1, 5));

    // normal case
    ASSERT_DOUBLE_EQ(0, move_cost_function_->Scale(0, 10, 0));
    ASSERT_DOUBLE_EQ(0.5, move_cost_function_->Scale(0, 10, 5));
    ASSERT_DOUBLE_EQ(1, move_cost_function_->Scale(0, 10, 10));

    // random case
    size_t times = 100;
    int min = 0;
    int max = 10;
    for (size_t i = 0; i < times; ++i) {
        int value = Random::Rand(min, max + 1);
        ASSERT_TRUE(move_cost_function_->Scale(min, max, value) >= 0);
        ASSERT_TRUE(move_cost_function_->Scale(min, max, value) <= 1);
    }
}

TEST_F(CostFunctionTest, ScaleFromArrayTest) {
    std::vector<double> stats_0 = {0, 0};
    ASSERT_DOUBLE_EQ(0, move_cost_function_->ScaleFromArray(stats_0));

    std::vector<double> stats_1 = {10, 10};
    ASSERT_DOUBLE_EQ(0, move_cost_function_->ScaleFromArray(stats_0));

    int begin = 0;
    int end = 100;
    size_t times = 100;
    std::vector<double> stats_2;
    for (size_t i = 0; i < times; ++i) {
        stats_2.clear();
        stats_2.emplace_back(Random::Rand(begin, end));
        stats_2.emplace_back(Random::Rand(begin, end));

        ASSERT_TRUE(move_cost_function_->ScaleFromArray(stats_2) >= 0);
        ASSERT_TRUE(move_cost_function_->ScaleFromArray(stats_2) <= 1);
    }
}

TEST_F(MoveCountCostFunctionTest, CostTest) {
    move_cost_function_->tablet_max_move_num_ = 10;
    cluster_->tablet_num_ = 10;

    cluster_->tablet_moved_num_ = 1;
    ASSERT_DOUBLE_EQ(0.1, move_cost_function_->Cost());

    cluster_->tablet_moved_num_ = 6;
    ASSERT_DOUBLE_EQ(0.6, move_cost_function_->Cost());

    cluster_->tablet_moved_num_ = 10;
    ASSERT_DOUBLE_EQ(1, move_cost_function_->Cost());

    cluster_->tablet_moved_num_ = 11;
    ASSERT_DOUBLE_EQ(move_cost_function_->kExpensiveCost, move_cost_function_->Cost());
}

TEST_F(TabletCountCostFunctionTest, CostTest) {
}

TEST_F(SizeCostFunctionTest, CostTest) {
}

} // namespace load_balancer
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
