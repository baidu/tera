// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "load_balancer/actions.h"

namespace tera {
namespace load_balancer {

class ActionsTest : public ::testing::Test {
};

TEST_F(ActionsTest, MoveActionTest) {
    MoveAction move_action(0, 0, 1);
    std::shared_ptr<MoveAction> undo_action(dynamic_cast<MoveAction*>(move_action.UndoAction()));

    ASSERT_EQ(move_action.tablet_index_, undo_action->tablet_index_);
    ASSERT_EQ(move_action.source_node_index_, undo_action->dest_node_index_);
    ASSERT_EQ(move_action.dest_node_index_, undo_action->source_node_index_);
}

} // namespace load_balancer
} // namespace tera

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
