// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include "common/this_thread.h"
#include "quota/ts_write_flow_controller.h"

namespace tera {
namespace test {

TEST(TsWriteFlowControllerTest, BaseTest) {
  auto& wfc = TsWriteFlowController::Instance();
  wfc.Append(0, 4);
  wfc.Append(0, 4);
  wfc.Append(0, 4);
  wfc.Append(0, 4);
  EXPECT_EQ(wfc.write_throughput_queue_.size(), 4);
  EXPECT_FALSE(wfc.InSlowdownMode());
  wfc.SetSlowdownMode(0.5);
  EXPECT_TRUE(wfc.InSlowdownMode());
  EXPECT_EQ(wfc.flow_controller_.limiter_.limit_, 2);
  wfc.Append(700000, 3);
  wfc.Append(700000, 3);
  EXPECT_EQ(wfc.write_throughput_queue_.size(), 2);
  wfc.SetSlowdownMode(0.5);
  EXPECT_TRUE(wfc.InSlowdownMode());
  EXPECT_EQ(wfc.flow_controller_.limiter_.limit_, 1);
  wfc.Append(1900000, 8);
  wfc.Append(1900000, 8);
  wfc.Append(1900000, 10);
  wfc.Append(1900000, 10);
  EXPECT_EQ(wfc.write_throughput_queue_.size(), 4);
  wfc.SetSlowdownMode(0.8);
  EXPECT_TRUE(wfc.InSlowdownMode());
  EXPECT_EQ(wfc.flow_controller_.limiter_.limit_, 7);
  wfc.flow_controller_.stop_event_.Set();
  wfc.flow_controller_.limiter_.ResetQuota();
  EXPECT_TRUE(wfc.TryWrite(3));
  EXPECT_TRUE(wfc.TryWrite(4));
  EXPECT_FALSE(wfc.TryWrite(2));
  EXPECT_TRUE(wfc.InSlowdownMode());
  wfc.ResetSlowdownMode();
  EXPECT_FALSE(wfc.InSlowdownMode());
  EXPECT_EQ(wfc.flow_controller_.status_, FlowController::FlowControlStatus::kNormal);
  EXPECT_TRUE(wfc.TryWrite(10000));
}
}
}
