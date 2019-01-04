// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gtest/gtest.h"
#include "master/abnormal_node_mgr.h"

namespace tera {
namespace master {
namespace test {

class AbnormalNodeTest : public ::testing::Test {
 public:
  AbnormalNodeTest() {}

  virtual ~AbnormalNodeTest() {}
};

TEST_F(AbnormalNodeTest, TestRecoredNodeDelete) {
  FLAGS_abnormal_node_check_period_s = 10;
  FLAGS_abnormal_node_trigger_count = 3;
  FLAGS_abnormal_node_auto_recovery_period_s = 30;
  AbnormalNodeMgr mgr;
  const std::string addr = "host0";

  mgr.RecordNodeDelete(addr, get_micros() / 1000000);
  ASSERT_EQ(1, mgr.nodes_abnormal_infos_[addr].deleted_times.size());
  ASSERT_EQ(0, mgr.nodes_abnormal_infos_[addr].abnormal_count);

  mgr.RecordNodeDelete(addr, get_micros() / 1000000);
  ASSERT_EQ(2, mgr.nodes_abnormal_infos_[addr].deleted_times.size());
  ASSERT_EQ(0, mgr.nodes_abnormal_infos_[addr].abnormal_count);

  int64_t t1 = get_micros() / 1000000;
  mgr.RecordNodeDelete(addr, t1);
  ASSERT_EQ(0, mgr.nodes_abnormal_infos_[addr].deleted_times.size());
  ASSERT_EQ(1, mgr.nodes_abnormal_infos_[addr].abnormal_count);
  ASSERT_EQ(mgr.nodes_abnormal_infos_[addr].recovery_time,
            t1 + FLAGS_abnormal_node_auto_recovery_period_s);

  mgr.RecordNodeDelete(addr, get_micros() / 1000000);
  ASSERT_EQ(1, mgr.nodes_abnormal_infos_[addr].deleted_times.size());
  ASSERT_EQ(1, mgr.nodes_abnormal_infos_[addr].abnormal_count);

  mgr.RecordNodeDelete(addr, get_micros() / 1000000);
  ASSERT_EQ(2, mgr.nodes_abnormal_infos_[addr].deleted_times.size());
  ASSERT_EQ(1, mgr.nodes_abnormal_infos_[addr].abnormal_count);

  int64_t t2 = get_micros() / 1000000;
  mgr.RecordNodeDelete(addr, t2);
  ASSERT_EQ(0, mgr.nodes_abnormal_infos_[addr].deleted_times.size());
  ASSERT_EQ(2, mgr.nodes_abnormal_infos_[addr].abnormal_count);
  ASSERT_EQ(mgr.nodes_abnormal_infos_[addr].recovery_time,
            t2 + (FLAGS_abnormal_node_auto_recovery_period_s << 1));
}

TEST_F(AbnormalNodeTest, TestIsAbnormalNode) {
  AbnormalNodeMgr mgr;
  const std::string addr = "host0";
  const std::string uuid = "host0:uuid";
  FLAGS_abnormal_node_check_period_s = 10;
  FLAGS_abnormal_node_trigger_count = 3;
  FLAGS_abnormal_node_auto_recovery_period_s = 30;

  // empty node info
  ASSERT_FALSE(mgr.IsAbnormalNode(addr, uuid));

  mgr.RecordNodeDelete(addr, get_micros() / 1000000);
  std::vector<int64_t>& times = mgr.nodes_abnormal_infos_[addr].deleted_times;
  mgr.RecordNodeDelete(addr, get_micros() / 1000000);

  // has't trigger delete too frequent
  ASSERT_FALSE(mgr.IsAbnormalNode(addr, uuid));

  int64_t t1 = get_micros() / 1000000;
  mgr.RecordNodeDelete(addr, t1);

  // trigger delete too frequent
  ASSERT_TRUE(mgr.IsAbnormalNode(addr, uuid));

  // not recovery
  ASSERT_TRUE(mgr.IsAbnormalNode(addr, uuid));

  mgr.nodes_abnormal_infos_[addr].recovery_time = t1;

  // auto recovery
  ASSERT_FALSE(mgr.IsAbnormalNode(addr, uuid));
}

TEST_F(AbnormalNodeTest, TestDeleteTooFrequent) {
  AbnormalNodeMgr mgr;
  const std::string addr = "host0";
  FLAGS_abnormal_node_check_period_s = 10;
  FLAGS_abnormal_node_trigger_count = 3;

  std::vector<int64_t>& times = mgr.nodes_abnormal_infos_[addr].deleted_times;
  ASSERT_FALSE(mgr.DeleteTooFrequent(times));

  times.emplace_back(1);
  ASSERT_FALSE(mgr.DeleteTooFrequent(times));

  times.emplace_back(2);
  ASSERT_FALSE(mgr.DeleteTooFrequent(times));

  times.emplace_back(3);
  ASSERT_TRUE(mgr.DeleteTooFrequent(times));
}
}
}
}
