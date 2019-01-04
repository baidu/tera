// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#define private public

#include "tabletnode_sysinfo.h"
#include "common/timer.h"
#include "gtest/gtest.h"

namespace tera {
namespace tabletnode {

class TabletNodeSysInfoTest : public ::testing::Test, public TabletNodeSysInfo {
 public:
  TabletNodeSysInfoTest() {}
  ~TabletNodeSysInfoTest() {}
};

TEST_F(TabletNodeSysInfoTest, CollectHardwareInfo) {
  int64_t ts = get_micros();
  CollectHardwareInfo();
  ts = get_micros() - ts;
  LOG(ERROR) << "cost: " << ts << " ms.";
}

TEST_F(TabletNodeSysInfoTest, ToString) {
  SetCurrentTime();
  AddExtraInfo("read", 100);
}
}  // namespace tabletnode
}  // namespace tera
