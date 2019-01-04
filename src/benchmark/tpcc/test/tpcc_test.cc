// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: baorenyi@baidu.com

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace tera {
namespace tpcc {

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace tpcc
}  // namespace tera
