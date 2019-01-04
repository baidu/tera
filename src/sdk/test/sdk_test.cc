// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
DECLARE_bool(tera_sdk_tso_client_enabled);
DECLARE_bool(tera_sdk_client_for_gtxn);

int main(int argc, char* argv[]) {
  FLAGS_tera_sdk_client_for_gtxn = true;
  FLAGS_tera_sdk_tso_client_enabled = false;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
