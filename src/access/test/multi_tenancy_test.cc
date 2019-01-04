// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

DECLARE_bool(tera_quota_enabled);
DECLARE_string(tera_quota_limiter_type);
DECLARE_int64(tera_quota_normal_estimate_value);

int main(int argc, char** argv) {
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_tera_quota_enabled = true;
  FLAGS_tera_quota_limiter_type = "general_quota_limiter";
  FLAGS_tera_quota_normal_estimate_value = 1;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
