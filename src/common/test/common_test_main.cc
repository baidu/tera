// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "utils/utils_cmd.h"

int main(int argc, char** argv) {
  ::google::InitGoogleLogging(argv[0]);
  FLAGS_v = 16;
  FLAGS_minloglevel = 0;
  FLAGS_log_dir = "./log";
  if (access(FLAGS_log_dir.c_str(), F_OK)) {
    mkdir(FLAGS_log_dir.c_str(), 0777);
  }
  std::string pragram_name("tera");
  tera::utils::SetupLog(pragram_name);
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
