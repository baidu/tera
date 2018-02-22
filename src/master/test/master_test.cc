// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "utils/utils_cmd.h"

DECLARE_string(tera_leveldb_env_type);

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    tera::utils::SetupLog("master_test");
    FLAGS_tera_leveldb_env_type = "local";
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}

